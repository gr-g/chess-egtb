mod batch;
mod cache;
mod frame;
pub(crate) mod writer;

pub(crate) use batch::BatchReader;
pub(crate) use cache::SharedFileCache;
use frame::Frame;

use crate::TableIndexer;
use crate::error::{EgtbError, EgtbResult, try_allocate};
use crate::storage::{Location, block_on};
use crate::table_id::TableId;
use bytes::Bytes;
use object_store::{GetOptions, GetRange, ObjectStoreExt, path::Path as ObjectPath};
use shakmaty::Chess;

/// Resolves existing tables with input-first, missing-only fallback.
#[derive(Debug)]
pub(crate) struct Source {
    base: Location,
    input: Option<Location>,
}

impl Source {
    pub(crate) fn new(base: Location) -> Self {
        Self { base, input: None }
    }

    pub(crate) fn with_input(&self, input: Option<Location>) -> Self {
        Self {
            base: self.base.clone(),
            input,
        }
    }

    pub(crate) fn base(&self) -> &Location {
        &self.base
    }

    /// Opens only metadata. The object must not be replaced while frames are read on demand.
    pub(crate) fn open(&self, id: &TableId) -> EgtbResult<TableFile> {
        let egt = TableIndexer::new(id);
        let range = GetRange::Suffix(seek_table_size(&egt) as u64);
        self.read(egt, Some(range)).map(|(file, _)| file)
    }

    /// Fetches the whole object once and decodes its frames in order.
    pub(crate) fn load(&self, id: &TableId) -> EgtbResult<(TableFile, Vec<Frame>)> {
        let (file, bytes) = self.read(TableIndexer::new(id), None)?;
        let table = &file.seek_table;
        let frames = (0..file.num_frames())
            .map(|frame| {
                let start = table.frame_start_comp(frame as u32)? as usize;
                let end = table.frame_end_comp(frame as u32)? as usize;
                file.decode_frame(frame, &bytes[start..end])
            })
            .collect::<EgtbResult<_>>()?;
        Ok((file, frames))
    }

    fn read(&self, egt: TableIndexer, range: Option<GetRange>) -> EgtbResult<(TableFile, Bytes)> {
        let path = egt.id().relative_path("ggegtb");
        let fetched = if let Some(input) = &self.input {
            match fetch(input, &path, range.clone()) {
                Err(EgtbError::FileNotFound(_)) => fetch(&self.base, &path, range),
                result => result,
            }
        } else {
            fetch(&self.base, &path, range)
        };
        let (location, key, offset, bytes) = fetched?;
        let seek_table = read_seek_table(&egt, &bytes, offset)
            .map_err(|error| location.invalid_data(&key, error))?;
        Ok((
            TableFile {
                egt,
                location,
                key,
                seek_table,
            },
            bytes,
        ))
    }
}

fn fetch(
    location: &Location,
    path: &str,
    range: Option<GetRange>,
) -> EgtbResult<(Location, ObjectPath, u64, Bytes)> {
    location.validate_format()?;
    let key = location.key(path);
    let options = GetOptions::new().with_range(range);
    let result = block_on(location.store.get_opts(&key, options))
        .map_err(|error| location.error(&key, error))?;
    let offset = result.range.start;
    let bytes = block_on(result.bytes()).map_err(|error| location.error(&key, error))?;
    Ok((location.clone(), key, offset, bytes))
}

/// File metadata and frame I/O for one existing canonical table.
#[derive(Debug)]
pub(crate) struct TableFile {
    egt: TableIndexer,
    location: Location,
    key: ObjectPath,
    seek_table: zeekstd::SeekTable,
}

impl TableFile {
    /// The immutable descriptor of this canonical table.
    pub(crate) fn egt(&self) -> &TableIndexer {
        &self.egt
    }

    pub(crate) fn num_frames(&self) -> usize {
        self.egt.index_range().div_ceil(self.egt.frame_size())
    }

    /// Indexes a position already routed to this table. The caller must have
    /// selected this identity and its reflection flag via `TableId::from_position`.
    pub(crate) fn index(&self, position: &Chess, reflected: bool) -> EgtbResult<usize> {
        let index = if reflected {
            self.egt.reflected_position_to_index(position)
        } else {
            self.egt.position_to_index(position)
        };
        if index >= self.egt.index_range() {
            return Err(EgtbError::IndexOutOfRange {
                index,
                range: self.egt.index_range(),
            });
        }
        Ok(index)
    }

    fn locate(&self, index: usize) -> (usize, usize) {
        let frame_size = self.egt.frame_size();
        (index / frame_size, index % frame_size)
    }

    fn frame_len(&self, frame_idx: usize) -> usize {
        let frame_size = self.egt.frame_size();
        frame_size.min(self.egt.index_range() - frame_idx * frame_size)
    }

    fn invalid_frame(
        &self,
        frame: usize,
        source: impl Into<Box<dyn std::error::Error + Send + Sync>>,
    ) -> EgtbError {
        EgtbError::InvalidData {
            location: format!(
                "{} (frame {frame}, indices {}..{})",
                self.location.describe(&self.key),
                frame * self.egt.frame_size(),
                frame * self.egt.frame_size() + self.frame_len(frame)
            ),
            source: source.into(),
        }
    }

    fn decode_frame(&self, frame: usize, bytes: &[u8]) -> EgtbResult<Frame> {
        let encoded_len = self.seek_table.frame_size_decomp(frame as u32)? as usize;
        decode_frame(bytes, self.frame_len(frame), encoded_len).map_err(|error| match error {
            EgtbError::WorkingMemoryAllocation { .. } => error,
            other => self.invalid_frame(frame, other.into_data_source()),
        })
    }

    /// Reads and decodes frames in the requested order.
    fn read_frames(&self, frames: &[usize]) -> EgtbResult<Vec<Frame>> {
        if frames.is_empty() {
            return Ok(Vec::new());
        }
        let table = &self.seek_table;
        let ranges = frames
            .iter()
            .map(|&frame| {
                Ok(table.frame_start_comp(frame as u32)?..table.frame_end_comp(frame as u32)?)
            })
            .collect::<EgtbResult<Vec<_>>>()?;
        // Local get_ranges reports partial EOF differently from get_range.
        let compressed = if let [range] = ranges.as_slice() {
            block_on(self.location.store.get_range(&self.key, range.clone()))
                .map(|bytes| vec![bytes])
        } else {
            block_on(self.location.store.get_ranges(&self.key, &ranges))
        }
        .map_err(|error| {
            // A backend may reject a short range instead of returning partial bytes.
            // Only classify it as corrupt data when truncation is confirmed.
            if let Ok(meta) = block_on(self.location.store.head(&self.key))
                && let Some(index) = ranges.iter().position(|range| range.end > meta.size)
            {
                return self.invalid_frame(frames[index], error);
            }
            self.location.error(&self.key, error)
        })?;
        if compressed.len() != frames.len() {
            return Err(EgtbError::Internal("invalid tablebase range count"));
        }
        frames
            .iter()
            .copied()
            .zip(ranges)
            .zip(compressed)
            .map(|((frame, range), bytes)| {
                if bytes.len() as u64 != range.end - range.start {
                    return Err(self.invalid_frame(frame, "truncated tablebase range"));
                }
                self.decode_frame(frame, &bytes)
            })
            .collect()
    }
}

fn seek_table_size(egt: &TableIndexer) -> usize {
    // Our checksum-free seek tables have 8-byte entries and 17 bytes of framing.
    17 + 8 * egt.index_range().div_ceil(egt.frame_size())
}

/// The response can be either the seek-table suffix or the entire object.
fn read_seek_table(
    egt: &TableIndexer,
    bytes: &[u8],
    offset: u64,
) -> Result<zeekstd::SeekTable, Box<dyn std::error::Error + Send + Sync>> {
    let start = bytes
        .len()
        .checked_sub(seek_table_size(egt))
        .ok_or("truncated tablebase seek table")?;
    let bytes = &bytes[start..];
    let index_range = egt.index_range();
    let frame_size = egt.frame_size();
    let num_frames = index_range.div_ceil(frame_size);
    // zeekstd allocates from the footer count before checking the source length.
    let count = u32::from_le_bytes(bytes[bytes.len() - 9..bytes.len() - 5].try_into().unwrap());
    if count as usize != num_frames {
        return Err("invalid tablebase frame count".into());
    }
    let table = zeekstd::SeekTable::from_seekable(&mut zeekstd::BytesWrapper::new(bytes))?;
    if table.size_comp() != offset + start as u64 {
        return Err("invalid tablebase compressed length".into());
    }
    for index in 0..num_frames {
        let len = frame_size.min(index_range - index * frame_size);
        let decompressed = table.frame_size_decomp(index as u32)?;
        let compressed = table.frame_size_comp(index as u32)?;
        if !frame::valid_length(decompressed, len) || compressed == 0 {
            return Err(format!("invalid frame {index} length").into());
        }
    }
    Ok(table)
}

fn decode_frame(compressed: &[u8], len: usize, encoded_len: usize) -> EgtbResult<Frame> {
    let mut data = try_allocate(encoded_len, "frame decompression buffer")?;
    data.resize(encoded_len, 0);
    // One-shot decoding is bounded by both slices and rejects truncated input.
    // zeekstd's streaming decoder can otherwise loop at premature EOF.
    let decoded = zstd_safe::decompress(&mut data[..], compressed).map_err(|code| {
        EgtbError::invalid_data(
            "tablebase frame",
            format!(
                "invalid compressed tablebase frame: {}",
                zstd_safe::get_error_name(code)
            ),
        )
    })?;
    if decoded != data.len() {
        return Err(EgtbError::invalid_data(
            "tablebase frame",
            "decompressed tablebase frame has wrong length",
        ));
    }
    Frame::decode(data, len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConversionType;
    use crate::outcome::PackedOutcome;
    use crate::position::mirror_horizontally;
    use crate::table_file::writer::write_fixture;
    use shakmaty::Color;
    use shakmaty::fen::Fen;
    use shakmaty::{CastlingMode, Position};
    use std::path::Path;

    fn open_local(base: &Path, name: &str) -> EgtbResult<TableFile> {
        Source::new(Location::parse(base)?).open(&TableId::parse(name)?)
    }

    fn table_path(base: &Path, name: &str) -> std::path::PathBuf {
        let path = base.join(TableId::parse(name).unwrap().relative_path("ggegtb"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        path
    }

    #[test]
    fn pawn_orders_route_to_distinct_tables_and_their_twins() {
        for name in ["KPa_KPa", "KPao_KPao", "KPaPa_KPb", "KPa_KPbPb"] {
            let id = TableId::parse(name).unwrap();
            let egt = TableIndexer::new(&id);
            let (twin, _) = id.twin();
            let twin_egt = TableIndexer::new(&twin);
            let stride = (egt.index_range() / 64).max(1);
            for turn in [Color::White, Color::Black] {
                for index in (0..egt.index_range()).step_by(stride) {
                    let Ok(position) = egt.position_from_index(index, turn) else {
                        continue;
                    };
                    assert_eq!(TableId::from_position(&position).unwrap().0, id);
                    assert_eq!(egt.position_to_index(&position), index);
                    for mv in position.legal_moves() {
                        if mv.is_capture() || mv.is_promotion() {
                            continue;
                        }
                        let successor = position.clone().play(mv).unwrap();
                        let (routed, reflected) = TableId::from_position(&successor).unwrap();
                        assert_eq!(routed, twin);
                        let local = if reflected {
                            twin_egt.reflected_position_to_index(&successor)
                        } else {
                            twin_egt.position_to_index(&successor)
                        };
                        assert_eq!(
                            twin_egt
                                .position_from_index(local, successor.turn())
                                .unwrap(),
                            if reflected {
                                mirror_horizontally(&successor)
                            } else {
                                successor
                            }
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn requires_format_and_uses_exact_hierarchical_namespace() {
        let dir = crate::TestDir::new("exact_namespace");
        let legacy = dir.0.join("KP_K.ggegtb");
        std::fs::write(&legacy, b"legacy indexing").unwrap();
        assert!(open_local(&dir.0, "KPa_K").is_err());
        assert!(!dir.0.join("format.json").exists());
        let file = write_fixture(&dir.0, "KPa_K", |_| PackedOutcome::INVALID);
        assert!(table_path(&dir.0, &file.egt().id().name()).exists());
        assert_eq!(std::fs::read(legacy).unwrap(), b"legacy indexing");
        assert!(open_local(&dir.0, "KPa_K").is_ok());
        assert!(open_local(&dir.0, "KP_K").is_err());
    }

    #[test]
    fn reads_unassigned_and_draw_outcomes() {
        let dir = crate::TestDir::new("unassigned_and_draw_outcomes");
        let position: Chess = "8/8/8/8/8/8/P7/K6k w - - 0 1"
            .parse::<Fen>()
            .unwrap()
            .into_position(CastlingMode::Standard)
            .unwrap();
        for outcome in [
            PackedOutcome::INVALID,
            PackedOutcome::new_unknown(3),
            PackedOutcome::DRAW,
        ] {
            let file = write_fixture(&dir.0, "KPa_K", |_| outcome);
            let (_, reflected) = TableId::from_position(&position).unwrap();
            let (frame, offset) = file.locate(file.index(&position, reflected).unwrap());
            assert_eq!(file.read_frames(&[frame]).unwrap()[0].get(offset), outcome);
        }
    }

    #[test]
    fn published_file_probes_canonical_and_mirrored_positions() {
        let dir = crate::TestDir::new("published_file_probe");
        let position = "8/8/8/8/8/8/P7/K6k w - - 0 1"
            .parse::<Fen>()
            .unwrap()
            .into_position(CastlingMode::Standard)
            .unwrap();
        let egt = TableIndexer::new(&crate::TableId::parse("KPa_K").unwrap());
        let index = egt.position_to_index(&position);
        let outcome = PackedOutcome::new_win(ConversionType::Checkmate, 987);
        let reader = write_fixture(&dir.0, "KPa_K", |i| {
            if i == index {
                outcome
            } else {
                PackedOutcome::INVALID
            }
        });
        for position in [&position, &mirror_horizontally(&position)] {
            let (id, reflected) = TableId::from_position(position).unwrap();
            assert_eq!(id, reader.egt().id());
            assert_eq!(reader.index(position, reflected).unwrap(), index);
            let (frame, offset) = reader.locate(index);
            assert_eq!(
                reader.read_frames(&[frame]).unwrap()[0].get(offset),
                outcome
            );
        }
        let prober = crate::Tablebase::new(&dir.0).unwrap();
        assert_eq!(
            prober.probe(&position).unwrap(),
            crate::DtcOutcome::try_from(outcome).unwrap()
        );
    }

    #[test]
    fn full_frames_short_tail_and_table_boundaries() {
        let dir = crate::TestDir::new("frame_boundaries");
        let egt = TableIndexer::new(&crate::TableId::parse("KQ_KR").unwrap());
        let length = egt.index_range();
        let frame_size = egt.frame_size();
        let indexes = [0, frame_size - 1, frame_size, frame_size + 1, length - 1];
        let reader = write_fixture(&dir.0, "KQ_KR", |idx| {
            indexes
                .iter()
                .position(|&i| i == idx)
                .map_or(PackedOutcome::INVALID, |n| {
                    PackedOutcome::new_loss(ConversionType::Capture, n as u16 + 1)
                })
        });
        assert!(reader.num_frames() > 1);
        assert_ne!(length % frame_size, 0);
        assert_eq!(
            reader.frame_len(reader.num_frames() - 1),
            length % frame_size
        );
        let id = reader.egt().id();
        let source = Source::new(Location::parse(&dir.0).unwrap());
        let batch = BatchReader::open(&source, &[id, id]).unwrap();
        assert!(
            batch
                .read_chunk(&id, frame_size - 1, frame_size + 1)
                .is_err()
        );
        for (n, &idx) in indexes.iter().enumerate() {
            assert_eq!(
                batch.read_chunk(&id, idx, idx + 1).unwrap()[0],
                PackedOutcome::new_loss(ConversionType::Capture, n as u16 + 1)
            );
        }
        assert!(batch.read_chunk(&id, length, length).unwrap().is_empty());
        assert!(batch.read_chunk(&id, length, length + 1).is_err());
        assert!(batch.read_chunk(&id, 1, 0).is_err());
    }

    #[test]
    fn rejects_invalid_frame_layout() {
        let dir = crate::TestDir::new("invalid_frame_layout");
        let file = write_fixture(&dir.0, "K_K", |_| PackedOutcome::INVALID);
        let frame_size = file.egt.frame_size();
        for bytes in [0, 3, 922, 926, frame_size * 2, frame_size * 2 + 2] {
            let output = std::fs::File::create(table_path(&dir.0, "K_K")).unwrap();
            let mut encoder = zeekstd::EncodeOptions::new().into_encoder(output).unwrap();
            encoder.compress(&vec![0; bytes]).unwrap();
            encoder.finish().unwrap();
            assert!(
                open_local(&dir.0, "K_K")
                    .and_then(|reader| reader.read_frames(&[0]))
                    .is_err()
            );
        }
        // A correct payload with an extra empty frame has the wrong frame count.
        let output = std::fs::File::create(table_path(&dir.0, "K_K")).unwrap();
        let mut encoder = zeekstd::EncodeOptions::new().into_encoder(output).unwrap();
        let (data, _) =
            frame::encode(&vec![PackedOutcome::INVALID; file.egt.index_range()]).unwrap();
        encoder.compress(&data).unwrap();
        encoder.end_frame().unwrap();
        encoder.finish().unwrap();
        assert!(open_local(&dir.0, "K_K").is_err());
    }

    #[test]
    fn rejects_malformed_seek_table_suffixes() {
        let dir = crate::TestDir::new("malformed_seek_table_suffixes");
        let path = table_path(&dir.0, "K_K");
        write_fixture(&dir.0, "K_K", |_| PackedOutcome::INVALID);
        let valid = std::fs::read(&path).unwrap();
        let table_start = valid.len() - 25;

        for length in [0, 8, 9, 24] {
            std::fs::write(&path, &valid[valid.len() - length..]).unwrap();
            assert!(matches!(open_local(&dir.0, "K_K"),
                Err(EgtbError::InvalidData { location, .. }) if location.ends_with("/K_K.ggegtb")));
        }
        // Reject the declared count before zeekstd can allocate from it.
        let mut inflated = valid.clone();
        let count_start = inflated.len() - 9;
        inflated[count_start..count_start + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(&path, inflated).unwrap();
        assert!(matches!(
            open_local(&dir.0, "K_K"),
            Err(EgtbError::InvalidData { location, source })
                if location.ends_with("/K_K.ggegtb") && source.to_string() == "invalid tablebase frame count"
        ));

        // zeekstd, rather than our loader, validates magic and reserved bits.
        for offset in [valid.len() - 1, valid.len() - 5, table_start] {
            let mut corrupt = valid.clone();
            corrupt[offset] ^= 0x04;
            std::fs::write(&path, corrupt).unwrap();
            assert!(matches!(
                open_local(&dir.0, "K_K"),
                Err(EgtbError::InvalidData { .. })
            ));
        }
        let mut prefixed = vec![0];
        prefixed.extend_from_slice(&valid);
        std::fs::write(&path, prefixed).unwrap();
        assert!(matches!(
            open_local(&dir.0, "K_K"),
            Err(EgtbError::InvalidData { source, .. })
                if source.to_string() == "invalid tablebase compressed length"
        ));

        // The reader deliberately accepts only our checksum-free seek-table format.
        let mut checksummed = valid.clone();
        checksummed.splice(table_start + 16..table_start + 16, [0; 4]);
        checksummed[table_start + 4..table_start + 8].copy_from_slice(&21u32.to_le_bytes());
        let descriptor = checksummed.len() - 5;
        checksummed[descriptor] |= 0x80;
        std::fs::write(&path, checksummed).unwrap();
        assert!(open_local(&dir.0, "K_K").is_err());

        std::fs::write(&path, valid).unwrap();
        open_local(&dir.0, "K_K").unwrap();
    }

    #[test]
    fn rejects_corrupt_frames_and_short_ranges() {
        let dir = crate::TestDir::new("short_frame_range");
        for name in ["K_K", "KQ_KR"] {
            let path = table_path(&dir.0, name);
            let reader = write_fixture(&dir.0, name, |_| PackedOutcome::INVALID);
            let valid = std::fs::read(&path).unwrap();
            let last = reader.num_frames() - 1;
            let frame_end = reader.seek_table.frame_end_comp(last as u32).unwrap();
            let mut corrupt = valid.clone();
            corrupt[0] ^= 0xff;
            std::fs::write(&path, corrupt).unwrap();
            assert!(matches!(reader.read_frames(&[0]),
                Err(EgtbError::InvalidData { location, source })
                    if location.contains(&format!("{name}.ggegtb (frame 0, indices 0.."))
                        && source.to_string().contains("invalid compressed tablebase frame")));
            assert!(
                matches!(Source::new(reader.location.clone()).load(&reader.egt.id()),
                Err(EgtbError::InvalidData { location, source })
                    if location.contains(&format!("{name}.ggegtb (frame 0, indices 0.."))
                        && source.to_string().contains("invalid compressed tablebase frame"))
            );
            std::fs::write(&path, valid).unwrap();
            std::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .set_len(frame_end - 1)
                .unwrap();
            let frames: Vec<_> = (0..reader.num_frames()).collect();
            assert!(matches!(reader.read_frames(&frames),
                Err(EgtbError::InvalidData { location, .. })
                    if location.contains(&format!("{name}.ggegtb (frame {last}, indices "))));
        }
    }
}
