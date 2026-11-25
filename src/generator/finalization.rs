//! Single-table finalization in the caller's Rayon pool, with ordered frames.

mod stats;

use std::io::Write;
use std::sync::atomic::AtomicU16;

use rayon::prelude::*;

use self::stats::StatsAccumulator;
use super::WinLoss;
use crate::TableIndexer;
use crate::error::{EgtbError, EgtbResult};
use crate::outcome::PackedOutcome;
use crate::position::InvalidCounts;
use crate::stats::{TableStats, write_stats};
use crate::storage::Location;
use crate::table_file::writer::{
    COMPRESSION_LEVEL, CompressedSegment, TableFileWriter, encode_frame,
};
use crate::table_id::TableId;

fn finalize_batch(
    values: &mut [AtomicU16],
    start: usize,
    egt: &TableIndexer,
    frame_size: usize,
    maxima: WinLoss<u16>,
) -> EgtbResult<(CompressedSegment, StatsAccumulator)> {
    let mut data = Vec::new();
    let mut encoder = zeekstd::EncodeOptions::new()
        .compression_level(COMPRESSION_LEVEL)
        .into_encoder(&mut data)?;
    let mut plain = Vec::with_capacity(frame_size.min(values.len()));
    let mut stats = StatsAccumulator::default();
    let mut frame_lengths = Vec::new();
    for (frame_idx, chunk) in values.chunks_mut(frame_size).enumerate() {
        plain.clear();
        for (offset, cell) in chunk.iter_mut().enumerate() {
            // Propagation has joined; exclusive access needs no atomic load/CAS.
            let raw = PackedOutcome::from_u16(*cell.get_mut());
            let outcome = if raw.is_unknown() {
                PackedOutcome::DRAW
            } else {
                raw
            };
            stats.record_outcome(
                outcome,
                start + frame_idx * frame_size + offset,
                egt,
                maxima,
            );
            plain.push(outcome);
        }
        stats.record_frame(encode_frame(&mut encoder, &plain)?);
        frame_lengths.push(plain.len());
    }
    // into_seek_table does not flush, and finish would append a per-batch footer.
    encoder.flush()?;
    let seek_table = encoder.into_seek_table();
    Ok((
        CompressedSegment {
            data: data.into(),
            seek_table,
            frame_lengths,
        },
        stats,
    ))
}

/// Resolves unknowns to draws, computes statistics, and publishes one table and
/// its JSON sidecar. Call only after all propagation workers have joined.
/// Binary and sidecar publication are not an atomic multi-object transaction.
pub(super) fn finalize_table(
    location: &Location,
    id: &TableId,
    egt: &TableIndexer,
    mut values: Vec<AtomicU16>,
    maxima: WinLoss<u16>,
    invalid: InvalidCounts,
) -> EgtbResult<TableStats> {
    if values.len() != egt.index_range() {
        return Err(EgtbError::Internal("completed table has wrong length"));
    }
    let frame_size = egt.frame_size();
    let writer = TableFileWriter::new(location, id, egt.index_range(), frame_size)?;
    let frames = values.len().div_ceil(frame_size);
    let batch_size = frames.div_ceil(rayon::current_num_threads()).max(1) * frame_size;
    // Contiguous batches reuse one encoder each. Indexed collection preserves
    // frame and example order, independently of worker completion order.
    let batches: Vec<_> = values
        .par_chunks_mut(batch_size)
        .enumerate()
        .map(|(idx, batch)| finalize_batch(batch, idx * batch_size, egt, frame_size, maxima))
        .collect();
    drop(values);
    let batches = batches.into_iter().collect::<EgtbResult<Vec<_>>>()?;
    let (segments, parts): (Vec<_>, Vec<_>) = batches.into_iter().unzip();
    let mut stats = StatsAccumulator::default();
    stats.merge(parts);
    let (bytes, sha256) = writer.publish(segments)?;
    let stats = stats.build(egt, maxima, invalid, bytes, sha256);
    write_stats(location, id, &stats)?;
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_encoded_palettes_including_invalid_and_short_frames() {
        let id = TableId::parse("KQ_K").unwrap();
        let egt = TableIndexer::new(&id);
        let wins = (0..256)
            .map(|distance| PackedOutcome::new_win(crate::ConversionType::Checkmate, distance));
        let mut values: Vec<_> = wins
            .chain(std::iter::once(PackedOutcome::INVALID))
            .chain([PackedOutcome::DRAW, PackedOutcome::new_unknown(1)])
            .map(|outcome| AtomicU16::new(outcome.to_u16()))
            .collect();
        let (segment, stats) =
            finalize_batch(&mut values, 0, &egt, 257, WinLoss::default()).unwrap();
        assert_eq!(segment.frame_lengths, [257, 2]);
        let stats = stats.build(
            &egt,
            WinLoss::default(),
            InvalidCounts::default(),
            0,
            String::new(),
        );
        assert_eq!(
            stats.totals.num_frames_by_distinct_outcomes,
            [(1, 1), (512, 1)].into()
        );
    }

    #[test]
    fn wrong_length_does_not_publish() {
        let dir = crate::TestDir::new("finalization_wrong_length");
        let location = Location::parse(&dir.0).unwrap();
        let id = TableId::parse("K_K").unwrap();
        let egt = TableIndexer::new(&id);
        assert!(matches!(
            finalize_table(
                &location,
                &id,
                &egt,
                Vec::new(),
                WinLoss::default(),
                InvalidCounts::default()
            ),
            Err(EgtbError::Internal("completed table has wrong length"))
        ));
        assert!(!dir.0.join(id.relative_path("ggegtb")).exists());
        assert!(!dir.0.join(id.relative_path("json")).exists());
    }
}
