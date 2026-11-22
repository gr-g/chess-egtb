mod finalization;
mod solver;
mod unmoves;

use std::time::{Instant, SystemTime};

use object_store::{ObjectStoreExt, PutMode, PutOptions, path::Path as ObjectPath};

use self::solver::{TableState, solve_pair};
use crate::report::{generation_summary, print_table_stats};
use crate::selection::{TablePair, plan_pairs};
use crate::storage::{Location, block_on};
use crate::table_file::{BatchReader, Source};
use crate::table_id::TableId;
use crate::{AggregateStats, EgtbResult, TableIndexer, TableStats, resolve_selectors};

#[derive(Default, Clone, Copy)]
struct WinLoss<T> {
    win: T,
    loss: T,
}

/// Generates missing selected tables and their twins in the base location.
pub(crate) fn generate(
    source: &Source,
    pool: &rayon::ThreadPool,
    selectors: &[&str],
) -> EgtbResult<Vec<TableStats>> {
    let start = Instant::now();
    let pairs = plan_pairs(resolve_selectors(selectors)?);
    let base = source.base();
    base.ensure_format()?;
    let mut results = Vec::new();
    let (mut complete, mut busy) = (0, 0);
    println!(
        "Considering {} twin-pair jobs using {} threads.",
        pairs.len(),
        pool.current_num_threads()
    );
    for pair in pairs {
        if missing_outputs(base, &pair)?.is_empty() {
            complete += 1;
            continue;
        }
        let lock = pair_lock_key(base, &pair);
        if !try_lock(base, &lock)? {
            println!(
                "{}: skipped busy pair ({})",
                pair.table,
                base.describe(&lock)
            );
            busy += 1;
            continue;
        }
        // A previous owner may have finished between the first check and lock acquisition.
        let generated = missing_outputs(base, &pair).and_then(|missing| {
            if missing.is_empty() {
                Ok(Vec::new())
            } else {
                pool.install(|| generate_pair(source, &pair, &missing))
            }
        });
        let unlock = block_on(base.store.delete(&lock)).map_err(|error| base.error(&lock, error));
        let generated = match generated {
            Ok(stats) => {
                unlock?;
                stats
            }
            Err(error) => {
                if let Err(unlock) = unlock {
                    eprintln!("{}: could not release pair lock: {unlock}", pair.table);
                }
                return Err(error);
            }
        };
        complete += usize::from(generated.is_empty());
        results.extend(generated);
    }
    println!("Skipped twin-pair jobs: {complete} complete, {busy} busy.");
    let mut total = AggregateStats::default();
    for stats in &results {
        total.add(stats);
    }
    let lowest_compression = results
        .iter()
        .filter(|stats| stats.totals.unique_positions > 0)
        .map(|stats| {
            (
                stats.tablename.as_str(),
                stats.totals.bytes as f64 * 8.0 / stats.totals.unique_positions as f64,
            )
        })
        .max_by(|a, b| a.1.total_cmp(&b.1));
    println!(
        "{}",
        generation_summary(
            &total,
            start.elapsed(),
            pool.current_num_threads(),
            lowest_compression
        )
    );
    Ok(results)
}

fn pair_ids(pair: &TablePair) -> impl Iterator<Item = &TableId> {
    std::iter::once(&pair.table).chain(pair.twin.iter())
}

fn missing_outputs(base: &Location, pair: &TablePair) -> EgtbResult<Vec<TableId>> {
    let mut missing = Vec::new();
    for id in pair_ids(pair) {
        let key = base.key(&id.relative_path("ggegtb"));
        match block_on(base.store.head(&key)) {
            Ok(_) => {}
            Err(object_store::Error::NotFound { .. }) => missing.push(*id),
            Err(error) => return Err(base.error(&key, error)),
        }
    }
    Ok(missing)
}

fn pair_lock_key(base: &Location, pair: &TablePair) -> ObjectPath {
    base.key(&pair.table.relative_path("lock"))
}

fn try_lock(base: &Location, key: &ObjectPath) -> EgtbResult<bool> {
    let started = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let payload = serde_json::to_vec(&serde_json::json!({
        "pid": std::process::id(),
        "started_unix_seconds": started.as_secs(),
    }))?;
    let options = PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    };
    match block_on(base.store.put_opts(key, payload.into(), options)) {
        Ok(_) => Ok(true),
        Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
        Err(error) => Err(base.error(key, error)),
    }
}

/// Solves both twins in the caller's Rayon pool and publishes only missing outputs.
fn generate_pair(
    source: &Source,
    pair: &TablePair,
    missing: &[TableId],
) -> EgtbResult<Vec<TableStats>> {
    let dependency_start = Instant::now();
    let mut dependencies: Vec<_> = pair_ids(pair).flat_map(|id| id.dependencies()).collect();
    dependencies.sort();
    dependencies.dedup();
    let reader = BatchReader::open(source, &dependencies)?;
    println!(
        "{}: loaded {} bytes of dependencies.",
        pair.table,
        reader.resident_bytes(),
    );
    let dependency_time = dependency_start.elapsed();

    let base = source.base();
    let setup_start = Instant::now();
    let mut tables = pair_ids(pair)
        .map(|id| TableState::new(TableIndexer::new(id)))
        .collect::<EgtbResult<Vec<_>>>()?;
    let setup_time = setup_start.elapsed();
    let timings = solve_pair(&mut tables, pair.reflected, &reader)?;
    let init_time = setup_time + timings.init;

    let finalize_start = Instant::now();
    let mut stats = Vec::with_capacity(missing.len());
    for (id, table) in pair_ids(pair).zip(tables) {
        if !missing.contains(id) {
            continue;
        }
        let (egt, values, maxima, invalid) = table.into_parts();
        println!("{id}: finalizing outcomes, statistics and compression...");
        stats.push(finalization::finalize_table(
            base, id, &egt, values, maxima, invalid,
        )?);
    }
    let finalize_time = finalize_start.elapsed();
    let total_time = dependency_time + init_time + timings.propagation + finalize_time;
    let names = pair_ids(pair)
        .map(TableId::name)
        .collect::<Vec<_>>()
        .join(", ");
    println!(
        "{}: phase timings: dependencies {:.3}s, init {:.3}s, propagation {:.3}s, finalize/publish {:.3}s, total {:.3}s.",
        names,
        dependency_time.as_secs_f64(),
        init_time.as_secs_f64(),
        timings.propagation.as_secs_f64(),
        finalize_time.as_secs_f64(),
        total_time.as_secs_f64(),
    );
    for table_stats in &stats {
        print_table_stats(table_stats);
    }
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selection::TableSelector;
    use crate::{EgtbError, Tablebase};

    #[test]
    fn parallel_missing_dependency_returns_error_without_generating() {
        let dir = crate::TestDir::new("parallel_missing_dependency");
        let mut generator = Tablebase::new(&dir.0).unwrap();
        generator.set_threads(4);
        let result = generator.generate(&["KQ_K"]);
        assert!(
            matches!(result, Err(EgtbError::DependencyUnavailable { dependency, source })
            if dependency == "K_K" && matches!(*source, EgtbError::FileNotFound(_)))
        );
        for name in ["K_K", "KQ_K", "K_KQ"] {
            let id = TableId::parse(name).unwrap();
            assert!(!dir.0.join(id.relative_path("ggegtb")).exists());
            assert!(!dir.0.join(id.relative_path("lock")).exists());
        }
    }

    #[test]
    fn a_pair_locked_via_one_twin_is_busy_via_the_other() {
        let dir = crate::TestDir::new("busy_pair");
        let location = Location::parse(&dir.0).unwrap();
        location.ensure_format().unwrap();
        let pair = plan_pairs([TableId::parse("K_KQ").unwrap()]).pop().unwrap();
        let lock = pair_lock_key(&location, &pair);
        std::thread::scope(|scope| {
            let contender = scope.spawn(|| try_lock(&location, &lock).unwrap());
            let acquired = try_lock(&location, &lock).unwrap();
            assert_ne!(acquired, contender.join().unwrap());
        });

        let mut generator = Tablebase::new(&dir.0).unwrap();
        assert!(generator.generate(&["KQ_K"]).unwrap().is_empty());
        assert_eq!(missing_outputs(&location, &pair).unwrap().len(), 2);
        assert!(!try_lock(&location, &lock).unwrap());

        block_on(location.store.delete(&lock)).unwrap();
        assert_eq!(generator.generate(&["K_K", "KQ_K"]).unwrap().len(), 3);
        assert!(try_lock(&location, &lock).unwrap());
    }

    #[test]
    fn three_piece_generation_matches_established_outcome_counts() {
        let dir = crate::TestDir::new("three_piece_generation");
        let location = Location::parse(&dir.0).unwrap();
        let mut generator = Tablebase::new(&dir.0).unwrap();
        generator.set_threads(2);
        generator.generate(&["2", "3"]).unwrap();

        // Index range, wins, draws, losses, excluded indices.
        for (material, expected) in [
            ("K_K", [462, 0, 462, 0, 0]),
            ("KQ_K", [28644, 18081, 0, 0, 10563]),
            ("K_KQ", [28644, 0, 2896, 25160, 588]),
            ("KR_K", [28644, 21959, 0, 0, 6685]),
            ("K_KR", [28644, 0, 2796, 25260, 588]),
            ("KB_K", [28644, 0, 24178, 0, 4466]),
            ("K_KB", [28644, 0, 28056, 0, 588]),
            ("KN_K", [28644, 0, 25750, 0, 2894]),
            ("K_KN", [28644, 0, 28056, 0, 588]),
            ("KP_K", [93744, 62480, 19184, 0, 12080]),
            ("K_KP", [93744, 0, 35210, 48802, 9732]),
        ] {
            let mut totals = [0; 5];
            for id in TableSelector::parse(material).unwrap().resolve().unwrap() {
                let stats = crate::stats::read_stats(&location, &id).unwrap();
                let t = stats.totals;
                let excluded = t.invalid_opposite_check
                    + t.invalid_ep_squares_occupied
                    + t.invalid_illegal_ep_capture
                    + t.redundant;
                for (total, count) in
                    totals
                        .iter_mut()
                        .zip([stats.index_range, t.win, t.draw, t.loss, excluded])
                {
                    *total += count;
                }
            }
            assert_eq!(totals, expected, "{material}");
        }
    }
}
