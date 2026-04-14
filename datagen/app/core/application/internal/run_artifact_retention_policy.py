from collections.abc import Collection, Sequence


def select_run_ids_to_remove(
    run_ids: Sequence[str],
    protected_run_ids: Collection[str],
    min_retained_runs: int,
) -> list[str]:
    """Выбирает run_id для удаления: protected run_id сохраняются, остальные добираются свежими до минимума."""
    sorted_run_ids = sorted(run_ids)
    run_ids_to_keep = set(protected_run_ids).intersection(sorted_run_ids)
    remaining_slots = max(min_retained_runs - len(run_ids_to_keep), 0)

    for run_id in reversed(sorted_run_ids):
        if run_id in run_ids_to_keep:
            continue
        if remaining_slots <= 0:
            break
        run_ids_to_keep.add(run_id)
        remaining_slots -= 1

    return [run_id for run_id in sorted_run_ids if run_id not in run_ids_to_keep]
