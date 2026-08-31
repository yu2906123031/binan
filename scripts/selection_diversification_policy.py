from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable


def _text(value: Any) -> str:
    return str(value or '').strip().upper()


def _group_key(candidate: Any) -> str:
    """Resolve an explicit diversification group without guessing from symbols."""
    for attr, prefix in (
        ('portfolio_correlation_group', 'CORR'),
        ('correlation_group', 'CORR'),
        ('portfolio_narrative_bucket', 'NARR'),
        ('narrative_bucket', 'NARR'),
        ('sector', 'SECTOR'),
        ('sector_name', 'SECTOR'),
        ('category', 'CATEGORY'),
        ('theme', 'THEME'),
        ('market_segment', 'SEGMENT'),
    ):
        value = _text(getattr(candidate, attr, ''))
        if value and value not in {'UNKNOWN', 'NONE', 'N/A', 'OTHER'}:
            return f'{prefix}:{value}'
    return ''


def _upstream_score(candidate: Any) -> float:
    """Return the latest score produced by the relative-selection layer.

    ``rerank_candidate_cohort`` is called repeatedly while a scan cohort grows.
    Reconstructing the upstream score prevents an early partial-cohort score from
    becoming a stale diversification baseline while preserving direct-call
    idempotence when no relative-selection metadata exists.
    """
    relative_base = getattr(candidate, 'relative_selection_base_score', None)
    relative_multiplier = getattr(candidate, 'relative_selection_multiplier', None)
    if relative_base is not None and relative_multiplier is not None:
        try:
            return float(relative_base) * float(relative_multiplier)
        except (TypeError, ValueError):
            pass
    if hasattr(candidate, 'diversification_base_score'):
        return float(getattr(candidate, 'diversification_base_score', 0.0) or 0.0)
    return float(getattr(candidate, 'score', 0.0) or 0.0)


def apply_selection_diversification(candidates: Iterable[Any]) -> list[Any]:
    """Softly deweight duplicate opportunities within the same explicit group.

    Candidates are processed in their pre-diversification score order separately
    for triggered and non-triggered stages. This preserves the strategy's
    trigger-fired priority while keeping the top-quality member of each group
    untouched. Missing group metadata is deliberately neutral.
    """
    cohort = list(candidates)
    if len(cohort) < 2:
        return cohort

    for candidate in cohort:
        candidate.diversification_base_score = round(_upstream_score(candidate), 4)
        candidate.score = candidate.diversification_base_score

    grouped_counts: dict[tuple[bool, str], int] = defaultdict(int)
    ordered = sorted(
        cohort,
        key=lambda item: (
            1 if bool(getattr(item, 'trigger_fired', False)) and not bool(getattr(item, 'pretrigger_watch', False)) else 0,
            float(getattr(item, 'diversification_base_score', getattr(item, 'score', 0.0)) or 0.0),
        ),
        reverse=True,
    )

    for candidate in ordered:
        triggered = bool(getattr(candidate, 'trigger_fired', False)) and not bool(getattr(candidate, 'pretrigger_watch', False))
        group = _group_key(candidate)
        duplicate_index = 0
        multiplier = 1.0
        if group:
            key = (triggered, group)
            duplicate_index = grouped_counts[key]
            grouped_counts[key] += 1
            if duplicate_index == 1:
                multiplier = 0.97
            elif duplicate_index == 2:
                multiplier = 0.93
            elif duplicate_index >= 3:
                multiplier = 0.89

        base = float(getattr(candidate, 'diversification_base_score', getattr(candidate, 'score', 0.0)) or 0.0)
        candidate.selection_diversification_group = group
        candidate.selection_diversification_duplicate_index = duplicate_index
        candidate.selection_diversification_multiplier = multiplier
        candidate.score = round(base * multiplier, 4)
        reasons = [
            reason for reason in list(getattr(candidate, 'reasons', []) or [])
            if not str(reason).startswith('selection_diversification=')
        ]
        reasons.append(
            'selection_diversification='
            f'group={group or "-"}:duplicate_index={duplicate_index}:multiplier={multiplier:.4f}'
        )
        candidate.reasons = reasons
    return cohort


def install_selection_diversification_hook(relative_selection_module: Any) -> None:
    original = getattr(relative_selection_module, 'rerank_candidate_cohort', None)
    if not callable(original) or getattr(original, '_selection_diversification_hook', False):
        return

    def rerank_with_diversification(candidates: Iterable[Any]):
        cohort = original(candidates)
        return apply_selection_diversification(cohort)

    rerank_with_diversification._selection_diversification_hook = True  # type: ignore[attr-defined]
    relative_selection_module.rerank_candidate_cohort = rerank_with_diversification
