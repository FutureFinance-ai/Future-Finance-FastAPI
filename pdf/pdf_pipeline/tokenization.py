from __future__ import annotations

from typing import Dict, List, Tuple


def words_to_rows(words: List[Dict[str, object]], y_tolerance: float = 2.5) -> List[Dict[str, object]]:
    """
    Group words (with x0,x1,top,bottom,text) into rough rows by baseline proximity.
    Returns a list of { y, tokens } where tokens are the original word dicts.
    """
    if not words:
        return []
    words_sorted = sorted(words, key=lambda w: (float(w.get("top", 0.0)), float(w.get("x0", 0.0))))
    rows: List[Dict[str, object]] = []
    for w in words_sorted:
        top = float(w.get("top", 0.0))
        if not rows or abs(rows[-1]["y"] - top) > y_tolerance:
            rows.append({"y": top, "tokens": [w]})
        else:
            rows[-1]["tokens"].append(w)
            # keep representative baseline as the first token's top
    return rows


def infer_column_bands(header_tokens: List[Dict[str, object]], max_cols: int = 10) -> List[Tuple[float, float]]:
    """
    Infer column bands from header tokens by clustering x-midpoints in reading order.
    Simple gap-based approach to avoid heavy dependencies.
    """
    mids = sorted(((float(t.get("x0", 0.0)) + float(t.get("x1", 0.0))) / 2.0) for t in header_tokens)
    if not mids:
        return []
    # compute gaps and split at large gaps (>= 15pt)
    gaps = [(mids[i+1] - mids[i]) for i in range(len(mids)-1)]
    split_indices = [i for i, g in enumerate(gaps) if g >= 15.0]
    clusters: List[List[float]] = []
    start = 0
    for idx in split_indices:
        clusters.append(mids[start:idx+1])
        start = idx + 1
    clusters.append(mids[start:])
    # limit columns
    if len(clusters) > max_cols:
        clusters = clusters[:max_cols]
    # turn clusters into bands with padding
    bands: List[Tuple[float, float]] = []
    for c in clusters:
        if not c:
            continue
        xmin = min(c) - 10.0
        xmax = max(c) + 10.0
        if not bands or xmin > bands[-1][1]:
            bands.append((xmin, xmax))
        else:
            # merge overlapping
            prev = bands[-1]
            bands[-1] = (prev[0], max(prev[1], xmax))
    return bands


def assign_tokens_to_columns(tokens: List[Dict[str, object]], bands: List[Tuple[float, float]]) -> List[List[Dict[str, object]]]:
    cols: List[List[Dict[str, object]]] = [[] for _ in bands]
    for t in tokens:
        mid = (float(t.get("x0", 0.0)) + float(t.get("x1", 0.0))) / 2.0
        for i, (xmin, xmax) in enumerate(bands):
            if xmin <= mid <= xmax:
                cols[i].append(t)
                break
    return cols


