from __future__ import annotations

from typing import List, Dict

import numpy as np


def compute_twrr(cashflows: List[float], valuations: List[float]) -> float:
    if not cashflows or not valuations or len(cashflows) != len(valuations):
        return float("nan")
    # Placeholder: proper TWRR requires subperiod returns. This is a stub.
    returns = []
    for i in range(1, len(valuations)):
        if valuations[i - 1] == 0:
            continue
        r = (valuations[i] - valuations[i - 1] - cashflows[i]) / valuations[i - 1]
        returns.append(1 + r)
    if not returns:
        return float("nan")
    return float(np.prod(returns) - 1)


