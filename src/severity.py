from __future__ import annotations

from dataclasses import dataclass

ALLOWED_LEVELS = ("Normal", "Watch", "Elevated", "Critical")

# Default global thresholds (score_0_100)
# 0–24   Normal
# 25–49  Watch
# 50–74  Elevated
# 75–100 Critical
DEFAULT_THRESHOLDS = {
    "watch_min": 25,
    "elevated_min": 50,
    "critical_min": 75,
}


@dataclass(frozen=True)
class SeverityResult:
    level: str
    icon: str


def clamp_score(score: int) -> int:
    return max(0, min(100, int(score)))


def classify_score(score_0_100: int) -> SeverityResult:
    """
    Global, consistent mapping from numeric score to status.level + icon.
    """
    s = clamp_score(score_0_100)

    if s >= DEFAULT_THRESHOLDS["critical_min"]:
        return SeverityResult("Critical", "🔴")
    if s >= DEFAULT_THRESHOLDS["elevated_min"]:
        return SeverityResult("Elevated", "🟠")
    if s >= DEFAULT_THRESHOLDS["watch_min"]:
        return SeverityResult("Watch", "🟡")
    return SeverityResult("Normal", "🟢")
