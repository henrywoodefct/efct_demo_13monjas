import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Any


ALLOWED_LEVELS = {"Normal", "Watch", "Elevated", "Critical"}

SEVERITY_RANK = {
    "Normal": 0,
    "Watch": 1,
    "Elevated": 2,
    "Critical": 3,
}

EFFORT_RANK = {
    "Low": 0,
    "Medium": 1,
    "High": 2,
}

URGENCY_RANK = {
    "Now": 0,
    "Next 3h": 1,
    "Monitor": 2,
}


def _parse_dt(value: str) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _safe_int(x: Any) -> int | None:
    return x if isinstance(x, int) else None


def _get_status(card: dict) -> dict:
    insight = card.get("insight") if isinstance(card.get("insight"), dict) else {}
    status = insight.get("status") if isinstance(insight.get("status"), dict) else {}
    return status


def _card_level(card: dict) -> str:
    level = _get_status(card).get("level")
    return level if isinstance(level, str) else "Normal"


def _card_score(card: dict) -> int | None:
    return _safe_int(_get_status(card).get("score_0_100"))


def _overall_status(cards: list[dict]) -> str:
    best = "Normal"
    best_rank = -1
    for c in cards:
        lvl = _card_level(c)
        rank = SEVERITY_RANK.get(lvl, 0)
        if rank > best_rank:
            best_rank = rank
            best = lvl if lvl in ALLOWED_LEVELS else "Normal"
    return best


def _counts_by_level(cards: list[dict]) -> dict:
    counts = {k: 0 for k in ["Normal", "Watch", "Elevated", "Critical"]}
    for c in cards:
        lvl = _card_level(c)
        if lvl in counts:
            counts[lvl] += 1
        else:
            # Unknown levels count as Normal to avoid breaking downstream
            counts["Normal"] += 1
    return counts


def _normalize_effort(effort: Any) -> str:
    if isinstance(effort, str):
        e = effort.strip().title()
        if e in EFFORT_RANK:
            return e
    return "Medium"

def _normalize_text(x: Any) -> str:
    return x.strip().lower() if isinstance(x, str) else ""


def _infer_urgency(action_obj: dict, card: dict) -> str:
    """
    Heuristic urgency inference using the action's 'when' and card time_horizon.
    """
    when_txt = _normalize_text(action_obj.get("when"))
    why_txt = _normalize_text(action_obj.get("why"))
    action_txt = _normalize_text(action_obj.get("action"))

    # Strong "Now" signals
    now_markers = [
        "now", "immediately", "right away",
        "10", "15", "minutes", "minute",
        "shortly before", "before reservation", "check-in",
        "call", "confirm",
    ]
    if any(m in when_txt for m in now_markers) or "shortly before" in action_txt:
        return "Now"

    # Strong "Next 3h" signals
    next3h_markers = ["next 3 hours", "next 3h", "next 2 hours", "next 2h", "during peak", "peak window", "tonight"]
    if any(m in when_txt for m in next3h_markers):
        return "Next 3h"

    # Use card time horizon as fallback
    insight = card.get("insight") if isinstance(card.get("insight"), dict) else {}
    horizon = _normalize_text(insight.get("time_horizon"))

    if "0–3h" in horizon or "0-3h" in horizon or "0–3" in horizon or "0-3" in horizon:
        return "Next 3h"

    # If the language sounds like monitoring / uncertainty management
    monitor_markers = ["monitor", "keep an eye", "if conditions", "may", "could", "watch"]
    if any(m in why_txt for m in monitor_markers) or any(m in action_txt for m in monitor_markers):
        return "Monitor"

    # Default
    return "Next 3h"


def _extract_actions(cards: list[dict]) -> list[dict]:
    """
    Collect actions from each card and attach enough context for a UI / summary later.
    """
    actions: list[dict] = []
    for c in cards:
        insight = c.get("insight") if isinstance(c.get("insight"), dict) else {}
        iid = insight.get("id") if isinstance(insight.get("id"), str) else "unknown"
        title = insight.get("title") if isinstance(insight.get("title"), str) else "Untitled"
        category = insight.get("category") if isinstance(insight.get("category"), str) else "General"

        level = _card_level(c)
        score = _card_score(c)
        gen = c.get("generated_at_local")

        suggested = insight.get("suggested_actions")
        if not isinstance(suggested, list):
            continue

        for a in suggested:
            if not isinstance(a, dict):
                continue
            action_text = a.get("action")
            if not isinstance(action_text, str) or not action_text.strip():
                continue

            effort = _normalize_effort(a.get("effort"))
            urgency = _infer_urgency(a, c)
            actions.append(
                {
                    "action": action_text.strip(),
                    "when": a.get("when"),
                    "why": a.get("why"),
                    "urgency": urgency,
                    "effort": effort,
                    "tradeoff": a.get("tradeoff"),
                    # Context for rollups / UI linking later
                    "source": {
                        "insight_id": iid,
                        "insight_title": title,
                        "category": category,
                        "status_level": level,
                        "score_0_100": score,
                        "generated_at_local": gen,
                    },
                }
            )
    return actions


def _ranked_top_actions(cards: list[dict], top_n: int = 3) -> list[dict]:
    actions = _extract_actions(cards)

    # Optional: de-duplicate exact same action text across cards (keep highest severity)
    dedup: dict[str, dict] = {}
    for a in actions:
        key = (a.get("action") or "").strip().lower()
        if not key:
            continue
        existing = dedup.get(key)
        if not existing:
            dedup[key] = a
            continue

        # Keep the one with higher severity; tie-breaker: lower effort; then higher score
        def _key(x: dict) -> tuple:
            src = x.get("source", {})
            lvl = src.get("status_level", "Normal")
            sev = SEVERITY_RANK.get(lvl, 0)
            eff = EFFORT_RANK.get(x.get("effort", "Medium"), 1)
            score = src.get("score_0_100")
            score_val = score if isinstance(score, int) else -1
            return (sev, -eff, score_val)

        if _key(a) > _key(existing):
            dedup[key] = a

    deduped = list(dedup.values())

    # Rank: higher severity first, then lower effort, then higher score, then stable
    def sort_key(a: dict) -> tuple:
        src = a.get("source", {})
        lvl = src.get("status_level", "Normal")
        sev = SEVERITY_RANK.get(lvl, 0)
        eff = EFFORT_RANK.get(a.get("effort", "Medium"), 1)
        urg = URGENCY_RANK.get(a.get("urgency", "Next 3h"), 1)

        score = src.get("score_0_100")
        score_val = score if isinstance(score, int) else -1

        return (urg, -sev, eff, -score_val, (a.get("action") or ""))

    deduped.sort(key=sort_key)
    return deduped[:top_n]

def _urgency_summary(top_actions: list[dict]) -> str:
    counts = {"Now": 0, "Next 3h": 0, "Monitor": 0}
    for a in top_actions:
        u = a.get("urgency")
        if u in counts:
            counts[u] += 1
    parts = []
    if counts["Now"]:
        parts.append(f"{counts['Now']} Now")
    if counts["Next 3h"]:
        parts.append(f"{counts['Next 3h']} Next 3h")
    if counts["Monitor"]:
        parts.append(f"{counts['Monitor']} Monitor")
    return " • ".join(parts) if parts else "No actions"


def _top_category(top_actions: list[dict]) -> str:
    # Pick the category that appears most among top actions (ties broken by severity)
    if not top_actions:
        return "Operations"
    tally: dict[str, int] = {}
    best_cat = "Operations"
    best_score = -1
    for a in top_actions:
        src = a.get("source", {}) if isinstance(a.get("source"), dict) else {}
        cat = src.get("category") if isinstance(src.get("category"), str) else "Operations"
        lvl = src.get("status_level") if isinstance(src.get("status_level"), str) else "Normal"
        sev = SEVERITY_RANK.get(lvl, 0)
        tally[cat] = tally.get(cat, 0) + 1
        score = tally[cat] * 10 + sev  # count dominates; severity breaks ties a bit
        if score > best_score:
            best_score = score
            best_cat = cat
    return best_cat


def _build_summary(overall_status: str, top_actions: list[dict]) -> str:
    cat = _top_category(top_actions)

    if overall_status == "Normal":
        if top_actions:
            return f"Conditions look stable; keep normal ops and monitor {cat.lower()} for changes."
        return "Conditions look stable; keep normal ops and monitor for changes."

    if overall_status == "Watch":
        if cat.lower() == "delivery":
            return "Watch conditions tonight: delivery reliability may vary; apply quick buffers and prioritize nearby zones."
        if cat.lower() == "reservations":
            return "Watch conditions tonight: arrival timing may vary; use proactive check-ins and small buffers to protect flow."
        return f"Watch conditions tonight: increased variability likely; take quick steps to protect {cat.lower()}."

    if overall_status == "Elevated":
        return f"Elevated risk tonight: disruptions are likely; prioritize buffers and proactive messaging to protect {cat.lower()}."

    # Critical
    return f"Critical conditions: significant disruption likely; enact contingency ops and communicate early to protect {cat.lower()}."



def main() -> None:
    # Usage: python build_feed.py <config_path> <cards_dir> <feed_path>
    if len(sys.argv) != 4:
        raise SystemExit("Usage: python build_feed.py <config_path> <cards_dir> <feed_path>")

    config_path = Path(sys.argv[1])
    cards_dir = Path(sys.argv[2])
    feed_path = Path(sys.argv[3])

    if not config_path.exists():
        raise SystemExit(f"Config not found: {config_path}")
    if not cards_dir.exists():
        raise SystemExit(f"Cards dir not found: {cards_dir}")

    cfg = json.loads(config_path.read_text(encoding="utf-8"))

    # Read all card JSON files
    card_files = sorted(cards_dir.glob("*.json"))
    cards: list[dict] = []
    for f in card_files:
        try:
            cards.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception:
            # Skip invalid JSON rather than crashing feed build
            continue

    # Dedupe by insight.id (keep newest generated_at_local)
    by_id: dict[str, dict] = {}
    for c in cards:
        insight = c.get("insight") if isinstance(c, dict) else None
        if not isinstance(insight, dict):
            continue
        iid = insight.get("id")
        if not isinstance(iid, str) or not iid.strip():
            continue

        existing = by_id.get(iid)
        if not existing:
            by_id[iid] = c
            continue

        dt_new = _parse_dt(c.get("generated_at_local", ""))
        dt_old = _parse_dt(existing.get("generated_at_local", ""))

        if dt_new and (not dt_old or dt_new > dt_old):
            by_id[iid] = c

    deduped_cards = list(by_id.values())

    # Feed timestamp = newest card timestamp if possible, else "now"
    newest = None
    for c in deduped_cards:
        dt = _parse_dt(c.get("generated_at_local", ""))
        if dt and (newest is None or dt > newest):
            newest = dt

    if newest is None:
        newest = datetime.now(timezone.utc)

    top_actions = _ranked_top_actions(deduped_cards, top_n=3)
    overall = _overall_status(deduped_cards)

    rollups = {
        "overall_status": overall,
        "counts_by_level": _counts_by_level(deduped_cards),
        "top_actions": top_actions,
        "urgency_summary": _urgency_summary(top_actions),
        "summary": _build_summary(overall, top_actions),
    }


    feed = {
        "schema_version": "ui-ready-feed-v1",
        "site_id": cfg.get("site_id", "unknown_site"),
        "site_name": cfg.get("site_name", "Unknown Site"),
        "generated_at_local": newest.isoformat(),
        "cards": deduped_cards,
        "rollups": rollups,
    }

    feed_path.parent.mkdir(parents=True, exist_ok=True)
    feed_path.write_text(json.dumps(feed, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"✅ Feed written: {feed_path}")
    print(f"✅ Cards included: {len(deduped_cards)}")
    print(f"✅ Rollups: overall_status={rollups['overall_status']} | top_actions={len(rollups['top_actions'])}")


if __name__ == "__main__":
    main()
