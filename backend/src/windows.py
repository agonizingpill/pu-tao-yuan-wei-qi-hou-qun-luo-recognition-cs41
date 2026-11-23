"""Shift windows and irrigation overlap rules.

Day shifts run 06:00-18:00 and night shifts 18:00-06:00 the next day, all in
China Standard Time (UTC+08:00, no daylight saving).
"""

from datetime import date, datetime, time, timedelta, timezone

CST = timezone(timedelta(hours=8))
SHIFT_START_HOUR = {'day': 6, 'night': 18}
SHIFT_HOURS = 12


def _slot_key(slot):
    return slot.value if hasattr(slot, 'value') else str(slot)


def shift_bounds(observed_day, slot):
    """Return the [start, end) instants covered by one day/night shift."""
    if isinstance(observed_day, str):
        observed_day = date.fromisoformat(observed_day)
    start = datetime.combine(observed_day, time(SHIFT_START_HOUR[_slot_key(slot)]), tzinfo=CST)
    return start, start + timedelta(hours=SHIFT_HOURS)


def overlaps(start_a, end_a, start_b, end_b):
    return start_a < end_b and start_b < end_a
