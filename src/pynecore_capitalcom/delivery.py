"""Per-deal fill delivery queue: every fill of an own deal reaches the engine once, in order.

The venue publishes a deal's fills (its entry, then each reduction and close)
in ``/history/activity``, but a read can return them incomplete: an entry no
source prices yet, a close without its size, or only a later fill of the deal
while an earlier one is still missing from the response. A fill booked out of
order corrupts the engine's book for good: a reduction booked before its entry
leaves the book short once the entry arrives, and a terminal close booked
before an earlier reduction tears the row down and turns the reduction into a
suppressed replay. A fill that cannot be booked when read is therefore queued
on its position row, ordered by the venue's activity time (``dateUTC``), and
delivered only once every fill before it is.

The queue is persisted on the row's ``extras`` in three keys, read and written
only through this module:

* :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_ENTRY_PRICE_PENDING`: the
  entry at the head of the queue, while no source has priced it;
* :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_DEFERRED_CLOSES`: the queued
  reductions and closes, ``{identity: dateUTC}``, each under its identity and
  its sizeless copy's (:func:`sizeless_close_identity`);
* :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_BOOKED_CLOSES`: the delivered
  closes, so a copy read again is a replay.

Two functions decide, every other path asks them:

* :func:`hold_reason` decides whether a fill read now is the next deliverable
  one of its deal;
* :func:`delivery_pending` decides whether a deal still owes a fill a read can
  return, its queued entry included. Every runtime terminal verdict on a deal
  (the close booking that tears the row down, the breadcrumb teardown of the
  snapshot pass, the fill synthesized for an accepted own close, the
  disappearance verdict) waits while it does: the teardown would settle the
  owed fill unbooked. A fill older than the venue's maximum lookback can no
  longer be read and owes nothing; the terminal verdicts then decide as
  before. The startup retirement of a vanished deal settles the queue: the
  engine adopts the venue's book at startup, so it owes the engine nothing.
  A cancel never retires a working-order row whose execution was already
  read (:func:`entry_executed`): its 404 is the execution.

:func:`book_fill` and :func:`book_entry` are the only writes that deliver a
fill, :func:`enqueue_entry`, :func:`enqueue_fill` and
:func:`enqueue_identities` (the closes a held partly netted entry deferred,
once the entry is booked) the only ones that queue it, and
:func:`settle_undelivered` the only one that drops the queue unbooked (a
teardown, after the engine's book no longer holds the deal).
"""
from time import time as epoch_time

from .helpers import (
    BOOKED_CLOSES_RETAINED,
    EXTRAS_KEY_BOOKED_CLOSES,
    EXTRAS_KEY_DEFERRED_CLOSES,
    EXTRAS_KEY_ENTRY_EXECUTED_UTC,
    EXTRAS_KEY_ENTRY_PRICE_PENDING,
    _ACTIVITY_MAX_PERIOD_S,
    _parse_iso_timestamp,
    _wire_float,
)
from .models import _close_identity

#: :func:`hold_reason`: the deal's entry is queued for its price.
HOLD_ENTRY_PRICE_PENDING = 'entry_price_pending'

#: :func:`hold_reason`: a queued close read again without its size.
HOLD_SIZE_UNKNOWN = 'size_unknown'

#: :func:`hold_reason`: an earlier fill of the deal is still queued.
HOLD_BEHIND_QUEUED_FILL = 'behind_queued_fill'


def activity_size(activity: dict) -> float | None:
    """The size an activity row states itself, ``None`` when it states none.

    :param activity: A ``/history/activity`` row.
    :return: The positive size at the top level or in ``details``.
    """
    details = activity.get('details') or {}
    size = _wire_float(activity.get('size') or details.get('size'), finite=True)
    return size if size is not None and size > 0.0 else None


def sizeless_close_identity(activity: dict) -> str:
    """The identity a copy of the close ``activity`` that states no size carries."""
    details = activity.get('details') or {}
    return _close_identity({**activity, 'size': None, 'details': {**details, 'size': None}})


def _fill_identities(activity: dict) -> tuple[str, ...]:
    """The identities of a fill: its own and its sizeless copy's."""
    return tuple(dict.fromkeys((_close_identity(activity), sizeless_close_identity(activity))))


def entry_pending_since(extras: dict) -> str | None:
    """The ``dateUTC`` of the queued entry of a row, while it is unbooked.

    :param extras: A position row's ``extras``.
    :return: The date from
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_ENTRY_PRICE_PENDING`,
        ``None`` when no entry is queued or the entry has been booked since.
    """
    pending = extras.get(EXTRAS_KEY_ENTRY_PRICE_PENDING)
    if pending is None or extras.get('entry_filled_at') is not None:
        return None
    return str(pending)


def entry_executed(extras: dict) -> bool:
    """Whether a row's entry executed on the venue: its fill booked or queued for its price.

    A working-order row carries its execution before its promotion onto the
    deal it opened: booked (``entry_filled_at``) when no clean position
    ``dealId`` was found in that poll, or queued
    (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_ENTRY_PRICE_PENDING`) while
    no source prices it. Such a row is a position, whatever its ``kind`` says:
    a cancel's 404 is the execution, never a cancelled verdict.

    :param extras: A position or working-order row's ``extras``.
    """
    return (extras.get('entry_filled_at') is not None
            or extras.get(EXTRAS_KEY_ENTRY_PRICE_PENDING) is not None)


def entry_execution_date(extras: dict) -> str | None:
    """The venue ``dateUTC`` of a row's entry execution, booked or queued.

    :param extras: A position or working-order row's ``extras``.
    :return: The date kept by the booking
        (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_ENTRY_EXECUTED_UTC`) or
        the queued entry's
        (:data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_ENTRY_PRICE_PENDING`),
        ``None`` when neither was read: then only the local booking stamp
        (``entry_filled_at``) dates the execution.
    """
    executed = extras.get(EXTRAS_KEY_ENTRY_EXECUTED_UTC) or extras.get(
        EXTRAS_KEY_ENTRY_PRICE_PENDING)
    return str(executed) if executed else None


def queued_fills(extras: dict) -> dict[str, str]:
    """The reductions and closes still queued on a row, by identity.

    :param extras: A position row's ``extras``.
    :return: The ``dateUTC`` of each fill in
        :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_DEFERRED_CLOSES` not yet
        in :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_BOOKED_CLOSES`.
    """
    booked = set(extras.get(EXTRAS_KEY_BOOKED_CLOSES) or ())
    return {close_id: str(date_utc or '')
            for close_id, date_utc in (extras.get(EXTRAS_KEY_DEFERRED_CLOSES) or {}).items()
            if close_id not in booked}


def readable_queued_dates(extras: dict) -> list[str]:
    """The ``dateUTC`` of each queued fill a read can still return, ascending.

    :param extras: A position row's ``extras``.
    :return: The dates of :func:`queued_fills` not older than the venue's
        maximum lookback; an older fill can no longer be read.
    """
    readable_since = epoch_time() - _ACTIVITY_MAX_PERIOD_S
    return sorted(date_utc for date_utc in queued_fills(extras).values()
                  if date_utc and _parse_iso_timestamp(date_utc) > readable_since)


def delivery_pending(extras: dict) -> bool:
    """Whether a row's deal still owes the engine a fill a read can return.

    The queued entry or a queued later fill of the deal is still readable: its
    activity delivers it, in order, and the close delivering the last one
    tears the row down. A queue whose every item is older than the venue's
    maximum lookback owes nothing a read could return.

    :param extras: A position row's ``extras``.
    """
    readable_since = epoch_time() - _ACTIVITY_MAX_PERIOD_S
    return any(date_utc and _parse_iso_timestamp(date_utc) > readable_since
               for date_utc in queue_dates(extras))


def is_delivered(extras: dict, activity: dict) -> bool:
    """Whether the fill ``activity`` (or a copy of it) was already booked."""
    return identity_delivered(extras, _close_identity(activity))


def identity_delivered(extras: dict, close_id: str) -> bool:
    """Whether the fill of identity ``close_id`` was already booked."""
    return close_id in (extras.get(EXTRAS_KEY_BOOKED_CLOSES) or ())


def hold_reason(extras: dict, activity: dict) -> str | None:
    """Why the reduction or close ``activity`` is not the next deliverable fill of its deal.

    The one decision of the delivery order, in this precedence:

    * the deal's entry is queued for its price (:data:`HOLD_ENTRY_PRICE_PENDING`):
      every later fill waits behind it;
    * a fill already booked never waits, its copy is a replay;
    * a copy of a queued fill that states no size (:data:`HOLD_SIZE_UNKNOWN`)
      cannot tell the reduction from a full close: it waits for its sized copy,
      which shares the fingerprint;
    * a readable queued fill dated before it (:data:`HOLD_BEHIND_QUEUED_FILL`):
      being queued itself does not let a fill pass an earlier queued one, so a
      read returning only the later fill cannot book it first.

    :param extras: The position row's current ``extras``.
    :param activity: A reduction or close activity of the row's deal.
    :return: The reason, ``None`` when the fill is deliverable now.
    """
    if entry_pending_since(extras) is not None:
        return HOLD_ENTRY_PRICE_PENDING
    if is_delivered(extras, activity):
        return None
    if activity_size(activity) is None and _close_identity(activity) in queued_fills(extras):
        return HOLD_SIZE_UNKNOWN
    date_utc = str(activity.get('dateUTC') or '')
    if any(queued < date_utc for queued in readable_queued_dates(extras)):
        return HOLD_BEHIND_QUEUED_FILL
    return None


def enqueue_fill(extras: dict, activity: dict) -> bool:
    """Queue the held reduction or close ``activity`` on a row's ``extras``.

    The fill joins :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_DEFERRED_CLOSES`
    under its identity and its sizeless copy's, with its ``dateUTC``. A fill
    already booked or already queued writes nothing.

    :param extras: The position row's ``extras``, mutated in place.
    :param activity: The held fill's activity.
    :return: Whether ``extras`` changed.
    """
    identities = _fill_identities(activity)
    if identities[0] in (extras.get(EXTRAS_KEY_BOOKED_CLOSES) or ()):
        return False
    return enqueue_identities(extras, identities, str(activity.get('dateUTC') or ''))


def enqueue_identities(extras: dict, identities: tuple[str, ...], date_utc: str) -> bool:
    """Queue one fill, known by ``identities``, dated ``date_utc``, on a row's ``extras``.

    The closes a held partly netted entry deferred join the queue this way
    once the entry is booked.

    :param extras: The position row's ``extras``, mutated in place.
    :param identities: The fill's identity and its sizeless copy's.
    :param date_utc: The fill activity's ``dateUTC``.
    :return: Whether ``extras`` changed.
    """
    queued = dict(extras.get(EXTRAS_KEY_DEFERRED_CLOSES) or {})
    if all(key in queued for key in identities):
        return False
    for key in identities:
        queued.setdefault(key, date_utc)
    extras[EXTRAS_KEY_DEFERRED_CLOSES] = queued
    return True


def mark_delivered(extras: dict, identity: str) -> None:
    """Record ``identity`` delivered without booking a fill under it.

    A sizeless copy superseded by its sized copy is the same fill: only the
    sized copy is booked, and a later sizeless copy is a replay.

    :param extras: The position row's ``extras``, mutated in place.
    :param identity: The superseded copy's identity.
    """
    booked = list(extras.get(EXTRAS_KEY_BOOKED_CLOSES) or ())
    if identity not in booked:
        extras[EXTRAS_KEY_BOOKED_CLOSES] = booked[-BOOKED_CLOSES_RETAINED + 1:] + [identity]


def enqueue_entry(extras: dict, date_utc: str) -> bool:
    """Queue a row's entry, which no source prices yet, at the head of its deal's queue.

    :param extras: The position row's ``extras``, mutated in place.
    :param date_utc: The entry activity's ``dateUTC``.
    :return: Whether ``extras`` changed.
    """
    if extras.get(EXTRAS_KEY_ENTRY_PRICE_PENDING) is not None:
        return False
    extras[EXTRAS_KEY_ENTRY_PRICE_PENDING] = date_utc
    return True


def book_fill(extras: dict, activity: dict, close_id: str) -> None:
    """Deliver the reduction or close ``close_id``: record it booked and dequeue it.

    The fill joins :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_BOOKED_CLOSES`
    and leaves :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_DEFERRED_CLOSES`,
    together with its sizeless copy's identity: either copy books the fill,
    and a sizeless copy read after the sized one booked it is dropped.

    :param extras: The position row's ``extras``, mutated in place.
    :param activity: The fill's activity.
    :param close_id: The fill's identity
        (:func:`~pynecore_capitalcom.models._close_identity`).
    """
    identities = list(dict.fromkeys((close_id, sizeless_close_identity(activity))))
    booked = [key for key in (extras.get(EXTRAS_KEY_BOOKED_CLOSES) or ()) if key not in identities]
    extras[EXTRAS_KEY_BOOKED_CLOSES] = (booked + identities)[-BOOKED_CLOSES_RETAINED:]
    queued = extras.get(EXTRAS_KEY_DEFERRED_CLOSES)
    if queued:
        queued = {key: value for key, value in queued.items() if key not in identities}
        if queued:
            extras[EXTRAS_KEY_DEFERRED_CLOSES] = queued
        else:
            extras.pop(EXTRAS_KEY_DEFERRED_CLOSES, None)


def book_entry(extras: dict, booked_at: float, executed_date_utc: str | None = None) -> None:
    """Deliver a row's entry: stamp it booked and drop it from the head of the queue.

    The venue time of the execution survives the booking in
    :data:`~pynecore_capitalcom.helpers.EXTRAS_KEY_ENTRY_EXECUTED_UTC`: the
    queued entry's ``dateUTC``, or ``executed_date_utc`` when the booking
    activity states it.

    :param extras: The position row's ``extras``, mutated in place.
    :param booked_at: Epoch seconds of the booking (``entry_filled_at``).
    :param executed_date_utc: The ``dateUTC`` of the execution activity being
        booked, ``None`` when the booking reads none.
    """
    executed = executed_date_utc or extras.get(EXTRAS_KEY_ENTRY_PRICE_PENDING)
    if executed:
        extras[EXTRAS_KEY_ENTRY_EXECUTED_UTC] = str(executed)
    extras['entry_filled_at'] = booked_at
    extras.pop(EXTRAS_KEY_ENTRY_PRICE_PENDING, None)


def settle_undelivered(extras: dict, settled_at: float) -> dict | None:
    """Drop a torn-down row's queue unbooked.

    Called only once the row's exposure left the journal: a later copy of a
    queued fill is then a suppressed replay, so it is no longer owed. An entry
    still queued for its price is stamped accounted, so a priced copy of its
    activity read later is a suppressed replay instead of exposure on a closed
    deal.

    :param extras: The position row's ``extras``, mutated in place.
    :param settled_at: Epoch seconds of the teardown.
    :return: The audit payload of what was settled, ``None`` when nothing was queued.
    """
    pending_since = entry_pending_since(extras)
    unbooked = sorted(queued_fills(extras))
    if pending_since is None and not unbooked:
        return None
    if pending_since is not None:
        book_entry(extras, settled_at)
    extras.pop(EXTRAS_KEY_DEFERRED_CLOSES, None)
    return {'deferred_entry_date_utc': pending_since, 'owed_fills': unbooked}


def queue_dates(extras: dict) -> list[str]:
    """Every ``dateUTC`` the row's queue waits for: the queued entry and the queued fills."""
    dates = list(queued_fills(extras).values())
    pending = entry_pending_since(extras)
    if pending is not None:
        dates.append(pending)
    return dates
