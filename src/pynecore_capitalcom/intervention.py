"""External-intervention detection: moved or removed SL/TP, manual closes.

Only the plugin can tell who changed a bot-owned position on Capital.com, so
it reports two kinds of evidence through
:meth:`~pynecore.core.plugin.broker.BrokerPlugin.report_external_intervention`:

* **Protection levels.** A Capital.com bracket is a set of attributes on the
  position (``stopLevel`` / ``profitLevel`` / ``trailingStop`` +
  ``stopDistance``), and every own ``PUT /positions/{dealId}`` replaces all
  of them. Each such PUT is recorded on a core
  :class:`~pynecore.core.broker.protection_guard.ProtectionLevelGuard` before
  it leaves (:meth:`_InterventionMixin._protection_write`), and the
  ``/positions`` poll judges every guarded position against the levels the
  bot put there (:meth:`_InterventionMixin._observe_protection`). The guard
  key is the position ``dealId``; the accepted levels persist on the parent
  entry row, which lives exactly as long as the position does — bracket leg
  rows can be retired while the position stays open, and hedging-mode rows
  have none.
  A bot position the bot never put protection on is guarded too: its
  baseline is "no stop, no take-profit", so a level someone else adds is
  judged like a moved one. A position whose levels the guard holds no record
  of (a released fail-safe parent, a row from an earlier plugin version)
  takes the venue's protection as its baseline instead; levels no own record
  explains are logged ERROR and audited, never reported.
* **Working orders.** The bot's resting entry order is its own state as
  well: the guard key ``working:{dealId}`` carries the level the bot placed
  (or last amended) the order at, every own ``PUT /workingorders/{dealId}``
  is recorded before it leaves (:meth:`_InterventionMixin._working_order_write`),
  and the ``/workingorders`` poll judges the venue's ``orderLevel``. The bot
  never puts a stop, a take-profit, an expiry or a guaranteed / trailing flag
  on its resting order, so one showing on it is someone else's edit too
  (:meth:`_InterventionMixin._judge_working_order_attributes`).
  As on a position, an edit the venue itself (``SYSTEM``) or its dealing
  desk (``DEALER``) made is accepted, never reported; the startup activity
  read brings back the edits made while no process ran.
* **Who made a change.** Who made a confirmed change is read from the edit
  activities on the deal (:meth:`_InterventionMixin._judge_level_edits`).
  The bot's own writes are stamped ``USER`` like anyone else's; each own
  write explains one such edit. A ``USER`` edit after the bot's last own
  write that no own write explains is someone else's and decides at once.
  Otherwise the venue publishes the edit up to a minute late, so the verdict
  waits for an activity read that started :data:`_LEVEL_EDIT_SETTLE_S` after
  the change was confirmed (:meth:`_InterventionMixin._resolve_held_edit_verdicts`).
  A change no venue edit explains is someone else's: the bot's levels moved
  and neither the bot nor the venue moved them. The edits of a deal stay
  remembered while a verdict on it waits, so an activity-read outage never
  loses the venue edit that explains it.
  Meanwhile the bot does not write that position's protection
  (:meth:`_InterventionMixin._defer_protection_write`), so someone else's
  levels are never replaced before the quarantine latches. While an own
  write the bot cannot defer is in flight, no verdict on its key is decided:
  a read may hold the write's own edit before the write is recorded. The
  verdict is persisted and a restarted process decides it; a change it
  confirmed before an operator resume is one the resume accepted.
* **Closes.** The venue stamps the bot's own ``DELETE /positions/{dealId}``
  and opposite-direction reduce ``POST`` with ``source=USER``, exactly like a
  close from the web platform. Every own close therefore leaves a persisted
  "expected own close" record on the target entry row before the request
  leaves (:meth:`_InterventionMixin._own_close`). A ``USER`` close activity
  that no record and no own netting order explains is someone else's: it is
  booked as before, then reported. A ``DEALER`` close is booked with a loud
  warning and never reported; TP / SL / stop-out / system closes never reach
  this path. Each close is judged once (keyed on its level-independent
  identity), so a re-issued activity of the same close is neither judged nor
  reported again.
* **Cancels.** The bot's own ``DELETE /workingorders/{dealId}`` is stamped
  ``source=USER`` too. Each one is recorded on the order's row before it
  leaves (:meth:`_InterventionMixin._own_cancel`), so a restarted process
  tells the bot's own cancel from someone else's
  (:meth:`_InterventionMixin._own_cancel_explains`). While the bot runs the
  engine knows its own cancels; a cancel the venue itself or its dealing
  desk made, an expiry and a rejection are recorded on the row, so the
  disappearance verdict never takes them for someone else's
  (:meth:`_InterventionMixin._note_working_order_end`).

Parents the core native fail-safe manager owns are left out of the guard:
the manager writes their stop itself and detects edits on its own. It owns a
parent while the parent has a live software partial-bracket leg, and, in the
process that sent it, from its own stop write until the bot writes the
position's protection itself — the engine releases a parent from the manager
before it does. A parent that loses its partial legs without the manager ever
having written its stop, and one whose stop the manager wrote in an earlier
process (a restarted engine registers only parents that still have partial
legs), takes the venue's protection as its guarded baseline again.
"""
import logging
import math
from abc import ABC
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from time import monotonic, time as epoch_time
from typing import TYPE_CHECKING, Any

import httpx

from pynecore.core.broker.exceptions import (
    BrokerError,
    OrderSkippedByPlugin,
    UnexpectedCancelError,
)
from pynecore.core.broker.protection_guard import (
    PROTECTION_LEVELS_EXTRA,
    ProtectionLevelGuard,
    ProtectionLevels,
)
from pynecore.core.broker.store_helpers import (
    ENGINE_TRIGGER_LEG_KINDS,
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    EXTRAS_KEY_LEG_KIND,
    EXTRAS_KEY_PARENT_ENTRY_DISPATCH_REF,
    KIND_CANCEL,
    KIND_MODIFY_ENTRY,
    KIND_MODIFY_EXIT,
    KIND_PARTIAL_CLOSE,
    STATE_REJECTED,
)

from ._base import _CapitalComBase
from .exceptions import CapitalComError
from .helpers import (
    ACTOR_CLIENT,
    ACTOR_DEALER,
    CLOSE_VERDICT_AMBIGUOUS,
    CLOSE_VERDICT_DEALER,
    CLOSE_VERDICT_FOREIGN,
    CLOSE_VERDICT_NETTING_PENDING,
    CLOSE_VERDICT_OWN,
    CLOSE_VERDICT_OWN_NETTING,
    CLOSE_VERDICT_UNJUDGED,
    CLOSE_VERDICT_VENUE,
    EXTRAS_KEY_BOOKED_CLOSES,
    EXTRAS_KEY_CANCEL_REQUESTED,
    EXTRAS_KEY_CLOSE_VERDICTS,
    EXTRAS_KEY_FAILSAFE_OWNED,
    EXTRAS_KEY_HELD_EDIT_VERDICTS,
    EXTRAS_KEY_LEVEL_EDITS_CONSUMED,
    EXTRAS_KEY_ORDER_LEVEL,
    EXTRAS_KEY_OWN_CANCEL_REQUESTS,
    EXTRAS_KEY_OWN_CANCEL_SENT_AT,
    EXTRAS_KEY_OWN_CLOSES,
    EXTRAS_KEY_OWN_LEVEL_WRITES,
    EXTRAS_KEY_PROTECTION_ADOPT_AFTER,
    EXTRAS_KEY_PROTECTION_HANDOVER,
    EXTRAS_KEY_WORKING_ORDER_END,
    EXTRAS_KEY_WORKING_ACCEPTED_ATTRIBUTES,
    OWN_CLOSE_CURSOR_RETIRED,
    _ACTIVITY_LAG_S,
    _activity_actor,
    _level_on_tick_grid,
    _parse_iso_timestamp,
    _wire_float,
    _wire_id,
)
from .models import (
    _VENUE_REFUSALS,
    _HeldEditVerdict,
    _OwnCloseHandle,
    _ProtectionWrite,
    _close_identity,
)

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

logger = logging.getLogger(__name__)

_PROTECTION_KEY_PREFIX = 'position:'
_WORKING_KEY_PREFIX = 'working:'
#: Held-verdict identity of a change of a resting order's attributes.
_ATTRIBUTES_HELD_PREFIX = 'working-attributes:'
#: Held-verdict identity of levels a position shows after its size changed.
_SIZE_CHANGE_HELD_PREFIX = 'size-change:'

#: Tolerated difference between the venue's activity clock (``dateUTC``) and
#: the local wall clock the own-close records are stamped with. Generous on
#: purpose: an own close missed because of a drifting local clock would be a
#: false quarantine, while the window only has to keep an unmatched record
#: from explaining an unrelated close much later. ``dateUTC`` is the deal
#: time, so the venue's publication lag does not count against it. Bounds the
#: late end of every record; the early end uses the measured clock floor
#: (:meth:`_InterventionMixin._own_close_earliest`) when a sample shares the
#: request's wall-clock frame.
_CLOCK_SKEW_S = 300.0

#: How long an activity read's clock sample keeps bounding the venue clock.
_VENUE_CLOCK_SAMPLE_WINDOW_S = 600.0

#: Slack under the measured venue-clock floor: an activity dated earlier than
#: an own request's send time by more than the floor plus this was not caused
#: by that request.
_VENUE_CLOCK_MARGIN_S = 5.0

#: Largest disagreement between the wall clock and the monotonic clock over
#: the time from a clock sample to an own request that still counts as one
#: wall-clock frame. Anything larger is a wall-clock step or a sleep, after
#: which the sample no longer bounds the venue clock for that request. Below
#: :data:`_VENUE_CLOCK_MARGIN_S`, so a smaller step stays inside the margin.
_CLOCK_FRAME_TOLERANCE_S = 1.0

#: Close verdicts kept per position row.
_CLOSE_VERDICTS_RETAINED = 64

#: How long after a close a ``/workingorders`` read must start (venue time)
#: before an own working order it still shows at full size proves the order
#: did not execute at the close. The venue's REST views are eventually
#: consistent (a fresh position was measured missing from ``/positions`` for
#: over seven seconds); generous on purpose, a later verdict only delays a
#: report.
_NETTING_PROOF_SETTLE_S = 20.0

#: How long a close only a resting own working order could explain waits for
#: such a read before it is left ambiguous.
_NETTING_PENDING_MAX_S = 300.0

#: Upper bound on how long an own close request whose outcome never came
#: back can still land on the venue.
_UNSETTLED_WRITE_S = 120.0

#: Own-close records older than this are dropped (the activity feed's
#: longest lookback).
_OWN_CLOSE_RETAIN_S = 86400.0

#: How long a level-edit activity is remembered for the guard's judgement,
#: counted from when this process first saw it (a restart's activity read
#: brings back the edits made while no process ran).
_LEVEL_EDIT_RETAIN_S = 600.0

#: How long after a change of the bot's levels was confirmed an activity read
#: must start before it is trusted to hold the edit that made the change. The
#: venue publishes an activity up to a minute after the deal it stamps
#: (measured on the live journal: an ``EDIT_STOP_AND_LIMIT`` activity first
#: read 57.8 s after its ``dateUTC``); a venue edit judged before its activity
#: is read would be reported as someone else's, and a venue edit already read
#: would explain a later edit by someone else.
_LEVEL_EDIT_SETTLE_S = float(_ACTIVITY_LAG_S)

#: Consecutive snapshot reads that must show the same other levels on a key
#: whose levels verdict is held before the verdict is about those levels
#: (:meth:`_InterventionMixin._note_held_levels_view`); as many as the guard
#: needs to confirm a change.
_HELD_VIEW_CONFIRMATIONS = 2

#: How long a venue verdict on a changed level waits for a snapshot of the key
#: read after the newest explaining edit was first seen
#: (:meth:`_InterventionMixin._held_view_outdated`) before it is decided on the
#: levels it holds.
_HELD_VIEW_WAIT_S = 120.0

#: Own writes kept per protection key on its row (:meth:`_InterventionMixin._own_write_claims`).
_OWN_LEVEL_WRITES_RETAINED = 16

#: Verdicts on a confirmed change of the bot's levels
#: (:meth:`_InterventionMixin._judge_level_edits`).
_EDIT_FOREIGN = 'foreign'
_EDIT_VENUE = 'venue'
_EDIT_UNEXPLAINED = 'unexplained'

#: Row states of a bracket leg whose venue outcome is not settled yet.
_UNSETTLED_LEG_STATES = frozenset({'submitted', 'disposition_unknown', 'cancel_pending'})

#: The activation PUT is in flight or not yet visible: the trailing monitor
#: owns the stop. A ``pending`` trail is judged like any protection — the
#: venue still carries the bot's pre-activation levels.
_TRAIL_ACTIVATING_STATES = frozenset({'activating'})


def _protection_key(deal_id: str) -> str:
    return _PROTECTION_KEY_PREFIX + deal_id


def _working_key(deal_id: str) -> str:
    return _WORKING_KEY_PREFIX + deal_id


#: ``workingOrderData`` fields of a resting entry order the bot never sets:
#: ``execute_entry`` places ``epic`` / ``direction`` / ``size`` / ``level`` /
#: ``type`` only, and an own amend moves ``level`` only.
_FOREIGN_WORKING_ORDER_FIELDS = (
    'stopLevel', 'stopDistance', 'stopAmount',
    'profitLevel', 'profitDistance', 'profitAmount',
    'goodTillDate', 'guaranteedStop', 'trailingStop',
)


def _foreign_working_order_attributes(data: dict) -> list[list[str]]:
    """The attributes the bot never sets that a ``workingOrderData`` row carries.

    Absent, ``null``, ``false``, zero and empty values are unset; a
    ``GOOD_TILL_DATE`` time in force is an expiry.

    :return: ``[field, value]`` pairs sorted by field, values normalized to
        strings so two reads of the same attributes compare equal.
    """
    found: list[list[str]] = []
    for name in _FOREIGN_WORKING_ORDER_FIELDS:
        value = data.get(name)
        if value is None or value is False or value == '' or value == 0:
            continue
        if isinstance(value, bool):
            found.append([name, 'true'])
        elif isinstance(value, (int, float)):
            found.append([name, repr(float(value))])
        else:
            found.append([name, str(value)])
    if str(data.get('timeInForce') or '').upper() == 'GOOD_TILL_DATE':
        found.append(['timeInForce', 'GOOD_TILL_DATE'])
    return sorted(found)


def _attribute_signature(attributes: object) -> tuple:
    """Hashable form of the ``[field, value]`` pairs :func:`_foreign_working_order_attributes` returns."""
    if not isinstance(attributes, list):
        return ()
    return tuple(tuple(pair) for pair in attributes if isinstance(pair, list))


def _level_tolerance(tick: float, venue: ProtectionLevels) -> float:
    """How far apart two levels may be and still match: half a price tick, plus float noise."""
    tolerance = 0.5 * tick if tick > 0.0 else 0.0
    return tolerance + 1e-12 * max(1.0, abs(venue.stop or 0.0), abs(venue.limit or 0.0))


def _finite_or_none(value: float) -> float | None:
    """``value`` for the journal, ``None`` when it is not finite."""
    return value if math.isfinite(value) else None


def _working_levels(order_type: str | None, level: float | None) -> ProtectionLevels:
    """A resting entry's level as guard levels: a stop order's in ``stop``, a limit's in ``limit``."""
    if (order_type or '').lower() == 'stop':
        return ProtectionLevels(stop=level)
    return ProtectionLevels(limit=level)


class _InterventionMixin(_CapitalComBase, ABC):
    """Protection-level guard wiring and manual-close attribution."""

    # --- Protection levels -------------------------------------------------

    def _protection_guard(self) -> ProtectionLevelGuard:
        """The lazily-built guard; ``store_ctx`` is injected after ``__init__``."""
        guard = self._protection
        if guard is None:
            guard = ProtectionLevelGuard(
                self._report_protection_edit, store_ctx=self.store_ctx,
            )
            self._protection = guard
        return guard

    @staticmethod
    def _guard_key_deal(key: str) -> str | None:
        """The ``dealId`` a position or working-order protection key names."""
        for prefix in (_PROTECTION_KEY_PREFIX, _WORKING_KEY_PREFIX):
            if key.startswith(prefix):
                return key[len(prefix):]
        return None

    def _report_protection_edit(self, reason: str, context: dict) -> None:
        """Guard ``report`` hook: route a confirmed edit into the policy.

        Who changed the levels of a bot position or a resting bot order is
        read from the edit activities on the deal (:meth:`_judge_level_edits`):
        someone else's edit is reported; levels the venue itself (``SYSTEM``)
        or its dealing desk (``DEALER``) set become the accepted baseline
        (:meth:`_adopt_venue_level_edit`). The verdict is held until the
        activity feed covers the change (:meth:`_hold_edit_verdict`).
        """
        key = str(context.get('protection_key') or '')
        deal_id = self._guard_key_deal(key)
        if deal_id is None:
            self._report_level_edit(reason, context, None)
            return
        read_started_at = self._judged_read_started_at
        coid = context.get('client_order_id')
        verdict = self._held_verdict({
            'kind': 'levels', 'deal_id': deal_id, 'guard_key': key,
            'observed_at': read_started_at if read_started_at is not None else epoch_time(),
            'edits_after': _finite_or_none(self._edits_after(deal_id, key)),
            'row_coid': coid if isinstance(coid, str) else None,
            'reason': reason, 'context': context,
        })
        if verdict is None:
            self._report_level_edit(reason, context, deal_id)
            return
        self._hold_edit_verdict(key, verdict, keep_held=True)

    def _report_level_edit(
            self, reason: str, context: dict, deal_id: str | None,
            *, changed_at_ms: int | None = None,
    ) -> None:
        """Report a confirmed change of the bot's levels as someone else's.

        :param changed_at_ms: When the change was made at the latest (Unix
            ms): an operator resume after it accepted it
            (:meth:`~pynecore.core.plugin.broker.BrokerPlugin.report_external_intervention`).
        """
        key = str(context.get('protection_key') or '')
        if key.startswith(_WORKING_KEY_PREFIX):
            expected = context.get('expected') or [None, None]
            observed = context.get('observed') or [None, None]
            reason = (
                f"bot working order {deal_id} changed on the venue by someone "
                f"else: expected level {expected[0] if expected[0] is not None else expected[1]}, "
                f"found {observed[0] if observed[0] is not None else observed[1]}"
            )
        logger.error("%s", reason)
        coid = context.get('client_order_id')
        self.report_external_intervention(
            reason, context,
            client_order_id=coid if isinstance(coid, str) else None,
            exchange_order_id=deal_id,
            changed_at_ms=changed_at_ms,
        )

    def _held_verdict(self, record: dict) -> _HeldEditVerdict | None:
        """Build the held verdict a record describes, freshly confirmed or persisted.

        :param record: ``kind`` (``'levels'``, ``'attributes'`` or
            ``'size_change'``), ``deal_id``, ``guard_key``, ``observed_at``,
            ``edits_after`` (``None``: every edit counts), ``row_coid``,
            ``provisional`` and the kind's own fields.
        :return: ``None`` for a record that is not readable.
        """
        kind = record.get('kind')
        deal_id = record.get('deal_id')
        key = record.get('guard_key')
        observed_at = _wire_float(record.get('observed_at'), finite=True)
        if (not isinstance(deal_id, str) or not isinstance(key, str)
                or observed_at is None):
            return None
        after = _wire_float(record.get('edits_after'), finite=True)
        edits_after = after if after is not None else float('-inf')
        raw_coid = record.get('row_coid')
        row_coid = raw_coid if isinstance(raw_coid, str) else None
        provisional = bool(record.get('provisional'))
        # The change was made before it was confirmed: a verdict an earlier
        # process held over an operator resume is about a change the resume
        # accepted.
        changed_at_ms = int(observed_at * 1000.0)

        def held(
                venue: Callable[[tuple[float, str], bool], None],
                foreign: Callable[[bool], None],
                *,
                unexplained: Callable[[bool], None] | None = None,
                own_write: Callable[[], None] | None = None,
        ) -> _HeldEditVerdict:
            return _HeldEditVerdict(
                deal_id=deal_id, guard_key=key, observed_at=observed_at,
                edits_after=edits_after, venue=venue, foreign=foreign,
                unexplained=unexplained, own_write=own_write,
                provisional=provisional, row_coid=row_coid, record=record,
            )

        if kind == 'levels':
            context = record.get('context')
            reason = record.get('reason')
            if not isinstance(context, dict) or not isinstance(reason, str):
                return None
            return held(
                lambda edit, own_write: self._adopt_venue_level_edit(
                    deal_id, key, context, edit, own_write=own_write),
                lambda _own_write: self._report_level_edit(
                    reason, context, deal_id, changed_at_ms=changed_at_ms),
                own_write=lambda: self._accept_observed_levels(key, context),
            )
        if kind == 'attributes':
            attributes = record.get('attributes')
            expected = record.get('expected')
            if (row_coid is None or not isinstance(attributes, list)
                    or not isinstance(expected, list)):
                return None
            coid = row_coid
            symbol = str(record.get('symbol') or '')
            accepted = {'attributes': attributes, 'resume_ts_ms': record.get('resume_ts_ms')}
            signature = _attribute_signature(attributes)

            def adopt_attributes(edit: tuple[float, str], _own_write: bool) -> None:
                self._working_attributes_reported.discard((deal_id, signature))
                self._adopt_venue_working_attributes(coid, deal_id, accepted, expected, edit)

            return held(
                adopt_attributes,
                lambda _own_write: self._report_working_attributes(
                    coid, deal_id, symbol, attributes, expected,
                    changed_at_ms=changed_at_ms),
            )
        if kind == 'size_change':
            raw_expected = record.get('expected')
            try:
                levels = ProtectionLevels.from_list(list(record.get('levels') or []))
            except (TypeError, ValueError):
                return None
            if row_coid is None or not isinstance(raw_expected, list):
                return None
            coid = row_coid
            expected_levels = raw_expected
            size = _wire_float(record.get('size'), finite=True)

            def adopt_levels(edit: tuple[float, str] | None, own_write: bool) -> None:
                self._adopt_levels_after_size_change(
                    coid, deal_id, key, levels, size, edit, own_write=own_write)

            return held(
                adopt_levels,
                lambda own_write: self._report_size_change_edit(
                    coid, deal_id, key, levels, expected_levels,
                    changed_at_ms=changed_at_ms, own_write=own_write),
                unexplained=lambda own_write: adopt_levels(None, own_write),
            )
        return None

    def _hold_edit_verdict(
            self, held_id: str, verdict: _HeldEditVerdict, *, keep_held: bool = False,
    ) -> None:
        """Wait with the verdict on a confirmed change until the activity feed covers it.

        Someone else's edit, an edit activity after the bot's last own write
        that no own write explains (:meth:`_judge_level_edits`), decides the
        verdict as soon as it is read. Otherwise the change is decided once an
        activity read started :data:`_LEVEL_EDIT_SETTLE_S` after it was
        confirmed (:meth:`_resolve_held_edit_verdicts`): the edit that made it
        can surface that late. Until then the bot does not write the
        position's protection itself (:meth:`_defer_protection_write`); a
        write that goes out anyway is prepared by
        :meth:`_keep_held_verdicts_past_own_write`. The verdict is persisted
        on its journal row, so a restarted process decides it too
        (:meth:`_reload_held_edit_verdicts`).

        :param held_id: The change's identity.
        :param verdict: The held verdict.
        :param keep_held: Keep a verdict already held under ``held_id`` (the
            same change confirmed again, e.g. after a restart reloaded it)
            instead of replacing it with this newer one.
        """
        existing = self._held_edit_verdicts.get(held_id)
        if existing is not None and keep_held:
            self._resolve_held_edit_verdicts()
            return
        if existing is not None:
            self._drop_held_verdict_record(held_id, existing)
        self._held_edit_verdicts[held_id] = verdict
        self._persist_held_verdict(held_id, verdict)
        logger.info(
            "bot %s changed on the venue; who changed it is decided from the "
            "activity feed", held_id,
        )
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'level_edit_verdict_held',
                exchange_order_id=verdict.deal_id,
                payload={'held_id': held_id, 'observed_at': verdict.observed_at,
                         'settle_s': self._level_edit_settle_s},
            )
        self._resolve_held_edit_verdicts()

    def _resolve_held_edit_verdicts(self) -> None:
        """Decide every held verdict the activity read so far settles.

        Someone else's edit settles a verdict at once. Any other verdict waits
        for an activity read that started :attr:`_level_edit_settle_s` after
        the change was confirmed; ``_activity_polled_at`` is when the latest
        successful activity read was issued.

        A verdict whose key the bot is writing right now waits until the
        write settles (:meth:`_own_write_in_flight`): a read may already hold
        the write's own edit activity, which counts as the bot's own only
        once the write is recorded (:meth:`_own_write_claims`).
        """
        if not self._held_edit_verdicts:
            return
        read_at = self._activity_polled_at
        for held_id, verdict in list(self._held_edit_verdicts.items()):
            if (self._held_edit_verdicts.get(held_id) is not verdict
                    or self._own_writes_in_flight.get(verdict.guard_key, 0) > 0):
                continue
            outcome, edit = self._judge_level_edits(verdict)
            settled = read_at >= verdict.observed_at + self._level_edit_settle_s
            if outcome != _EDIT_FOREIGN and not settled:
                continue
            if outcome == _EDIT_VENUE and self._held_view_outdated(verdict, read_at):
                continue
            del self._held_edit_verdicts[held_id]
            self._drop_held_verdict_record(held_id, verdict)
            if held_id == verdict.guard_key:
                self._held_level_views.pop(held_id, None)
                self._held_level_changes.pop(held_id, None)
            self._decide_edit_verdict(held_id, verdict, outcome, edit, settled=settled)

    def _note_held_levels_view(
            self, key: str, venue: ProtectionLevels, *, tick: float,
            read_started_at: float | None,
    ) -> None:
        """Keep watching a key whose levels verdict is held, as the guard no longer does.

        The guard stops observing a key once it reported a change, but the
        venue or its dealing desk may edit the levels again before the
        verdict is decided. Accepting the venue's edits would then make the
        levels of the first change the baseline while the venue shows the
        second, and the next poll would confirm a change no edit explains.
        So a snapshot read after the change was confirmed is noted here
        (:meth:`_held_view_outdated` waits for one), and other levels shown
        by :data:`_HELD_VIEW_CONFIRMATIONS` reads in a row become the levels
        the verdict is about: it is held afresh from that read, with the same
        edits counting, so the feed covers the newer edit too.

        :param key: The protection key the snapshot shows.
        :param venue: The levels it shows.
        :param tick: The instrument's price tick.
        :param read_started_at: Wall-clock start of the snapshot read;
            ``None`` (a push) is not noted.
        """
        verdict = self._held_edit_verdicts.get(key)
        if (verdict is None or verdict.provisional or read_started_at is None
                or read_started_at <= verdict.observed_at
                or verdict.record.get('kind') != 'levels'):
            return
        context = verdict.record.get('context')
        if not isinstance(context, dict):
            return
        try:
            observed = ProtectionLevels.from_list(list(context.get('observed') or []))
        except (TypeError, ValueError):
            return
        self._held_level_views[key] = read_started_at
        tolerance = _level_tolerance(tick, venue)
        if observed.matches(venue, tolerance):
            self._held_level_changes.pop(key, None)
            return
        pending = self._held_level_changes.get(key)
        repeated = pending is not None and pending[0].matches(venue, tolerance)
        count = pending[1] + 1 if pending is not None and repeated else 1
        if count < _HELD_VIEW_CONFIRMATIONS:
            self._held_level_changes[key] = (venue, count)
            return
        self._held_level_changes.pop(key, None)
        reason = str(verdict.record.get('reason') or '')
        renewed = self._held_verdict({
            **verdict.record,
            'observed_at': read_started_at,
            'reason': f"{reason}; then changed again to {venue.as_list()}",
            'context': {**context, 'observed': venue.as_list()},
        })
        if renewed is None:
            return
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'level_edit_verdict_levels_changed',
                exchange_order_id=verdict.deal_id,
                payload={'held_id': key, 'previous': context.get('observed'),
                         'observed': venue.as_list(), 'observed_at': read_started_at},
            )
        self._hold_edit_verdict(key, renewed)

    def _held_view_outdated(self, verdict: _HeldEditVerdict, read_at: float) -> bool:
        """Whether a venue verdict on a changed level waits for a fresher snapshot of the key.

        Accepting the venue's edit makes the levels the verdict holds the
        guard's baseline and consumes the newest edit
        (:meth:`_adopt_venue_level_edit`). Those levels must come from a read
        that started after that edit was first seen, with no other levels
        pending confirmation (:meth:`_note_held_levels_view`): an edit read
        before any snapshot shows it would be consumed beside levels it did
        not set. Waits at most :data:`_HELD_VIEW_WAIT_S` after the edit was
        seen, and never for a key the guard no longer guards.

        :param verdict: A held verdict the feed settled as the venue's.
        :param read_at: When the latest activity read was issued.
        """
        key = verdict.guard_key
        if (verdict.provisional or verdict.record.get('kind') != 'levels'
                or not self._protection_guard().known(key)):
            return False
        edits = self._level_edits.get(verdict.deal_id) or {}
        newest_seen = max((edit[2] for edit in edits.values()
                           if edit[0] >= verdict.edits_after), default=None)
        if newest_seen is None or read_at >= newest_seen + _HELD_VIEW_WAIT_S:
            return False
        if key in self._held_level_changes:
            return True
        latest_view = max(verdict.observed_at, self._held_level_views.get(key, float('-inf')))
        return latest_view <= newest_seen

    def _decide_edit_verdict(
            self, held_id: str, verdict: _HeldEditVerdict, outcome: str,
            edit: tuple[float, str] | None, *, settled: bool,
    ) -> None:
        """Apply one verdict :meth:`_judge_level_edits` reached, audited."""
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'level_edit_verdict_decided',
                exchange_order_id=verdict.deal_id,
                payload={'held_id': held_id, 'verdict': outcome,
                         'source': edit[1] if edit is not None else None,
                         'activity_ts': edit[0] if edit is not None else None,
                         'feed_settled': settled, 'provisional': verdict.provisional},
            )
        if outcome == _EDIT_VENUE and edit is not None:
            verdict.venue(edit, verdict.provisional)
        elif outcome == _EDIT_UNEXPLAINED and verdict.unexplained is not None:
            verdict.unexplained(verdict.provisional)
        else:
            verdict.foreign(verdict.provisional)

    def _judge_level_edits(
            self, verdict: _HeldEditVerdict,
    ) -> tuple[str, tuple[float, str] | None]:
        """Who made the change a held verdict is about, by the edit activities read so far.

        Only edits dated at or after :attr:`~._HeldEditVerdict.edits_after`
        count: an older one was replaced by the bot's own later write (a
        protection write replaces every level), or was already taken as the
        venue's. The bot's own writes are stamped ``USER`` like anyone
        else's; each own write the venue did not refuse explains one such
        edit (:meth:`_own_write_claims`).

        Who made an edit is read from its source (:func:`_activity_actor`),
        the same reading the close and cancel attribution use.

        * ``foreign``: a client (``USER``) edit no own write explains —
          someone else edited the bot's levels, whatever else happened since.
        * ``venue``: the newest edit is the venue's own or its dealing
          desk's; returned with it as ``(venue time, source)``. Own writes
          sent after the change was confirmed explain nothing about it, so
          their edits never count as the newest.
        * ``unexplained``: no edit, or only the bot's own, explains the change.
        """
        edits = self._level_edits.get(verdict.deal_id) or {}
        claims = self._own_write_claims(verdict.row_coid, verdict.guard_key, edits)
        candidates = [(edit[0], edit[1], identity) for identity, edit in edits.items()
                      if edit[0] >= verdict.edits_after]
        if any(_activity_actor(source) == ACTOR_CLIENT and identity not in claims
               for _, source, identity in candidates):
            return _EDIT_FOREIGN, None
        # An own echo the venue dated in the same millisecond as a venue edit
        # explains the bot's own levels, not the change: the venue's edit is
        # the newer one.
        newest = max(((activity_ts, source) for activity_ts, source, identity in candidates
                      if claims.get(identity, float('-inf')) < verdict.observed_at),
                     key=lambda edit: (edit[0], _activity_actor(edit[1]) != ACTOR_CLIENT),
                     default=None)
        if newest is None or _activity_actor(newest[1]) == ACTOR_CLIENT:
            return _EDIT_UNEXPLAINED, None
        return _EDIT_VENUE, newest

    def _own_write_claims(
            self, row_coid: str | None, key: str,
            edits: dict[str, tuple[float, str, float, float]],
    ) -> dict[str, float]:
        """The ``USER`` edits of a deal the bot's own writes of ``key`` explain.

        The venue stamps one edit activity per own write, dated between the
        venue times of the write's send and its answer: from
        :meth:`_venue_time_floor` of the send to :meth:`_venue_time_ceiling`
        of the answer. In send order, each own write explains the earliest
        such edit no earlier write explained. A write whose own edit is not
        read may explain someone else's edit in its window instead; the edit
        it leaves unexplained is then someone else's all the same.

        :param row_coid: The journal row the own writes are recorded on.
        :param key: The protection key.
        :param edits: The deal's recorded edits (:attr:`_level_edits`).
        :return: Edit identity -> send time of the own write that explains it.
        """
        writes = self._own_level_writes(row_coid, key)
        if not writes:
            return {}
        user_edits = sorted((edit[0], identity) for identity, edit in edits.items()
                            if _activity_actor(edit[1]) == ACTOR_CLIENT)
        claims: dict[str, float] = {}
        for sent_at, done_at in writes:
            earliest = self._venue_time_floor(sent_at)
            latest = self._venue_time_ceiling(done_at)
            for activity_ts, identity in user_edits:
                if identity in claims or activity_ts < earliest:
                    continue
                if activity_ts <= latest:
                    claims[identity] = sent_at
                break
        return claims

    def _own_level_writes(self, row_coid: str | None, key: str) -> list[tuple[float, float]]:
        """The own writes of ``key`` recorded on ``row_coid``: ``(sent_at, done_at)``, in send order."""
        store_ctx = self.store_ctx
        if store_ctx is None or row_coid is None:
            return []
        row = store_ctx.get_order(row_coid)
        writes = (row.extras or {}).get(EXTRAS_KEY_OWN_LEVEL_WRITES) if row is not None else None
        if not isinstance(writes, dict):
            return []
        records: list[tuple[float, float]] = []
        for entry in writes.get(key) or ():
            if not isinstance(entry, list) or len(entry) != 2:
                continue
            sent_at = _wire_float(entry[0], finite=True)
            done_at = _wire_float(entry[1], finite=True)
            if sent_at is not None and done_at is not None:
                records.append((sent_at, done_at))
        return sorted(records)

    def _note_own_level_write(
            self, row_coid: str, key: str, sent_at: float, done_at: float,
    ) -> None:
        """Record one own write of ``key`` the venue did not refuse (:meth:`_own_write_claims`)."""
        def add(extras: dict) -> None:
            saved = extras.get(EXTRAS_KEY_OWN_LEVEL_WRITES)
            writes = dict(saved) if isinstance(saved, dict) else {}
            kept = [entry for entry in writes.get(key) or () if isinstance(entry, list)]
            writes[key] = [*kept, [sent_at, done_at]][-_OWN_LEVEL_WRITES_RETAINED:]
            extras[EXTRAS_KEY_OWN_LEVEL_WRITES] = writes

        self._rewrite_extras(row_coid, add)

    def _note_own_protection_write(self, deal_id: str, sent_at: float, done_at: float) -> None:
        """Record an own ``PUT /positions/{deal_id}`` the guard does not track.

        The native fail-safe manager's stop write, and a write of a position
        the manager owns: the guard does not judge those positions, but the
        edit activity of the write must not be taken for someone else's by a
        verdict held on the position.
        """
        store_ctx = self.store_ctx
        if store_ctx is None or not deal_id:
            return
        row = store_ctx.find_by_ref('deal_id', deal_id)
        if row is None or row.closed_ts_ms is not None:
            return
        self._note_own_level_write(
            row.client_order_id, _protection_key(deal_id), sent_at, done_at)

    @contextmanager
    def _own_write_in_flight(self, key: str) -> Iterator[None]:
        """Mark an own write of ``key`` as in flight from before it leaves until it settles.

        The venue may publish the write's edit activity before its answer is
        back; the verdicts held on ``key`` are not judged until the write is
        recorded as the bot's own (:meth:`_resolve_held_edit_verdicts`). The
        caller records the write inside this block.

        :param key: The position or resting-order protection key written.
        """
        self._own_writes_in_flight[key] = self._own_writes_in_flight.get(key, 0) + 1
        try:
            yield
        finally:
            remaining = self._own_writes_in_flight.get(key, 1) - 1
            if remaining > 0:
                self._own_writes_in_flight[key] = remaining
            else:
                self._own_writes_in_flight.pop(key, None)

    def _keep_held_verdicts_past_own_write(self, key: str) -> None:
        """Prepare the verdicts held on ``key`` for an own write the bot cannot defer.

        A verdict the activity read so far settles is decided first (someone
        else's edit latches the quarantine, and the write is then refused).
        Any other keeps waiting under its own identity, so a later change of
        the key is judged on its own. A changed level is accepted as the
        guard's baseline before the write (the verdict reports it later if it
        was someone else's), so the bot's own write is guarded as usual; the
        verdict, persisted with that mark, survives a restart.
        """
        self._resolve_held_edit_verdicts()
        now = epoch_time()
        for held_id, verdict in list(self._held_edit_verdicts.items()):
            if verdict.guard_key != key or verdict.provisional:
                continue
            del self._held_edit_verdicts[held_id]
            self._drop_held_verdict_record(held_id, verdict)
            kept_id = f"{held_id}@{now!r}"
            kept = replace(verdict, provisional=True)
            self._held_edit_verdicts[kept_id] = kept
            self._persist_held_verdict(kept_id, kept)
            if verdict.own_write is not None:
                verdict.own_write()
            logger.info(
                "bot %s is written again while the verdict on its change waits; "
                "the change is still judged from the activity feed", held_id,
            )
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    'level_edit_verdict_kept_past_own_write',
                    exchange_order_id=verdict.deal_id,
                    payload={'held_id': held_id, 'kept_as': kept_id,
                             'observed_at': verdict.observed_at, 'own_write_at': now},
                )

    def _protection_write_deferred(self, deal_id: str) -> bool:
        """Whether a verdict on a change of ``deal_id``'s protection is still held."""
        key = _protection_key(deal_id)
        self._resolve_held_edit_verdicts()
        return any(verdict.guard_key == key for verdict in self._held_edit_verdicts.values())

    def _defer_protection_write(self, deal_id: str, *, intent_key: str, symbol: str) -> None:
        """Refuse an own write of ``deal_id``'s protection while a verdict on its change waits.

        Writing the protection then would replace levels someone else may have
        set before the quarantine latches. Nothing is journaled or sent; the
        engine re-evaluates the exit on its next sync, and it goes out once
        the change is taken as the venue's.

        :raises OrderSkippedByPlugin: ``reason='protection_verdict_pending'``.
        """
        if not deal_id or not self._protection_write_deferred(deal_id):
            return
        raise OrderSkippedByPlugin(
            f"Skipping exit {intent_key} on {symbol}: the protection of bot position "
            f"{deal_id} changed on the venue and who changed it is still being read "
            f"from the activity feed; the bot does not write it until that is decided "
            f"(re-evaluated next sync).",
            intent_key=intent_key,
            reason='protection_verdict_pending',
            context={'symbol': symbol, 'deal_id': deal_id},
        )

    def _persist_held_verdict(self, held_id: str, verdict: _HeldEditVerdict) -> None:
        """Store a held verdict on its journal row."""
        if verdict.row_coid is None:
            return
        record = {**verdict.record, 'provisional': verdict.provisional}

        def store(extras: dict) -> None:
            saved = extras.get(EXTRAS_KEY_HELD_EDIT_VERDICTS)
            held = dict(saved) if isinstance(saved, dict) else {}
            held[held_id] = record
            extras[EXTRAS_KEY_HELD_EDIT_VERDICTS] = held

        self._rewrite_extras(verdict.row_coid, store)

    def _drop_held_verdict_record(self, held_id: str, verdict: _HeldEditVerdict) -> None:
        """Remove a decided or replaced verdict from its journal row."""
        if verdict.row_coid is None:
            return

        def drop(extras: dict) -> None:
            saved = extras.get(EXTRAS_KEY_HELD_EDIT_VERDICTS)
            if not isinstance(saved, dict) or held_id not in saved:
                return
            held = {k: v for k, v in saved.items() if k != held_id}
            if held:
                extras[EXTRAS_KEY_HELD_EDIT_VERDICTS] = held
            else:
                del extras[EXTRAS_KEY_HELD_EDIT_VERDICTS]

        self._rewrite_extras(verdict.row_coid, drop)

    def _reload_held_edit_verdicts(self) -> None:
        """Pick up the verdicts an earlier process held and did not decide.

        Each one is decided as that process would have: from the startup
        activity read on, against the time its change was confirmed.
        """
        store_ctx = self.store_ctx
        if store_ctx is None or self._connection_generation > 0:
            return
        for row in store_ctx.iter_live_orders():
            saved = (row.extras or {}).get(EXTRAS_KEY_HELD_EDIT_VERDICTS)
            if not isinstance(saved, dict):
                continue
            for held_id, record in saved.items():
                if not isinstance(record, dict) or held_id in self._held_edit_verdicts:
                    continue
                verdict = self._held_verdict({**record, 'row_coid': row.client_order_id})
                if verdict is None:
                    continue
                self._held_edit_verdicts[str(held_id)] = verdict
                if record.get('kind') == 'attributes':
                    self._working_attributes_reported.add(
                        (verdict.deal_id, _attribute_signature(record.get('attributes'))))
                store_ctx.log_event(
                    'level_edit_verdict_reloaded',
                    client_order_id=row.client_order_id,
                    exchange_order_id=verdict.deal_id,
                    payload={'held_id': held_id, 'observed_at': verdict.observed_at,
                             'provisional': verdict.provisional},
                )

    def _note_level_edit(self, activity: dict) -> None:
        """Remember one accepted edit activity: who edited which deal's levels.

        A position's stop / limit edit (``EDIT_STOP_AND_LIMIT``) and a resting
        order's amend (``WORKING_ORDER`` ``MODIFIED``) both count. Recorded
        for every activity the poll and the startup read return (the bot's
        own echoes included), kept for :data:`_LEVEL_EDIT_RETAIN_S` after a
        read last served it. The edits of a deal a verdict is held on are
        kept as long as the verdict waits: they are its evidence, however
        long the activity reads failed in between.
        """
        kind = str(activity.get('type') or '').upper()
        status = str(activity.get('status') or '').upper()
        if not ((kind == 'EDIT_STOP_AND_LIMIT' and status in ('ACCEPTED', 'EXECUTED'))
                or (kind == 'WORKING_ORDER' and status == 'MODIFIED')):
            return
        deal_id = _wire_id(activity.get('dealId'))
        activity_ts = _parse_iso_timestamp(str(activity.get('dateUTC') or ''))
        if deal_id is None or activity_ts <= 0.0:
            return
        now = epoch_time()
        edits = self._level_edits.setdefault(deal_id, {})
        identity = _close_identity(activity)
        seen = edits.get(identity)
        edits[identity] = (activity_ts, str(activity.get('source') or '').upper(),
                           seen[2] if seen is not None else now, now)
        horizon = now - _LEVEL_EDIT_RETAIN_S
        held_deals = {verdict.deal_id for verdict in self._held_edit_verdicts.values()}
        for stale_deal in list(self._level_edits):
            if stale_deal in held_deals:
                continue
            kept = {k: v for k, v in self._level_edits[stale_deal].items()
                    if max(v[0], v[3]) >= horizon}
            if kept:
                self._level_edits[stale_deal] = kept
            else:
                del self._level_edits[stale_deal]

    def _edits_after(self, deal_id: str, key: str) -> float:
        """The earliest venue time of an edit that can explain a change of ``key`` now.

        The floor of the bot's last own write on ``key`` (the one this process
        sent, or, after a restart, the one the guard persisted on the row):
        the write replaced every level, so no older edit shows. Past the
        venue or dealing-desk edit a verdict already accepted
        (:meth:`_consume_level_edit`), which explains no later change. Past
        the operator resume this process applies: an edit the venue dates at
        or before it was accepted by the resume (the boundary
        :meth:`~pynecore.core.plugin.broker.BrokerPlugin.report_external_intervention`
        applies), so it never decides a change judged after it. Without any
        of them every edit counts.
        """
        written_at = self._protection_written_at.get(key)
        if written_at is None:
            written_at = self._persisted_write_time(deal_id, key)
        after = (self._venue_time_floor(written_at) if written_at is not None
                 else float('-inf'))
        consumed = self._consumed_edit_time(deal_id, key)
        if consumed is not None:
            after = max(after, math.nextafter(consumed, math.inf))
        resume_ts_ms = self.operator_resume_ts_ms
        if resume_ts_ms is not None:
            after = max(after, math.nextafter(resume_ts_ms / 1000.0, math.inf))
        return after

    def _consumed_edit_time(self, deal_id: str, key: str) -> float | None:
        """Venue time of the newest edit of ``key`` a verdict accepted, if recorded."""
        store_ctx = self.store_ctx
        if store_ctx is None:
            return None
        row = store_ctx.find_by_ref('deal_id', deal_id)
        saved = (row.extras or {}).get(EXTRAS_KEY_LEVEL_EDITS_CONSUMED) if row is not None else None
        if not isinstance(saved, dict):
            return None
        return _wire_float(saved.get(key), finite=True)

    def _consume_level_edit(self, deal_id: str, key: str, activity_ts: float) -> None:
        """Record that a verdict accepted the edit of ``key`` dated ``activity_ts``."""
        store_ctx = self.store_ctx
        if store_ctx is None:
            return
        row = store_ctx.find_by_ref('deal_id', deal_id)
        if row is None or row.closed_ts_ms is not None:
            return

        def consume(extras: dict) -> None:
            saved = extras.get(EXTRAS_KEY_LEVEL_EDITS_CONSUMED)
            consumed = dict(saved) if isinstance(saved, dict) else {}
            previous = _wire_float(consumed.get(key), finite=True)
            consumed[key] = max(activity_ts, previous) if previous is not None else activity_ts
            extras[EXTRAS_KEY_LEVEL_EDITS_CONSUMED] = consumed

        self._rewrite_extras(row.client_order_id, consume)

    def _persisted_write_time(self, deal_id: str, key: str) -> float | None:
        """When an earlier process last sent an own write on ``key`` (wall clock), if recorded."""
        store_ctx = self.store_ctx
        if store_ctx is None:
            return None
        row = store_ctx.find_by_ref('deal_id', deal_id)
        if row is None or row.closed_ts_ms is not None:
            return None
        saved = (row.extras or {}).get(PROTECTION_LEVELS_EXTRA)
        if not isinstance(saved, dict) or saved.get('key') != key:
            return None
        written_at = _wire_float(saved.get('written_at'), finite=True)
        return written_at if written_at is not None and written_at > 0.0 else None

    def _venue_time_floor(self, local_ts: float) -> float:
        """The earliest venue time a local wall-clock instant can correspond to.

        The measured venue-clock floor (:meth:`_note_venue_clock`) less
        :data:`_VENUE_CLOCK_MARGIN_S`, never ahead of the local time; without
        a sample the generous :data:`_CLOCK_SKEW_S`.
        """
        samples = [sample for _, _, sample in self._venue_clock_samples]
        if not samples:
            return local_ts - _CLOCK_SKEW_S
        return local_ts + min(0.0, max(samples) - _VENUE_CLOCK_MARGIN_S)

    def _venue_time_ceiling(self, local_ts: float) -> float:
        """The latest venue time a local wall-clock instant can correspond to.

        A clock sample is the newest ``dateUTC`` of an activity read less the
        local time it returned at. The venue publishes an activity at most
        :data:`~pynecore_capitalcom.helpers._ACTIVITY_LAG_S` after its deal,
        so a read that returned a fresh one (an own write's edit, read within
        the lag) bounds the venue clock from above too: at most that lag ahead
        of the sample. Never earlier than the local time plus the lag; without
        a sample the generous :data:`_CLOCK_SKEW_S`.
        """
        samples = [sample for _, _, sample in self._venue_clock_samples]
        if not samples:
            return local_ts + _CLOCK_SKEW_S
        return local_ts + max(0.0, max(samples)) + _ACTIVITY_LAG_S

    def _accept_observed_levels(self, key: str, context: dict) -> None:
        """Make the levels a confirmed change left on ``key`` the guard's baseline.

        A key the guard stopped guarding meanwhile (the position closed, the
        native fail-safe took it over) keeps no baseline.
        """
        try:
            observed = ProtectionLevels.from_list(list(context.get('observed') or []))
        except (TypeError, ValueError):
            observed = None
        guard = self._protection_guard()
        coid = context.get('client_order_id')
        if guard.known(key):
            guard.forget(key)
            if observed is not None:
                guard.adopt(key, observed, row_coid=coid if isinstance(coid, str) else None)

    def _adopt_venue_level_edit(
            self, deal_id: str, key: str, context: dict, edit: tuple[float, str],
            *, own_write: bool,
    ) -> None:
        """Accept the levels the venue or its dealing desk put on a bot position or order.

        Not an intervention: the venue's levels replace the bot's as the
        accepted baseline (:meth:`_accept_observed_levels`), audited as
        ``protection_venue_edit_adopted``, and the edit explains no later
        change (:meth:`_consume_level_edit`). A dealing-desk edit is a
        user-relevant decision of the broker: logged as a WARNING, like a
        dealing-desk close. When the bot wrote the key while the verdict
        waited, the baseline was taken before that write and the bot's own
        levels stand.
        """
        activity_ts, source = edit
        coid = context.get('client_order_id')
        if not own_write:
            self._accept_observed_levels(key, context)
        self._consume_level_edit(deal_id, key, activity_ts)
        message = (
            "bot %s %s: %s edited its %s (expected %s, found %s); "
            "the venue's levels are accepted, not an intervention"
        )
        working = key.startswith(_WORKING_KEY_PREFIX)
        args = ('working order' if working else 'position', deal_id,
                'the dealing desk' if _activity_actor(source) == ACTOR_DEALER else 'the venue',
                'order level' if working else 'stop/limit levels',
                context.get('expected'), context.get('observed'))
        if _activity_actor(source) == ACTOR_DEALER:
            logger.warning(message, *args)
        else:
            logger.info(message, *args)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'protection_venue_edit_adopted',
                client_order_id=coid if isinstance(coid, str) else None,
                exchange_order_id=deal_id,
                payload={'source': source, 'activity_ts': activity_ts,
                         'protection_key': key,
                         'expected': context.get('expected'),
                         'observed': context.get('observed'),
                         'own_write_while_held': own_write},
            )

    def _adopt_levels_after_size_change(
            self, coid: str, deal_id: str, key: str, levels: ProtectionLevels,
            size: float | None, edit: tuple[float, str] | None, *, own_write: bool,
    ) -> None:
        """Accept the levels a bot position shows after its size changed.

        The venue's effect of a reduction on the protection is not
        documented, so levels the bot did not set that no one else's edit
        explains are taken as the venue's: the guard's baseline from now on
        (unless the bot wrote the protection meanwhile, then its own levels
        stand). A dealing-desk edit is logged as a WARNING.
        """
        def adopted(extras: dict) -> None:
            extras.pop(EXTRAS_KEY_PROTECTION_ADOPT_AFTER, None)
            extras.pop(EXTRAS_KEY_PROTECTION_HANDOVER, None)

        self._rewrite_extras(coid, adopted)
        if not own_write:
            self._protection_guard().adopt(key, levels, row_coid=coid)
        source = edit[1] if edit is not None else None
        if edit is not None:
            self._consume_level_edit(deal_id, key, edit[0])
        if _activity_actor(source) == ACTOR_DEALER:
            logger.warning(
                "bot position %s: the dealing desk edited its stop/limit levels around "
                "a reduction (found %s); accepted, not an intervention",
                deal_id, levels.as_list(),
            )
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'protection_adopted_after_size_change',
                client_order_id=coid,
                exchange_order_id=deal_id,
                payload={'levels': levels.as_list(), 'size': size, 'source': source,
                         'own_write_while_held': own_write},
            )

    def _report_size_change_edit(
            self, coid: str, deal_id: str, key: str, levels: ProtectionLevels,
            expected: list, *, changed_at_ms: int | None = None, own_write: bool = False,
    ) -> None:
        """Report levels someone else set on a bot position around a reduction.

        The reported levels become the guard's baseline, so the same change is
        reported once; nothing is written back. When the bot wrote the
        protection while the verdict waited, its own levels stay the baseline:
        the venue carrying them is no further change.

        :param changed_at_ms: When the change was made at the latest (Unix ms).
        :param own_write: The bot wrote ``key`` while the verdict waited.
        """
        self._rewrite_extras(
            coid, lambda extras: extras.pop(EXTRAS_KEY_PROTECTION_ADOPT_AFTER, None))
        if not own_write:
            self._protection_guard().adopt(key, levels, row_coid=coid)
        reason = (
            f"bot protection {key!r} changed on the venue by someone else around a "
            f"reduction of the position: expected one of {expected}, found "
            f"{levels.as_list()}"
        )
        self._report_level_edit(reason, {
            'protection_key': key, 'expected': expected,
            'observed': levels.as_list(), 'client_order_id': coid,
        }, deal_id, changed_at_ms=changed_at_ms)

    @staticmethod
    def _levels_from_put_body(body: dict) -> ProtectionLevels:
        """The protection a ``PUT /positions/{dealId}`` body leaves on the position.

        The PUT replaces every bracket field: a field the body omits is
        cleared, so an omitted level is an absent level here.
        """
        trailing = body.get('trailingStop') is True
        return ProtectionLevels(
            stop=None if trailing else _wire_float(body.get('stopLevel'), finite=True),
            limit=_wire_float(body.get('profitLevel'), finite=True),
            trailing=trailing,
            trail_distance=(_wire_float(body.get('stopDistance'), finite=True)
                            if trailing else None),
        )

    @staticmethod
    def _levels_from_position(pos: dict) -> ProtectionLevels:
        """The protection a ``GET /positions`` row shows.

        A level the payload leaves out is read as absent, the same reading
        the leg-disposition resolver and the native fail-safe feed use: the
        venue carries no bracket field it was not given. A trailing stop's
        distance missing from the payload stays unknown (``None``), which the
        guard never counts as a mismatch.
        """
        data = pos.get('position') or {}
        trailing = bool(data.get('trailingStop'))
        return ProtectionLevels(
            stop=_wire_float(data.get('stopLevel'), finite=True),
            limit=_wire_float(data.get('profitLevel'), finite=True),
            trailing=trailing,
            trail_distance=(_wire_float(data.get('stopDistance'), finite=True)
                            if trailing else None),
        )

    def _protected_entry_row(self, deal_id: str) -> 'OrderRow | None':
        """The live bot position row a protection write on ``deal_id`` targets.

        ``None`` when the write is not guarded: no journal, no live position
        row for this exact deal, or a parent with a live software
        partial-bracket leg, whose stop the native fail-safe manager owns.
        """
        store_ctx = self.store_ctx
        if store_ctx is None or not deal_id:
            return None
        row = store_ctx.find_by_ref('deal_id', deal_id)
        if (row is None or row.closed_ts_ms is not None
                or row.exchange_order_id != deal_id):
            return None
        if (row.extras or {}).get('kind') != ENTRY_KIND_POSITION:
            return None
        if row.client_order_id in self._failsafe_parent_refs():
            return None
        return row

    @contextmanager
    def _protection_write(self, deal_id: str, body: dict) -> Iterator[_ProtectionWrite]:
        """Record one own ``PUT /positions/{deal_id}`` on the guard.

        ``expect`` runs before the caller sends the PUT; on exit the write is
        settled with its levels when the caller marked it
        :meth:`~._ProtectionWrite.accepted`, as refused (its levels stop being
        acceptable) when it was marked :meth:`~._ProtectionWrite.refused`,
        and as unknown otherwise (the guard resolves it from later
        snapshots). A position whose stop the native fail-safe manager wrote
        is taken back from it first (:meth:`_release_failsafe_for_own_write`).

        A write the guard does not track (the native fail-safe manager owns
        the position's stop) is still recorded as the bot's own
        (:meth:`_note_own_protection_write`).

        :param deal_id: The position the PUT targets.
        :param body: The PUT body, exactly as sent.
        """
        write = _ProtectionWrite(self._levels_from_put_body(body))
        row = self._protected_entry_row(deal_id)
        if row is None:
            sent_at = epoch_time()
            with self._own_write_in_flight(_protection_key(deal_id)):
                try:
                    yield write
                finally:
                    if not write.rejected:
                        self._note_own_protection_write(deal_id, sent_at, epoch_time())
            return
        released = self._release_failsafe_for_own_write(row)
        with self._guarded_write(_protection_key(deal_id), write, row.client_order_id):
            yield write
        if released and write.ok:
            self._rewrite_extras(
                row.client_order_id,
                lambda extras: extras.pop(EXTRAS_KEY_PROTECTION_HANDOVER, None),
            )

    def _release_failsafe_for_own_write(self, row: 'OrderRow') -> bool:
        """Take a position back from the native fail-safe manager for the bot's own write.

        The manager owns a stop it wrote until the engine releases the parent,
        which it does before it writes the parent's protection itself (a
        whole-row exit replacing a partial bracket whose last leg is gone).
        This own write is therefore the bot's again, and the guard judges the
        position from it on. Until the venue confirms the write it may still
        carry the manager's levels, which a baseline taken meanwhile adopts as
        the bot's own (``native_failsafe_handover``).

        :param row: The bot position row about to be written.
        :return: Whether the position was the manager's until now.
        """
        deal_id = row.exchange_order_id or ''
        stamped = (row.extras or {}).get(EXTRAS_KEY_FAILSAFE_OWNED) is not None
        if deal_id not in self._failsafe_deal_ids and not stamped:
            return False
        self._failsafe_deal_ids.discard(deal_id)
        released_at = epoch_time()

        def release(extras: dict) -> None:
            extras.pop(EXTRAS_KEY_FAILSAFE_OWNED, None)
            extras[EXTRAS_KEY_PROTECTION_HANDOVER] = released_at

        self._rewrite_extras(row.client_order_id, release)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'native_failsafe_released',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload={'reason': 'own_protection_write'},
            )
        return True

    @contextmanager
    def _guarded_write(self, key: str, write: _ProtectionWrite, row_coid: str) -> Iterator[None]:
        """Expect ``write`` on the guard around the request; settle it from its outcome.

        A verdict held on ``key`` is prepared for the write first
        (:meth:`_keep_held_verdicts_past_own_write`). A write the venue did not
        refuse is recorded as the bot's own (:meth:`_own_write_claims`); until
        then the verdicts held on ``key`` wait (:meth:`_own_write_in_flight`).
        """
        self._keep_held_verdicts_past_own_write(key)
        guard = self._protection_guard()
        write_id = guard.expect(key, write.levels, row_coid=row_coid)
        self._protection_in_flight[key] = self._protection_in_flight.get(key, 0) + 1
        sent_at = epoch_time()
        self._protection_written_at[key] = sent_at
        try:
            with self._own_write_in_flight(key):
                try:
                    yield
                finally:
                    if not write.rejected:
                        self._note_own_level_write(row_coid, key, sent_at, epoch_time())
        finally:
            remaining = self._protection_in_flight.get(key, 1) - 1
            if remaining > 0:
                self._protection_in_flight[key] = remaining
            else:
                self._protection_in_flight.pop(key, None)
            self._protection_written_at[key] = epoch_time()
            if write.ok:
                guard.settle(key, write.levels, write=write_id)
            elif write.rejected:
                guard.settle(key, None, rejected=write.levels, write=write_id)
            else:
                guard.settle(key, None, write=write_id)

    # --- Working orders ------------------------------------------------------

    @contextmanager
    def _working_order_write(
            self, row: 'OrderRow', level: float, *, previous_level: float | None,
    ) -> Iterator[_ProtectionWrite]:
        """Record one own ``PUT /workingorders/{dealId}`` (a level amend) on the guard.

        Same protocol as :meth:`_protection_write`: expected before the PUT
        leaves, settled from the caller's verdict.

        :param row: The own working-order row the PUT targets.
        :param level: The new order level the PUT sets.
        :param previous_level: The level the amend replaces, as the bot last
            set it. The guard's baseline when the row records none, so an
            amend the venue never applied does not leave the bot's own
            previous level unaccepted.
        """
        write = _ProtectionWrite(_working_levels((row.extras or {}).get('order_type'), level))
        deal_id = row.exchange_order_id
        if self.store_ctx is None or not deal_id:
            yield write
            return
        key = _working_key(deal_id)
        self._ensure_working_baseline(row, key, fallback=previous_level)
        with self._guarded_write(key, write, row.client_order_id):
            yield write

    def _ensure_working_baseline(
            self, row: 'OrderRow', key: str, *, fallback: float | None = None,
    ) -> None:
        """Seed the guard with the level the bot placed (or last amended) the order at.

        :param fallback: The bot's own level to seed when the row records none.
        """
        guard = self._protection_guard()
        if guard.known(key):
            return
        extras = row.extras or {}
        level = _wire_float(extras.get('amended_level'), finite=True)
        if level is None:
            level = _wire_float(extras.get(EXTRAS_KEY_ORDER_LEVEL), finite=True)
        if level is None and fallback is not None:
            level = _wire_float(fallback, finite=True)
        if level is None:
            return
        guard.adopt(key, _working_levels(extras.get('order_type'), level),
                    row_coid=row.client_order_id)

    def _forget_working_order_guard(self, deal_id: str | None) -> None:
        """Stop guarding the working order ``deal_id``: it filled or is gone."""
        if self.store_ctx is None or not deal_id:
            return
        self._working_attributes_seen.pop(deal_id, None)
        key = _working_key(deal_id)
        self._protection_in_flight.pop(key, None)
        self._protection_written_at.pop(key, None)
        guard = self._protection_guard()
        if guard.known(key):
            guard.forget(key)

    async def _observe_working_orders(
            self,
            working_by_deal: dict[str, dict],
            read_started_at: float | None,
    ) -> None:
        """Judge every resting own entry order's level in this ``/workingorders`` snapshot.

        Skipped while the bot itself may be changing the order: an amend
        command the venue has not refused, a cancel the bot asked for, a fill
        already seen. A level no own write explains, on two consecutive
        polls, is reported like a moved stop.

        :param working_by_deal: The poll's ``GET /workingorders`` rows by ``dealId``.
        :param read_started_at: Wall-clock time the poll's reads started.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return
        guard = self._protection_guard()
        live = list(store_ctx.iter_live_orders())
        busy_coids: set[str] = set()
        for row in live:
            extras = row.extras or {}
            if row.state != STATE_REJECTED and extras.get('kind') == KIND_MODIFY_ENTRY:
                target = extras.get('target_coid')
                if isinstance(target, str):
                    busy_coids.add(target)
        for row in live:
            extras = row.extras or {}
            deal_id = row.exchange_order_id
            if (not deal_id or extras.get('kind') != ENTRY_KIND_WORKING
                    or extras.get(EXTRAS_KEY_LEG_KIND) is not None
                    or row.state != 'confirmed'
                    or extras.get('entry_filled_at') is not None
                    or extras.get(EXTRAS_KEY_CANCEL_REQUESTED) is not None
                    or row.client_order_id in busy_coids):
                continue
            order = working_by_deal.get(deal_id)
            if order is None:
                continue
            data = order.get('workingOrderData') or {}
            self._judge_working_order_attributes(row, deal_id, data, read_started_at)
            key = _working_key(deal_id)
            self._ensure_working_baseline(row, key)
            if not guard.known(key):
                continue
            venue = _working_levels(
                data.get('orderType') or extras.get('order_type'),
                _wire_float(data.get('orderLevel'), finite=True),
            )
            tick = await self._price_tick(row.symbol)
            if tick <= 0.0:
                continue
            self._guard_observe(key, venue, tick=self._working_order_judge_tick(extras, tick),
                                read_started_at=read_started_at)

    def _judge_working_order_attributes(
            self, row: 'OrderRow', deal_id: str, data: dict, read_started_at: float | None,
    ) -> None:
        """Judge the attributes the bot never sets on its resting order ``deal_id``.

        The bot's own order carries none of them
        (:func:`_foreign_working_order_attributes`), unless an operator resume
        accepted the order as the venue showed it: the first read after the
        resume becomes the accepted state, persisted on the row with the
        resume's time, so a later process applying the same resume judges
        against it instead of accepting again. Anything else, on two
        consecutive polls, is someone else's edit of the bot's order: a stop
        or take-profit added, an expiry set (the order expiring later would
        be the venue's own mechanics, the expiry is not), a guaranteed or
        trailing flag. Reported once per edit like a moved level. Attributes
        the venue or its dealing desk set (:func:`_activity_actor`), as the
        newest edit activity on the order since the bot's last own amend says,
        become the accepted state instead (WARNING for the dealing desk), like
        the levels of :meth:`_adopt_venue_level_edit`. Either verdict waits
        for the activity feed to cover the change (:meth:`_hold_edit_verdict`).

        :param row: The own working-order row.
        :param deal_id: Its ``dealId``.
        :param data: The order's ``workingOrderData`` in this poll.
        :param read_started_at: Wall-clock time the poll's reads started.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return
        attributes = _foreign_working_order_attributes(data)
        accepted = (row.extras or {}).get(EXTRAS_KEY_WORKING_ACCEPTED_ATTRIBUTES)
        accepted = accepted if isinstance(accepted, dict) else {}
        resume_ts_ms = self.operator_resume_ts_ms
        if resume_ts_ms is not None and accepted.get('resume_ts_ms') != resume_ts_ms:
            record = {'attributes': attributes, 'resume_ts_ms': resume_ts_ms}
            self._rewrite_extras(
                row.client_order_id,
                lambda extras: extras.__setitem__(EXTRAS_KEY_WORKING_ACCEPTED_ATTRIBUTES, record),
            )
            self._working_attributes_seen.pop(deal_id, None)
            if attributes:
                store_ctx.log_event(
                    'working_order_attributes_accepted_by_resume',
                    client_order_id=row.client_order_id,
                    exchange_order_id=deal_id,
                    payload=record,
                )
            return
        expected = [list(pair) for pair in accepted.get('attributes') or ()]
        if attributes == expected:
            self._working_attributes_seen.pop(deal_id, None)
            return
        signature = tuple(tuple(pair) for pair in attributes)
        if self._working_attributes_seen.get(deal_id) != signature:
            # First sighting: a second read decides, like a moved level.
            self._working_attributes_seen[deal_id] = signature
            return
        if (deal_id, signature) in self._working_attributes_reported:
            return
        # Judged once per change; the verdict waits for the activity feed
        # like a moved level's.
        self._working_attributes_reported.add((deal_id, signature))
        key = _working_key(deal_id)
        verdict = self._held_verdict({
            'kind': 'attributes', 'deal_id': deal_id, 'guard_key': key,
            'observed_at': read_started_at if read_started_at is not None else epoch_time(),
            'edits_after': _finite_or_none(self._edits_after(deal_id, key)),
            'row_coid': row.client_order_id, 'symbol': row.symbol,
            'attributes': attributes, 'expected': expected,
            'resume_ts_ms': accepted.get('resume_ts_ms'),
        })
        if verdict is not None:
            self._hold_edit_verdict(_ATTRIBUTES_HELD_PREFIX + deal_id, verdict)

    def _adopt_venue_working_attributes(
            self, coid: str, deal_id: str, record: dict, expected: list,
            edit: tuple[float, str],
    ) -> None:
        """Accept the attributes the venue or its dealing desk set on a resting bot order."""
        attributes = record.get('attributes') or []
        self._rewrite_extras(
            coid, lambda extras: extras.__setitem__(EXTRAS_KEY_WORKING_ACCEPTED_ATTRIBUTES, record),
        )
        self._working_attributes_seen.pop(deal_id, None)
        activity_ts, source = edit
        self._consume_level_edit(deal_id, _working_key(deal_id), activity_ts)
        message = (
            "bot working order %s: %s set %s on it (expected %s); accepted, "
            "not an intervention"
        )
        args = (deal_id, 'the dealing desk' if _activity_actor(source) == ACTOR_DEALER else 'the venue',
                dict(attributes) or 'none', dict(expected) or 'none')
        if _activity_actor(source) == ACTOR_DEALER:
            logger.warning(message, *args)
        else:
            logger.info(message, *args)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'working_order_venue_edit_adopted',
                client_order_id=coid,
                exchange_order_id=deal_id,
                payload={'source': source, 'activity_ts': activity_ts,
                         'attributes': attributes, 'expected': expected},
            )

    def _report_working_attributes(
            self, coid: str, deal_id: str, symbol: str, attributes: list, expected: list,
            *, changed_at_ms: int | None = None,
    ) -> None:
        """Report attributes someone else set on a resting bot order.

        :param changed_at_ms: When the change was made at the latest (Unix ms).
        """
        reason = (
            f"bot working order {deal_id} changed on the venue by someone else: it "
            f"carries {dict(attributes) or 'none'} instead of "
            f"{dict(expected) or 'no stop, take-profit, expiry or stop flag'}"
        )
        logger.error("%s", reason)
        self.report_external_intervention(
            reason,
            {'working_order': deal_id, 'symbol': symbol,
             'observed_attributes': attributes, 'expected_attributes': expected},
            client_order_id=coid,
            exchange_order_id=deal_id,
            changed_at_ms=changed_at_ms,
        )

    @staticmethod
    def _working_order_judge_tick(extras: dict, tick: float) -> float:
        """The tick the guard judges a resting entry order's level with.

        The bot may place or amend an order at a level between two price
        ticks; how the venue keeps such a level on its grid (nearest,
        truncated) is not documented. While a level the bot recorded is off
        the grid, either grid neighbour is the bot's own: the guard then
        tolerates just under a whole tick instead of half of one. A recorded
        level on the grid — a venue echo, or a request on the grid — is
        judged at half a tick.
        """
        own_levels = [
            level for level in (_wire_float(extras.get('amended_level'), finite=True),
                                _wire_float(extras.get(EXTRAS_KEY_ORDER_LEVEL), finite=True))
            if level is not None
        ]
        if any(not _level_on_tick_grid(level, tick) for level in own_levels):
            return 2.0 * tick * (1.0 - 1e-6)
        return tick

    def _forget_protection(self, deal_id: str) -> None:
        """Stop guarding ``deal_id``: the position is gone or no longer ours to judge."""
        if self.store_ctx is None or not deal_id:
            return
        key = _protection_key(deal_id)
        self._protection_sizes.pop(key, None)
        self._protection_written_at.pop(key, None)
        self._protection_guard().forget(key)

    def _failsafe_parent_refs(self) -> set[str]:
        """Entry COIDs with a live software partial-bracket leg.

        The engine registers exactly these parents with its native fail-safe
        manager, which then owns their venue stop.
        """
        refs: set[str] = set()
        if self.store_ctx is None:
            return refs
        for row in self.store_ctx.iter_live_orders():
            extras = row.extras or {}
            if extras.get(EXTRAS_KEY_LEG_KIND) in ENGINE_TRIGGER_LEG_KINDS:
                parent = extras.get(EXTRAS_KEY_PARENT_ENTRY_DISPATCH_REF)
                if isinstance(parent, str):
                    refs.add(parent)
        return refs

    def _failsafe_owned(self, row: 'OrderRow', failsafe_parents: set[str]) -> bool:
        """Whether the native fail-safe manager owns ``row``'s protection.

        It does while the parent has a live software partial-bracket leg (the
        engine registers exactly those parents, also after a restart), and,
        in this process, from its own stop write until the bot's own next
        protection write (:meth:`_release_failsafe_for_own_write`).
        """
        return (row.client_order_id in failsafe_parents
                or row.exchange_order_id in self._failsafe_deal_ids)

    def _claim_failsafe_protection(self, deal_ids: Iterable[str]) -> None:
        """Hand ``deal_ids`` over to the native fail-safe manager, before its PUT.

        The manager compares the venue against its own desired levels and
        raises its own external-edit event, so the guard stops judging these
        positions — judging them too would double-report — until the bot
        writes their protection itself again. The claim is persisted on the
        row: a restarted engine does not register a parent without partial
        legs again, and the levels the manager left then become the guarded
        baseline (:meth:`_observe_protection`).
        """
        store_ctx = self.store_ctx
        for deal_id in deal_ids:
            self._failsafe_deal_ids.add(deal_id)
            self._forget_protection(deal_id)
            if store_ctx is None:
                continue
            row = store_ctx.find_by_ref('deal_id', deal_id)
            if row is None or row.closed_ts_ms is not None:
                continue
            if (row.extras or {}).get(EXTRAS_KEY_FAILSAFE_OWNED) is None:
                stamped_at = epoch_time()
                self._rewrite_extras(
                    row.client_order_id,
                    lambda extras: extras.__setitem__(EXTRAS_KEY_FAILSAFE_OWNED, stamped_at),
                )

    async def _price_tick(self, symbol: str) -> float:
        """The instrument's price tick, ``0.0`` when unknown."""
        rules = self._instrument_rules_cache.get(symbol)
        if rules is None:
            try:
                rules = await self._get_instrument_rules(symbol)
            except (BrokerError, CapitalComError, httpx.HTTPError, ConnectionError):
                rules = None
        tick = rules.price_tick if rules is not None else 0.0
        sym_info = self._sym_info
        if tick <= 0.0 and sym_info is not None and sym_info.ticker == symbol:
            tick = float(sym_info.mintick)
        return tick if math.isfinite(tick) and tick > 0.0 else 0.0

    async def _observe_protection(
            self,
            positions_by_deal: dict[str, dict],
            read_started_at: float | None,
    ) -> None:
        """Judge every bot position in this ``/positions`` snapshot.

        A position is skipped — nothing is judged, no mismatch is counted —
        while the bot itself may be changing it: a bracket leg row whose
        venue outcome is unsettled, a trailing activation whose PUT is in
        flight or not yet visible, an amend or cancel command for it that the
        venue has not refused, an own close in flight, a size change (a
        partial close, whose effect on the bracket the venue does not
        document). After an own or venue partial close the next snapshot that
        started later becomes the accepted protection when its levels are the
        bot's own, or the venue's: levels the bot did not set are judged from
        the edit activities like a moved level, someone else's edit reported
        and anything else accepted (:meth:`_hold_size_change_verdict`). On a
        position the bot left bare a reduction puts no protection: a level
        that snapshot shows is judged like any level someone else adds.

        A filled bot position the guard holds no levels for gets the venue's
        current protection as its baseline
        (:meth:`_adopt_unrecorded_protection`): "no stop, no take-profit" when
        nothing is set, so a level someone else adds later is a change of the
        bot's own state; the bot's own unrecorded levels (a released native
        fail-safe parent, a position opened by an earlier plugin version)
        otherwise. Levels nothing explains are accepted with an ERROR, never
        reported.

        :param positions_by_deal: The poll's ``GET /positions`` rows by ``dealId``.
        :param read_started_at: Wall-clock time the snapshot read started.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return
        guard = self._protection_guard()
        live = list(store_ctx.iter_live_orders())
        busy_deals: set[str] = set()
        busy_coids: set[str] = set()
        failsafe_parents: set[str] = set()
        for row in live:
            extras = row.extras or {}
            leg_kind = extras.get(EXTRAS_KEY_LEG_KIND)
            kind = extras.get('kind')
            if leg_kind in ENGINE_TRIGGER_LEG_KINDS:
                parent = extras.get(EXTRAS_KEY_PARENT_ENTRY_DISPATCH_REF)
                if isinstance(parent, str):
                    failsafe_parents.add(parent)
            elif leg_kind in ('tp', 'sl'):
                if (row.state in _UNSETTLED_LEG_STATES
                        or extras.get('trail_state') in _TRAIL_ACTIVATING_STATES):
                    parent_deal = _wire_id(extras.get('parent_deal_id'))
                    if parent_deal is not None:
                        busy_deals.add(parent_deal)
            elif row.state == STATE_REJECTED:
                # A refused command stays live for audit only; nothing of it
                # can still land on the venue.
                continue
            elif kind == KIND_MODIFY_EXIT:
                target = extras.get('target_coid')
                if isinstance(target, str):
                    busy_coids.add(target)
            elif kind == KIND_CANCEL:
                for target_coid in extras.get('target_coids') or ():
                    target = store_ctx.get_order(str(target_coid))
                    parent_deal = (_wire_id((target.extras or {}).get('parent_deal_id'))
                                   if target is not None else None)
                    if parent_deal is not None:
                        busy_deals.add(parent_deal)
            elif kind == KIND_PARTIAL_CLOSE:
                target_deal = _wire_id(extras.get('target_deal_id'))
                if target_deal is not None:
                    busy_deals.add(target_deal)

        now = epoch_time()
        for row in live:
            extras = row.extras or {}
            deal_id = row.exchange_order_id
            if (not deal_id or extras.get('kind') != ENTRY_KIND_POSITION
                    or extras.get(EXTRAS_KEY_LEG_KIND) is not None):
                continue
            key = _protection_key(deal_id)
            if extras.get('natural_close_at') is not None:
                self._forget_protection(deal_id)
                continue
            if self._failsafe_owned(row, failsafe_parents):
                self._forget_protection(deal_id)
                if extras.get(EXTRAS_KEY_PROTECTION_HANDOVER) is None:
                    self._rewrite_extras(
                        row.client_order_id,
                        lambda e: e.__setitem__(EXTRAS_KEY_PROTECTION_HANDOVER, now),
                    )
                continue
            if extras.get(EXTRAS_KEY_FAILSAFE_OWNED) is not None:
                # The manager wrote this stop in an earlier process, and the
                # parent has no partial leg left for the restarted engine to
                # register it by: nobody else judges the position, so the
                # levels the manager left become the guarded baseline.
                def hand_over(e: dict) -> None:
                    e.pop(EXTRAS_KEY_FAILSAFE_OWNED, None)
                    e.setdefault(EXTRAS_KEY_PROTECTION_HANDOVER, now)

                self._rewrite_extras(row.client_order_id, hand_over)
                row = store_ctx.get_order(row.client_order_id) or row
                extras = row.extras or {}
            pos = positions_by_deal.get(deal_id)
            if pos is None:
                # The disappearance tracker owns a vanished deal.
                continue
            if (row.state != 'confirmed'
                    or deal_id in busy_deals
                    or row.client_order_id in busy_coids
                    or self._own_close_in_flight(extras, now)):
                continue
            venue = self._levels_from_position(pos)
            if (not guard.known(key)
                    and extras.get(EXTRAS_KEY_PROTECTION_ADOPT_AFTER) is None):
                if (self._protection_in_flight.get(key, 0) > 0
                        or (row.filled_qty <= 0.0 and not extras.get('entry_filled_at'))
                        or (read_started_at is not None
                            and read_started_at < self._protection_written_at.get(key, 0.0))):
                    continue
                self._forget_working_order_guard(
                    _wire_id(extras.get('promoted_from_working_order_id')))
                saved = extras.get(PROTECTION_LEVELS_EXTRA)
                if isinstance(saved, dict) and str(saved.get('key') or '').startswith(
                        _WORKING_KEY_PREFIX):
                    guard.forget(str(saved['key']))
                await self._adopt_unrecorded_protection(row, key, venue)
            size = _wire_float((pos.get('position') or {}).get('size'), finite=True)
            previous_size = self._protection_sizes.get(key)
            if size is not None:
                self._protection_sizes[key] = size
            if (previous_size is not None and size is not None
                    and abs(size - previous_size) > 1e-9 * max(1.0, abs(previous_size))):
                self._stamp_protection_adopt(
                    row.client_order_id,
                    read_started_at if read_started_at is not None else now,
                )
                continue
            adopt_after = _wire_float(
                extras.get(EXTRAS_KEY_PROTECTION_ADOPT_AFTER), finite=True,
            )
            if adopt_after is not None:
                # Adopt only a snapshot that started after the size change
                # AND after the last own protection write: an older read
                # would replace the levels the bot just put on the venue.
                if (read_started_at is None or read_started_at <= adopt_after
                        or self._protection_in_flight.get(key, 0) > 0
                        or read_started_at < self._protection_written_at.get(key, 0.0)
                        or self._size_change_verdict_held(key)):
                    continue
                current = store_ctx.get_order(row.client_order_id) or row
                current_extras = current.extras or {}
                if venue != ProtectionLevels() and self._left_bare(current_extras, key):
                    # A reduction never puts protection on a position the bot
                    # left bare: a level showing up with it is judged like any
                    # level someone else adds.
                    def judged(e: dict) -> None:
                        e.pop(EXTRAS_KEY_PROTECTION_ADOPT_AFTER, None)
                        e.pop(EXTRAS_KEY_PROTECTION_HANDOVER, None)

                    self._rewrite_extras(row.client_order_id, judged)
                else:
                    tick = await self._price_tick(row.symbol)
                    accepted = self._accepted_levels(current_extras, key)
                    if (accepted is None or tick <= 0.0
                            or any(levels.matches(venue, _level_tolerance(tick, venue))
                                   for levels in accepted)):
                        self._adopt_levels_after_size_change(
                            row.client_order_id, deal_id, key, venue, size, None,
                            own_write=False)
                    else:
                        self._hold_size_change_verdict(
                            row, key, venue, size, accepted, read_started_at)
                    continue
            tick = await self._price_tick(row.symbol)
            if tick <= 0.0:
                continue
            self._guard_observe(key, venue, tick=tick, read_started_at=read_started_at)

    def _guard_observe(
            self, key: str, venue: ProtectionLevels, *, tick: float,
            read_started_at: float | None,
    ) -> None:
        """Judge one snapshot on the guard, telling its report hook when the read started.

        The read's start dates the change a confirmed mismatch shows: the
        verdict on its author waits for an activity read that started long
        enough after it (:meth:`_hold_edit_verdict`).
        """
        self._note_held_levels_view(key, venue, tick=tick, read_started_at=read_started_at)
        self._judged_read_started_at = read_started_at
        try:
            self._protection_guard().observe(
                key, venue, tick=tick, read_started_at=read_started_at)
        finally:
            self._judged_read_started_at = None

    def _hold_size_change_verdict(
            self, row: 'OrderRow', key: str, venue: ProtectionLevels, size: float | None,
            accepted: list[ProtectionLevels], read_started_at: float,
    ) -> None:
        """Judge levels a bot position shows after its size changed that the bot did not set.

        The venue's effect of a reduction on the protection is not
        documented: such levels are the venue's unless an edit activity shows
        someone else set them. Decided like a moved level
        (:meth:`_judge_level_edits`): someone else's ``USER`` edit is reported,
        a venue or dealing-desk edit, or no edit at all, makes them the
        accepted protection (:meth:`_adopt_levels_after_size_change`).

        :param row: The bot position row.
        :param key: Its protection key.
        :param venue: The levels the snapshot shows.
        :param size: The position size the snapshot shows.
        :param accepted: The level sets the guard accepts for the position.
        :param read_started_at: Wall-clock time the snapshot read started.
        """
        deal_id = row.exchange_order_id or ''
        verdict = self._held_verdict({
            'kind': 'size_change', 'deal_id': deal_id, 'guard_key': key,
            'observed_at': read_started_at,
            'edits_after': _finite_or_none(self._edits_after(deal_id, key)),
            'row_coid': row.client_order_id, 'levels': venue.as_list(),
            'expected': [levels.as_list() for levels in accepted], 'size': size,
        })
        if verdict is not None:
            self._hold_edit_verdict(_SIZE_CHANGE_HELD_PREFIX + key, verdict, keep_held=True)

    def _size_change_verdict_held(self, key: str) -> bool:
        """Whether levels ``key`` showed after a size change are still being judged."""
        return (_SIZE_CHANGE_HELD_PREFIX + key) in self._held_edit_verdicts

    @staticmethod
    def _accepted_levels(extras: dict, key: str) -> list[ProtectionLevels] | None:
        """Every level set the guard accepts for ``key``, as persisted on the position row.

        ``None`` when the row records none for this key, or an operator resume
        re-baselines it.
        """
        saved = extras.get(PROTECTION_LEVELS_EXTRA)
        if (not isinstance(saved, dict) or saved.get('key') != key
                or saved.get('rebaseline')):
            return None
        try:
            return [ProtectionLevels.from_list(levels) for _, levels in saved.get('writes') or []]
        except (TypeError, ValueError):
            return None

    async def _adopt_unrecorded_protection(
            self, row: 'OrderRow', key: str, venue: ProtectionLevels,
    ) -> None:
        """Accept the venue's protection of a bot position the guard holds no levels for.

        The baseline is what the venue carries; how sure it is to be the
        bot's own decides only how loudly it is accepted:

        * ``native_failsafe_handover``: the native fail-safe manager owned the
          protection until now — the levels are the manager's or the bot's
          own unguarded writes.
        * ``own_bracket_legs``: the bot's live bracket leg rows carry exactly
          these levels (a position whose levels were never persisted, such as
          one opened by an earlier plugin version).
        * ``nothing_set``: no stop, no take-profit, and no own leg says
          otherwise — a level someone else adds later is judged like a moved
          one.
        * ``unverified``: anything else. Nothing proves who set these levels,
          so they are not reported: logged ERROR with a
          ``protection_baseline_unverified`` audit event, then accepted.

        :param row: The bot position row.
        :param key: Its protection key.
        :param venue: The levels the current snapshot shows.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return
        extras = row.extras or {}
        own = self._own_leg_levels(row)
        if extras.get(EXTRAS_KEY_PROTECTION_HANDOVER) is not None:
            source = 'native_failsafe_handover'
        elif own is not None:
            tick = await self._price_tick(row.symbol)
            source = ('own_bracket_legs' if own.matches(venue, _level_tolerance(tick, venue))
                      else 'unverified')
        elif venue == ProtectionLevels():
            source = 'nothing_set'
        else:
            source = 'unverified'
        self._protection_guard().adopt(key, venue, row_coid=row.client_order_id)
        if extras.get(EXTRAS_KEY_PROTECTION_HANDOVER) is not None:
            self._rewrite_extras(
                row.client_order_id,
                lambda e: e.pop(EXTRAS_KEY_PROTECTION_HANDOVER, None),
            )
        payload = {'levels': venue.as_list(), 'source': source,
                   'own_legs': own.as_list() if own is not None else None}
        if source == 'unverified':
            logger.error(
                "bot position %s (%s) carries protection no own record explains "
                "(venue %s, own bracket legs %s): accepted as the baseline, not "
                "reported — nothing shows who set it",
                row.exchange_order_id, row.symbol, venue.as_list(),
                own.as_list() if own is not None else None,
            )
            store_ctx.log_event(
                'protection_baseline_unverified',
                client_order_id=row.client_order_id,
                exchange_order_id=row.exchange_order_id,
                payload=payload,
            )
            return
        store_ctx.log_event(
            'protection_baseline_adopted',
            client_order_id=row.client_order_id,
            exchange_order_id=row.exchange_order_id,
            payload=payload,
        )

    def _own_leg_levels(self, row: 'OrderRow') -> ProtectionLevels | None:
        """The protection the bot's live bracket leg rows put on ``row``'s deal.

        ``None`` when the position has no live own TP / SL leg row (hedging
        mode keeps none). A trailing stop whose activation is still pending is
        the leg's fixed stop, as the venue carries it.
        """
        store_ctx = self.store_ctx
        deal_id = row.exchange_order_id
        if store_ctx is None or not deal_id:
            return None
        tp_leg: OrderRow | None = None
        sl_leg: OrderRow | None = None
        for leg in store_ctx.iter_live_orders(symbol=row.symbol):
            extras = leg.extras or {}
            if (str(extras.get('parent_deal_id') or '') != deal_id
                    or leg.state == STATE_REJECTED
                    or extras.get('natural_close_at') is not None):
                continue
            leg_kind = extras.get(EXTRAS_KEY_LEG_KIND)
            if leg_kind == 'tp' and tp_leg is None:
                tp_leg = leg
            elif leg_kind == 'sl' and sl_leg is None:
                sl_leg = leg
        if tp_leg is None and sl_leg is None:
            return None
        limit = _wire_float(tp_leg.tp_level, finite=True) if tp_leg is not None else None
        if sl_leg is not None and sl_leg.trailing_stop:
            return ProtectionLevels(
                limit=limit, trailing=True,
                trail_distance=_wire_float(sl_leg.trailing_distance, finite=True),
            )
        stop = _wire_float(sl_leg.sl_level, finite=True) if sl_leg is not None else None
        return ProtectionLevels(stop=stop, limit=limit)

    @staticmethod
    def _left_bare(extras: dict, key: str) -> bool:
        """Whether every level set the guard accepts for ``key`` is "no protection".

        Read from the levels the guard persisted on the position row; a row an
        operator resume re-baselines is not bare.

        :param extras: The position row's extras.
        :param key: Its protection key.
        """
        accepted = _InterventionMixin._accepted_levels(extras, key)
        return bool(accepted) and all(levels == ProtectionLevels() for levels in accepted)

    async def _venue_protection_is_own(self, deal_id: str, pos: dict) -> bool:
        """Whether the venue carries protection the bot itself set on ``deal_id``.

        Compared against every level set the guard currently accepts for the
        position (persisted on its row), within half a price tick. A
        position the guard holds no levels for yet, or one an operator
        resume re-baselines, has nothing to compare and counts as own.

        :param deal_id: The bot position.
        :param pos: Its ``GET /positions`` row.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return True
        row = store_ctx.find_by_ref('deal_id', deal_id)
        if row is None:
            return True
        accepted = self._accepted_levels(row.extras or {}, _protection_key(deal_id))
        if not accepted:
            return True
        venue = self._levels_from_position(pos)
        tick = await self._price_tick(row.symbol)
        return any(levels.matches(venue, _level_tolerance(tick, venue)) for levels in accepted)

    def _stamp_protection_adopt(self, coid: str, after: float) -> None:
        """Make the next snapshot read started after ``after`` the accepted protection."""
        self._rewrite_extras(
            coid,
            lambda extras: extras.__setitem__(EXTRAS_KEY_PROTECTION_ADOPT_AFTER, after),
        )

    def _rewrite_extras(self, coid: str, mutate: Callable[[dict], Any]) -> None:
        """Apply ``mutate`` to the CURRENT extras of a live row and store them."""
        store_ctx = self.store_ctx
        if store_ctx is None:
            return
        with store_ctx.transaction():
            row = store_ctx.get_order(coid)
            if row is None or row.closed_ts_ms is not None:
                return
            extras = dict(row.extras or {})
            mutate(extras)
            store_ctx.upsert_order(coid, extras=extras)

    # --- Own closes ----------------------------------------------------------

    @contextmanager
    def _own_close(
            self, deal_ids: Iterable[str], *, full: bool, size: float | None,
    ) -> Iterator[_OwnCloseHandle]:
        """Record an own close of ``deal_ids`` around the request that sends it.

        The record is persisted before the request leaves, so the close
        activity — which the venue stamps ``source=USER`` like a manual
        close — is attributed to the bot even if it is read while the request
        is still in flight, after a crash, or long after the command finished.
        A request the venue refused (or that never left) withdraws the
        record; an ambiguous outcome keeps it.

        :param deal_ids: The positions the request closes or reduces.
        :param full: The whole position is closed (any activity size matches).
        :param size: The reduced size of a partial close.
        :return: A handle whose :meth:`~._OwnCloseHandle.failed` withdraws the
            record when the refusal only shows after the request returned.
        """
        store_ctx = self.store_ctx
        coids: list[str] = []
        if store_ctx is not None:
            for deal_id in deal_ids:
                row = store_ctx.find_by_ref('deal_id', deal_id) if deal_id else None
                if row is not None and row.closed_ts_ms is None:
                    coids.append(row.client_order_id)
        sent_at = epoch_time()
        self._note_own_close_send(sent_at)
        record = {'full': full, 'size': size, 'sent_at': sent_at,
                  'done_at': None, 'matched': []}

        def add(extras: dict) -> None:
            kept = [r for r in extras.get(EXTRAS_KEY_OWN_CLOSES) or ()
                    if isinstance(r, dict)
                    and (_wire_float(r.get('sent_at'), finite=True) or 0.0)
                    > sent_at - _OWN_CLOSE_RETAIN_S]
            extras[EXTRAS_KEY_OWN_CLOSES] = [*kept, dict(record)]

        def withdraw() -> None:
            def drop(extras: dict) -> None:
                extras[EXTRAS_KEY_OWN_CLOSES] = [
                    r for r in extras.get(EXTRAS_KEY_OWN_CLOSES) or ()
                    if isinstance(r, dict) and r.get('sent_at') != sent_at
                ]

            for target in coids:
                self._rewrite_extras(target, drop)

        for coid in coids:
            self._rewrite_extras(coid, add)
        handle = _OwnCloseHandle(withdraw=withdraw, sent_at=sent_at)
        refused = False
        try:
            yield handle
        except _VENUE_REFUSALS:
            # Refused by the venue, or never sent: no deal can follow.
            refused = True
            raise
        finally:
            if refused:
                withdraw()
            else:
                done_at = epoch_time()

                def finish(extras: dict) -> None:
                    for r in extras.get(EXTRAS_KEY_OWN_CLOSES) or ():
                        if isinstance(r, dict) and r.get('sent_at') == sent_at:
                            r['done_at'] = done_at
                    if not full:
                        extras[EXTRAS_KEY_PROTECTION_ADOPT_AFTER] = done_at

                for coid in coids:
                    self._rewrite_extras(coid, finish)

    @staticmethod
    def _own_close_in_flight(extras: dict, now: float) -> bool:
        """Whether an own close of this position may still be landing."""
        for record in extras.get(EXTRAS_KEY_OWN_CLOSES) or ():
            if not isinstance(record, dict) or record.get('done_at') is not None:
                continue
            sent_at = _wire_float(record.get('sent_at'), finite=True)
            if sent_at is not None and now - sent_at < _UNSETTLED_WRITE_S:
                return True
        return False

    # --- Venue clock ---------------------------------------------------------

    def _note_venue_clock(self, activities: Iterable[dict], read_at: float) -> None:
        """Sample the venue clock against the local clock from one activity read.

        An activity's ``dateUTC`` is the venue time of its deal, which cannot
        be later than the venue's clock when the read returned. The newest
        ``dateUTC`` of a read minus the local time right after the read is
        therefore never above the true offset (venue minus local) in the
        wall-clock frame of that moment. The monotonic clock is sampled with
        it, so a later wall-clock step or sleep shows as a disagreement
        between the two (:meth:`_own_close_earliest`).

        :param activities: The rows of one ``/history/activity`` read.
        :param read_at: Local wall-clock time right after the read returned.
        """
        read_mono = monotonic()
        newest = max(
            (_parse_iso_timestamp(str(a.get('dateUTC') or '')) for a in activities),
            default=0.0,
        )
        samples = self._venue_clock_samples
        if newest > 0.0:
            samples.append((read_at, read_mono, newest - read_at))
        while samples and read_mono - samples[0][1] > _VENUE_CLOCK_SAMPLE_WINDOW_S:
            samples.popleft()

    def _note_own_close_send(self, sent_at: float) -> None:
        """Pair an own close request's wall-clock send time with the monotonic clock."""
        now = monotonic()
        sends = self._own_close_sends
        for stale in [key for key, mono in sends.items() if now - mono > _OWN_CLOSE_RETAIN_S]:
            del sends[stale]
        sends[sent_at] = now

    def _own_close_earliest(self, sent_at: float) -> float:
        """The earliest venue time an own close request sent at ``sent_at`` executes at.

        An own request sent at local time ``t`` executes at venue time
        ``t + offset`` or later. The floor of the offset is the largest clock
        sample less :data:`_VENUE_CLOCK_MARGIN_S`, never above zero, so a
        venue clock running ahead never narrows the window past the local
        send time. A sample only bounds the offset in the wall-clock frame it
        was taken in: it counts for a request this process sent when the wall
        clock and the monotonic clock advanced alike between the two, within
        :data:`_CLOCK_FRAME_TOLERANCE_S`. A request without such a sample (a
        wall-clock step or a sleep in between, or a request an earlier process
        sent) gets the generous :data:`_CLOCK_SKEW_S`.

        :param sent_at: The request's local wall-clock send time.
        """
        sent_mono = self._own_close_sends.get(sent_at)
        if sent_mono is None:
            return sent_at - _CLOCK_SKEW_S
        floor: float | None = None
        for read_at, read_mono, sample in self._venue_clock_samples:
            if abs((sent_at - read_at) - (sent_mono - read_mono)) > _CLOCK_FRAME_TOLERANCE_S:
                continue
            floor = sample if floor is None else max(floor, sample)
        if floor is None:
            return sent_at - _CLOCK_SKEW_S
        return sent_at + min(0.0, floor - _VENUE_CLOCK_MARGIN_S)

    # --- Close attribution -----------------------------------------------------

    def _claim_own_close(self, row: 'OrderRow', activity: dict) -> bool:
        """Attribute a close activity to a recorded own close, if one fits.

        A record fits when the activity's venue time falls between the
        request's send and its answer and, for a partial close, the activity
        size matches within half a lot step. The early end of the window is
        the send time shifted by the measured venue-clock floor
        (:meth:`_own_close_earliest`): a close dated before it happened before
        the request left, so it is someone else's even when a full-close
        record covers the deal. The late end tolerates :data:`_CLOCK_SKEW_S`.
        A partial record is consumed by the first activity it explains; a
        replay of that same activity (same ``dateUTC``) still matches it. A
        full close may produce more than one activity row, so its record
        keeps matching inside the window.
        """
        record = self._matching_own_close(row, activity)
        if record is None:
            return False
        date_utc = str(activity.get('dateUTC') or '')
        if date_utc not in [str(d) for d in record.get('matched') or ()]:
            record_sent_at = _wire_float(record.get('sent_at'), finite=True)

            def mark(extras: dict) -> None:
                for r in extras.get(EXTRAS_KEY_OWN_CLOSES) or ():
                    if (isinstance(r, dict)
                            and _wire_float(r.get('sent_at'), finite=True) == record_sent_at):
                        r['matched'] = [*(r.get('matched') or ()), date_utc]

            self._rewrite_extras(row.client_order_id, mark)
        return True

    def _own_close_cursor_retired(self, row: 'OrderRow', activity: dict) -> bool:
        """Whether ``activity`` is an own partial close the run's exposure cursor already left.

        The cursor drops when the partial close returns from its dispatch, or
        when a parked one settles; the own-close record is flagged then
        (:data:`~pynecore_capitalcom.helpers.OWN_CLOSE_CURSOR_RETIRED`).
        """
        record = self._matching_own_close(row, activity)
        return record is not None and bool(record.get(OWN_CLOSE_CURSOR_RETIRED))

    def _matching_own_close(self, row: 'OrderRow', activity: dict) -> dict | None:
        """The own-close record on ``row`` that explains the close ``activity``, if one fits.

        The fit is the one :meth:`_claim_own_close` documents; nothing is
        marked.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return None
        fresh = store_ctx.get_order(row.client_order_id) or row
        records = [r for r in (fresh.extras or {}).get(EXTRAS_KEY_OWN_CLOSES) or ()
                   if isinstance(r, dict)]
        if not records:
            return None
        date_utc = str(activity.get('dateUTC') or '')
        activity_ts = _parse_iso_timestamp(date_utc)
        details = activity.get('details') or {}
        size = _wire_float(activity.get('size') or details.get('size'), finite=True)
        rules = self._instrument_rules_cache.get(row.symbol)
        size_tolerance = max(
            0.5 * rules.lot_step if rules is not None else 0.0,
            1e-9 * max(1.0, abs(size or 0.0)),
        )
        for record in records:
            sent_at = _wire_float(record.get('sent_at'), finite=True)
            if sent_at is None:
                continue
            done_at = _wire_float(record.get('done_at'), finite=True)
            if activity_ts > 0.0:
                latest = done_at if done_at is not None else sent_at + _UNSETTLED_WRITE_S
                earliest = self._own_close_earliest(sent_at)
                if not (earliest <= activity_ts <= latest + _CLOCK_SKEW_S):
                    continue
            matched = [str(d) for d in record.get('matched') or ()]
            if not record.get('full'):
                if matched and date_utc not in matched:
                    continue
                expected = _wire_float(record.get('size'), finite=True)
                if (size is not None and expected is not None
                        and abs(size - expected) > size_tolerance):
                    continue
            return record
        return None

    def _unconfirmed_reverse_leg(
            self, positions: Iterable[dict], *, coid: str, intent_key: str | None, symbol: str,
            direction: str, target_deal_id: str, size: float, lot_step: float,
            sent_at: float, done_at: float | None,
    ) -> str | None:
        """The deal an own partial close's reduce POST opened, when no confirm names it.

        The venue nets the POST FIFO against the target's side and opens a
        fresh deal of the POST's whole size only when nothing is left there to
        net: the target was gone (a TP / SL fill, a stop-out) by the time the
        POST executed. Such a deal is the POST's own reverse leg, judged like
        an own close (:meth:`_matching_own_close`): created inside the POST's
        window and of exactly its size, while the target is no longer listed.
        Only a deal this run did not open, that no other Pyne run owns and no
        unconfirmed own entry may have opened is considered, and only one that
        fits alone is attributed (audited as
        ``partial_close_reverse_leg_attributed``). Anything else stays
        unattributed and is never touched.

        :param positions: ``GET /positions`` rows (account-wide).
        :param coid: The partial-close command row (audit).
        :param intent_key: The close intent (audit).
        :param symbol: The epic.
        :param direction: The POST's direction (``'BUY'`` / ``'SELL'``).
        :param target_deal_id: The deal the POST was to reduce.
        :param size: The POST's size, as sent.
        :param lot_step: The instrument's lot step.
        :param sent_at: When the POST left (local wall clock).
        :param done_at: When its answer arrived; ``None`` while unknown.
        :return: The reverse leg's ``dealId``, or ``None``.
        """
        rows = [raw for raw in positions if (raw.get('market') or {}).get('epic') == symbol]
        if any(_wire_id((raw.get('position') or {}).get('dealId')) == target_deal_id
               for raw in rows):
            return None
        earliest = self._own_close_earliest(sent_at)
        latest = (done_at if done_at is not None else sent_at + _UNSETTLED_WRITE_S) + _CLOCK_SKEW_S
        tolerance = max(0.5 * lot_step, 1e-9 * max(1.0, abs(size)))
        candidates = [
            deal for deal in self._foreign_deals(rows, symbol, direction=direction)
            if not deal.get('ownership_pending')
            and deal.get('size') is not None and abs(deal['size'] - size) <= tolerance
            and earliest <= _parse_iso_timestamp(str(deal.get('created') or '')) <= latest
        ]
        other_runs = set(self._other_run_deal_ids(
            symbol, [str(deal['deal_id']) for deal in candidates]))
        candidates = [deal for deal in candidates if deal['deal_id'] not in other_runs]
        if len(candidates) != 1:
            return None
        deal = candidates[0]
        reverse_id = str(deal['deal_id'])
        logger.warning(
            "own partial close %s on %s: target deal %s was gone when its reduce POST "
            "executed, and deal %s (%s %s, created %s) is the POST's own reverse leg "
            "(no confirm names it); it is closed again",
            coid, symbol, target_deal_id, reverse_id, deal['direction'], deal['size'],
            deal['created'],
        )
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'partial_close_reverse_leg_attributed',
                client_order_id=coid,
                exchange_order_id=reverse_id,
                intent_key=intent_key,
                payload={'target_deal_id': target_deal_id, 'size': size,
                         'sent_at': sent_at, 'done_at': done_at, 'symbol': symbol,
                         'deal': deal},
            )
        return reverse_id

    # --- Own cancels -----------------------------------------------------------

    @contextmanager
    def _own_cancel(self, coid: str) -> Iterator[float]:
        """Record an own ``DELETE /workingorders/{dealId}`` of ``coid`` around the request.

        The venue stamps the bot's own cancel ``source=USER`` like an
        operator's. The record, persisted on the order's row before the
        request leaves, tells them apart after a restart, when the engine
        holds no memory of the cancel (:meth:`_own_cancel_explains`). A
        request the venue refused, or that never left, withdraws it: the
        order kept resting, and a later cancel is not the bot's.

        :param coid: The working-order row the DELETE targets.
        :return: The request's send time (wall clock).
        """
        sent_at = epoch_time()

        def add(extras: dict) -> None:
            kept = [t for t in extras.get(EXTRAS_KEY_OWN_CANCEL_REQUESTS) or ()
                    if (_wire_float(t, finite=True) or 0.0) > sent_at - _OWN_CLOSE_RETAIN_S]
            extras[EXTRAS_KEY_OWN_CANCEL_REQUESTS] = [*kept, sent_at]

        def withdraw(extras: dict) -> None:
            extras[EXTRAS_KEY_OWN_CANCEL_REQUESTS] = [
                t for t in extras.get(EXTRAS_KEY_OWN_CANCEL_REQUESTS) or () if t != sent_at
            ]

        self._rewrite_extras(coid, add)
        try:
            yield sent_at
        except _VENUE_REFUSALS:
            self._rewrite_extras(coid, withdraw)
            raise

    @staticmethod
    def _venue_end_of_working_order(activity: dict) -> str | None:
        """Who ended a resting order, when its terminal ``activity`` shows no client did.

        Who cancelled it is read from the source (:func:`_activity_actor`).

        :return: ``'dealer'`` for a dealing-desk cancel; ``'venue'`` for an
            expiry, a rejection, or a cancel the venue itself made; ``None``
            for a client (``USER``) cancel or an execution.
        """
        status = str(activity.get('status') or '').upper()
        if status == 'EXECUTED' or str(activity.get('type') or '').upper() == 'POSITION':
            return None
        if status in ('EXPIRED', 'REJECTED'):
            return 'venue'
        actor = _activity_actor(activity.get('source'))
        if actor == ACTOR_DEALER:
            return 'dealer'
        if actor == ACTOR_CLIENT:
            return None
        return 'venue'

    def _note_working_order_end(self, row: 'OrderRow', activity: dict) -> bool:
        """Record who cancelled the resting bot order ``row``; whether the venue itself did.

        The cancel is recorded on the row, so the disappearance verdict needs
        not wait for the activity (``_confirm_missing_cancelled``). A venue
        cancel (``SYSTEM`` or another venue source), an expiry and a
        dealing-desk cancel are not someone else's intervention: logged (a
        WARNING for the dealing desk) and audited as
        ``working_order_venue_cancel``, and the verdict retires the order as
        the venue's instead of applying the unexpected-cancel policy. A
        ``USER`` cancel is left to that policy: the engine knows its own
        cancels.

        :param row: The own working-order row.
        :param activity: The order's cancel or expiry activity.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return False
        end = self._venue_end_of_working_order(activity)
        record = {'by': end or 'client', 'status': activity.get('status'),
                  'source': activity.get('source'), 'dateUTC': activity.get('dateUTC')}
        self._rewrite_extras(
            row.client_order_id,
            lambda extras: extras.__setitem__(EXTRAS_KEY_WORKING_ORDER_END, record),
        )
        if end is None:
            return False
        message = "bot working order %s (%s, %s %s) ended by %s: %s %s at %s; not an intervention"
        args = (row.exchange_order_id, row.symbol, row.side, row.qty,
                'the dealing desk' if end == 'dealer' else 'the venue',
                record['source'], record['status'], record['dateUTC'])
        if end == 'dealer':
            logger.warning(message, *args)
        else:
            logger.info(message, *args)
        store_ctx.log_event(
            'working_order_venue_cancel',
            client_order_id=row.client_order_id,
            exchange_order_id=row.exchange_order_id,
            payload=record,
        )
        return True

    @staticmethod
    def _own_cancel_explains(row: 'OrderRow', activity_ts: float) -> bool:
        """Whether a cancel of the resting own order ``row`` is the bot's own.

        The venue accepted an own DELETE of the order (it was still resting
        then, and an order is cancelled once), or an own DELETE was sent
        within the window a cancel it caused is dated in:
        :data:`_CLOCK_SKEW_S` either side of the send, plus
        :data:`_UNSETTLED_WRITE_S` for a request whose answer never came.
        A cancel of unknown time (``activity_ts`` ``0``) is explained by any
        own DELETE.

        :param row: The own working-order row.
        :param activity_ts: The cancel's venue time (epoch seconds), ``0`` when unknown.
        """
        extras = row.extras or {}
        if extras.get(EXTRAS_KEY_OWN_CANCEL_SENT_AT) is not None:
            return True
        for value in extras.get(EXTRAS_KEY_OWN_CANCEL_REQUESTS) or ():
            sent_at = _wire_float(value, finite=True)
            if sent_at is None:
                continue
            if (activity_ts <= 0.0
                    or sent_at - _CLOCK_SKEW_S <= activity_ts
                    <= sent_at + _UNSETTLED_WRITE_S + _CLOCK_SKEW_S):
                return True
        return False

    def _own_netting_candidates(
            self, row: 'OrderRow', activity_ts: float, size: float | None,
            positions_by_deal: dict[str, dict] | None,
    ) -> tuple[list[str], list[str]]:
        """The own opposite-direction orders that may have netted ``row`` down.

        On a netting account an opposite entry — a resting working order
        that executed, a ``strategy.order``, a stop-fired market — reduces
        the existing position under the existing ``dealId``, and the venue
        logs that reduction like a close. Such an entry row exists before the
        reduction, is filled no earlier than it, and reduces the position by
        at most what it did not open as a deal of its own.

        Only rows that could still have netted count. A row whose own deal
        closed (``natural_close_at``), is being closed (``closing``) or never
        landed (``rejected``) explains nothing; a row whose deal holds its
        whole size opened that deal instead of netting. An own position entry
        dispatched before the reduction, or a working order whose execution
        was seen, is own execution evidence. A working order with no seen
        execution is not: it only makes the reduction ambiguous.

        :param row: The bot position the close activity reduced.
        :param activity_ts: The activity's venue time (epoch seconds).
        :param size: The reduced size, ``None`` when the activity omits it.
        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        :return: The client order ids of the executed candidates and of the
            resting working-order candidates.
        """
        executed: list[str] = []
        resting: list[str] = []
        store_ctx = self.store_ctx
        if store_ctx is None or self._hedging_enabled:
            return executed, resting
        rules = self._instrument_rules_cache.get(row.symbol)
        size_tolerance = max(
            0.5 * rules.lot_step if rules is not None else 0.0,
            1e-9 * max(1.0, abs(size or 0.0)),
        )
        for other in store_ctx.iter_live_orders(symbol=row.symbol):
            if other.client_order_id == row.client_order_id or other.side == row.side:
                continue
            extras = other.extras or {}
            if (extras.get(EXTRAS_KEY_LEG_KIND) is not None
                    or extras.get('natural_close_at') is not None
                    or other.state in ('closing', STATE_REJECTED)):
                continue
            kind = extras.get('kind')
            if kind not in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING):
                continue
            capacity = max(other.qty, other.filled_qty)
            own_deal = (positions_by_deal or {}).get(other.exchange_order_id or '')
            if kind == ENTRY_KIND_POSITION and own_deal is not None:
                held = _wire_float((own_deal.get('position') or {}).get('size'), finite=True)
                capacity -= held or 0.0
            if capacity <= size_tolerance:
                continue
            if size is not None and capacity < size - size_tolerance:
                continue
            filled_at = _wire_float(extras.get('entry_filled_at'), finite=True)
            if kind == ENTRY_KIND_WORKING and filled_at is None:
                resting.append(other.client_order_id)
                continue
            if other.created_ts_ms / 1000.0 > activity_ts + _CLOCK_SKEW_S:
                continue
            if filled_at is None or filled_at >= activity_ts - _CLOCK_SKEW_S:
                executed.append(other.client_order_id)
        return executed, resting

    def _attribute_close_activity(
            self, row: 'OrderRow', activity: dict,
            positions_by_deal: dict[str, dict] | None = None,
    ) -> str:
        """Judge who closed (part of) a bot position, once per close.

        Who closed it is read from the source (:func:`_activity_actor`). Own
        closes and closes an own netting order explains are logged. A
        dealing-desk close is booked with a loud warning, never reported. The
        venue's own closes are its mechanics and are not judged. The
        verdict is persisted on the row under the close's level-independent
        identity (:func:`~pynecore_capitalcom.models._close_identity`); a
        re-issued activity of the same close gets the stored verdict back
        without being judged, logged or reported again.

        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        :return: One of the ``CLOSE_VERDICT_*`` values; ``CLOSE_VERDICT_FOREIGN``
            is someone else's close, booked and then reported.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return CLOSE_VERDICT_UNJUDGED
        source = str(activity.get('source') or '').upper()
        actor = _activity_actor(source)
        if actor not in (ACTOR_CLIENT, ACTOR_DEALER):
            return CLOSE_VERDICT_VENUE
        close_id = _close_identity(activity)
        fresh = store_ctx.get_order(row.client_order_id) or row
        stored = ((fresh.extras or {}).get(EXTRAS_KEY_CLOSE_VERDICTS) or {}).get(close_id)
        if isinstance(stored, str):
            return stored
        verdict = self._judge_close_activity(row, activity, source, positions_by_deal)

        def keep(extras: dict) -> None:
            verdicts = dict(extras.get(EXTRAS_KEY_CLOSE_VERDICTS) or {})
            verdicts[close_id] = verdict
            extras[EXTRAS_KEY_CLOSE_VERDICTS] = dict(
                list(verdicts.items())[-_CLOSE_VERDICTS_RETAINED:])

        self._rewrite_extras(row.client_order_id, keep)
        return verdict

    def _judge_close_activity(
            self, row: 'OrderRow', activity: dict, source: str,
            positions_by_deal: dict[str, dict] | None,
    ) -> str:
        """The verdict of :meth:`_attribute_close_activity` for a close seen first."""
        store_ctx = self.store_ctx
        assert store_ctx is not None
        deal_id = row.exchange_order_id or _wire_id(activity.get('dealId'))
        payload = {
            'dateUTC': activity.get('dateUTC'),
            'source': source,
            'size': (activity.get('details') or {}).get('size', activity.get('size')),
        }
        if self._claim_own_close(row, activity):
            store_ctx.log_event(
                'own_close_activity_attributed',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload=payload,
            )
            return CLOSE_VERDICT_OWN
        if _activity_actor(source) == ACTOR_DEALER:
            logger.warning(
                "Capital.com dealing desk closed bot position %s (%s) at %s; "
                "booked, not treated as an operator intervention",
                deal_id, row.symbol, activity.get('dateUTC'),
            )
            store_ctx.log_event(
                'dealer_close_booked',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload=payload,
            )
            return CLOSE_VERDICT_DEALER
        activity_ts = _parse_iso_timestamp(str(activity.get('dateUTC') or ''))
        size = _wire_float(
            activity.get('size') or (activity.get('details') or {}).get('size'),
            finite=True,
        )
        executed, resting = self._own_netting_candidates(
            row, activity_ts or epoch_time(), size, positions_by_deal,
        )
        if executed:
            logger.warning(
                "close of bot position %s (%s) at %s matches no own close, but "
                "own opposite order(s) %s netted it; not reported",
                deal_id, row.symbol, activity.get('dateUTC'), executed,
            )
            store_ctx.log_event(
                'user_close_attributed_to_own_netting',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload={**payload, 'candidates': executed},
            )
            return CLOSE_VERDICT_OWN_NETTING
        if resting:
            # Only a resting own working order could explain the reduction:
            # it executed, or someone else closed the position while it
            # still rests. A ``/workingorders`` read made well after the
            # close tells them apart (:meth:`_resolve_netting_pending`).
            pending = {
                'close_id': _close_identity(activity),
                'client_order_id': row.client_order_id,
                'deal_id': deal_id,
                'activity_ts': activity_ts,
                'judged_at': epoch_time(),
                'size': size,
                'candidates': resting,
                'payload': payload,
                'report': self._manual_close_report(row, activity),
            }
            logger.warning(
                "close of bot position %s (%s) at %s matches no own close and no "
                "own fill; resting own working order(s) %s could have netted it — "
                "judged once a later working-order read shows whether they executed",
                deal_id, row.symbol, activity.get('dateUTC'), resting,
            )
            store_ctx.log_event(
                'user_close_netting_pending',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload=pending,
            )
            self._netting_pending[pending['close_id']] = (epoch_time(), pending)
            return CLOSE_VERDICT_NETTING_PENDING
        # The close becomes durable before it is booked, so the report it
        # owes is persisted with it: a process that dies before reporting
        # sends it on the next start (:meth:`_send_pending_manual_close_reports`).
        store_ctx.log_event(
            'manual_close_report_pending',
            client_order_id=row.client_order_id,
            exchange_order_id=deal_id,
            payload=self._manual_close_report(row, activity),
        )
        return CLOSE_VERDICT_FOREIGN

    def _foreign_consumption_report(
            self, row: 'OrderRow', activities: Iterable[dict], *,
            earliest: float, latest: float,
    ) -> dict | None:
        """The report of someone else's order that netted a vanished bot position away.

        On a netting account an opposite order nets FIFO against the oldest
        deals of the other side, and the venue logs a deal it consumes whole
        on the consuming deal only: the bot's deal just disappears, with no
        close activity of its own. A client (``USER``) ``POSITION`` activity
        on the epic, in the direction that reduces the bot's position, at
        least as large as the bot's exposure, dated in the window the deal
        vanished in, and on a deal this run did not open, is that consumer:
        someone else's order closed the bot's position. Nothing is reported
        (``None``) when the evidence could be the bot's own: an own close
        whose window covers the activity, an own entry still waiting for its
        ``dealId`` in that direction, or an own opposite order that could
        have netted the position.

        :param row: The vanished bot position row.
        :param activities: Activity rows to search: the run's
            ``external_activity_ignored`` audit, and the startup read.
        :param earliest: Venue time the deal was last known to exist.
        :param latest: Venue time it was known gone.
        :return: A manual-close report (:meth:`_send_manual_close_report`),
            or ``None``.
        """
        store_ctx = self.store_ctx
        deal_id = row.exchange_order_id
        if store_ctx is None or self._hedging_enabled or not deal_id:
            return None
        reducing = 'SELL' if row.side == 'buy' else 'BUY'
        rules = self._instrument_rules_cache.get(row.symbol)
        tolerance = max(0.5 * rules.lot_step if rules is not None else 0.0,
                        1e-9 * max(1.0, row.filled_qty))
        candidates: list[tuple[float, dict]] = []
        for activity in activities:
            details = activity.get('details') or {}
            consumer = _wire_id(activity.get('dealId'))
            activity_ts = _parse_iso_timestamp(str(activity.get('dateUTC') or ''))
            size = _wire_float(activity.get('size') or details.get('size'), finite=True)
            working_order = _wire_id(details.get('workingOrderId'))
            if (str(activity.get('type') or '').upper() != 'POSITION'
                    or str(activity.get('status') or '').upper() not in ('ACCEPTED', 'EXECUTED')
                    or _activity_actor(activity.get('source')) != ACTOR_CLIENT
                    or activity.get('epic') != row.symbol
                    or str(details.get('direction') or '').upper() != reducing
                    or consumer is None or consumer == deal_id
                    or not earliest <= activity_ts <= latest
                    or (size is not None and size < row.filled_qty - tolerance)
                    or store_ctx.find_by_ref('deal_id', consumer) is not None
                    or (working_order is not None
                        and store_ctx.find_by_ref('deal_id', working_order) is not None)):
                continue
            candidates.append((activity_ts, activity))
        if not candidates:
            return None
        activity_ts, activity = min(candidates, key=lambda candidate: candidate[0])
        own_explanation = self._own_explanation_of_consumption(row, activity_ts, reducing)
        if own_explanation is not None:
            logger.error(
                "bot position %s (%s) vanished while opposite order %s (USER, %s) was "
                "dated %s, but %s could have netted it too: not reported — check the "
                "venue history",
                deal_id, row.symbol, activity.get('dealId'), reducing,
                activity.get('dateUTC'), own_explanation,
            )
            store_ctx.log_event(
                'position_netted_away_ambiguous',
                client_order_id=row.client_order_id,
                exchange_order_id=deal_id,
                payload={'consumer': {k: activity.get(k) for k in (
                    'dateUTC', 'dealId', 'type', 'status', 'source')},
                    'own_explanation': own_explanation},
            )
            return None
        details = activity.get('details') or {}
        size = details.get('size', activity.get('size'))
        consumer = activity.get('dealId')
        return {
            'report_id': f"netted:{deal_id}:{_close_identity(activity)}",
            'client_order_id': row.client_order_id,
            'deal_id': deal_id,
            'changed_at_ms': int(activity_ts * 1000.0) if activity_ts > 0.0 else None,
            'reason': (
                f"bot position {deal_id} ({row.symbol}) was closed on the venue by "
                f"someone else: USER {reducing} order {consumer} of {size} at "
                f"{activity.get('dateUTC')} netted it away and matches no own order"
            ),
            'context': {
                'deal_id': deal_id,
                'client_order_id': row.client_order_id,
                'exposure': row.filled_qty,
                'consumer': {k: activity.get(k) for k in (
                    'dateUTC', 'dealId', 'type', 'status', 'source')},
                'size': size,
                'direction': reducing,
            },
        }

    def _own_explanation_of_consumption(
            self, row: 'OrderRow', activity_ts: float, direction: str,
    ) -> str | None:
        """What of the bot's own could have netted ``row``'s deal away at ``activity_ts``.

        :param row: The vanished bot position row.
        :param activity_ts: Venue time of the opposite order.
        :param direction: The direction that reduces the position.
        :return: A description for the log, or ``None`` when nothing own could.
        """
        store_ctx = self.store_ctx
        assert store_ctx is not None
        for other in store_ctx.iter_live_orders(symbol=row.symbol):
            for record in (other.extras or {}).get(EXTRAS_KEY_OWN_CLOSES) or ():
                if not isinstance(record, dict):
                    continue
                sent_at = _wire_float(record.get('sent_at'), finite=True)
                if sent_at is None:
                    continue
                done_at = _wire_float(record.get('done_at'), finite=True)
                latest = (done_at if done_at is not None
                          else sent_at + _UNSETTLED_WRITE_S) + _CLOCK_SKEW_S
                if self._own_close_earliest(sent_at) <= activity_ts <= latest:
                    return f"own close of {other.exchange_order_id or other.client_order_id}"
        for entry_direction, sent_at in self._unconfirmed_own_entries(row.symbol):
            if entry_direction == direction and abs(activity_ts - sent_at) <= _CLOCK_SKEW_S:
                return "an own entry still waiting for its dealId"
        executed, resting = self._own_netting_candidates(row, activity_ts, None, None)
        if executed or resting:
            return f"own opposite order(s) {[*executed, *resting]}"
        return None

    def _resolve_netting_pending(
            self, working_by_deal: dict[str, dict], read_started_at: float | None,
    ) -> None:
        """Judge the closes only a resting own working order could explain.

        A ``/workingorders`` read that started, in venue time, at least
        :data:`_NETTING_PROOF_SETTLE_S` after such a close shows whether the
        order executed: one still resting at its full size did not net
        anything at the close. An order whose own ``DELETE`` the venue
        accepted after the close (:meth:`_own_cancel_after_close`) was still
        resting then too, whenever the read comes. When every candidate is
        shown resting so, the close is someone else's
        (``CLOSE_VERDICT_FOREIGN``: the run's exposure cursor drops by it,
        then it is reported). An order gone or reduced otherwise may have
        netted it (``CLOSE_VERDICT_AMBIGUOUS``: ERROR and a
        ``user_close_ambiguous_netting`` audit event, never reported). Without
        such evidence within :data:`_NETTING_PENDING_MAX_S` the close stays
        ambiguous.

        :param working_by_deal: The poll's ``GET /workingorders`` rows by ``dealId``.
        :param read_started_at: Wall-clock time the poll's reads started.
        """
        store_ctx = self.store_ctx
        if store_ctx is None or not self._netting_pending:
            return
        now = epoch_time()
        proof_floor = (self._venue_time_floor(read_started_at)
                       if read_started_at is not None else None)
        for close_id, (since, pending) in list(self._netting_pending.items()):
            activity_ts = _wire_float(pending.get('activity_ts'), finite=True) or 0.0
            judged_at = _wire_float(pending.get('judged_at'), finite=True) or since
            candidates = [str(c) for c in pending.get('candidates') or ()]
            cancelled = {coid for coid in candidates
                         if self._own_cancel_after_close(coid, activity_ts, judged_at)}
            read_proof = (proof_floor is not None and activity_ts > 0.0
                          and proof_floor > activity_ts + _NETTING_PROOF_SETTLE_S)
            proven = read_proof or (bool(candidates) and cancelled == set(candidates))
            if not proven and now - since <= _NETTING_PENDING_MAX_S:
                continue
            if proven and all(coid in cancelled
                              or self._own_order_still_resting(coid, working_by_deal)
                              for coid in candidates):
                verdict = CLOSE_VERDICT_FOREIGN
            else:
                verdict = CLOSE_VERDICT_AMBIGUOUS
            del self._netting_pending[close_id]
            self._settle_netting_pending(pending, verdict, proven=proven)

    def _own_cancel_after_close(self, coid: str, activity_ts: float, judged_at: float) -> bool:
        """Whether the venue accepted the bot's own DELETE of ``coid`` after the close.

        The venue took the request while the order was still resting, so the
        order had not executed when the close happened before it. The DELETE
        counts when it was sent after the close was read and judged (the
        close was on the venue before the read), or, by the measured venue
        clock, after the close's venue time. An order with any fill on its
        row is never counted (like the rest of the plugin, a working order is
        taken to execute all-or-nothing).

        :param coid: The own working-order candidate.
        :param activity_ts: The close's venue time (epoch seconds), ``0`` when unknown.
        :param judged_at: Local time the close was judged.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return False
        row = store_ctx.get_order(coid)
        if row is None:
            return False
        extras = row.extras or {}
        if (extras.get('kind') != ENTRY_KIND_WORKING
                or extras.get('entry_filled_at') is not None
                or row.filled_qty > 0.0):
            return False
        sent_at = _wire_float(extras.get(EXTRAS_KEY_OWN_CANCEL_SENT_AT), finite=True)
        if sent_at is None:
            return False
        return (sent_at >= judged_at
                or (activity_ts > 0.0 and self._venue_time_floor(sent_at) > activity_ts))

    def _stamp_own_cancel_accepted(self, coid: str, sent_at: float) -> None:
        """Record that the venue accepted the bot's DELETE of the working order ``coid``.

        The order was still resting when the venue took the request: evidence
        :meth:`_own_cancel_after_close` reads.
        """
        if self.store_ctx is None:
            return
        self._rewrite_extras(
            coid, lambda extras: extras.__setitem__(EXTRAS_KEY_OWN_CANCEL_SENT_AT, sent_at),
        )

    def _own_order_still_resting(self, coid: str, working_by_deal: dict[str, dict]) -> bool:
        """Whether the own working order ``coid`` rests unexecuted at its full size."""
        store_ctx = self.store_ctx
        if store_ctx is None:
            return False
        row = store_ctx.get_order(coid)
        if (row is None or row.closed_ts_ms is not None or row.state == STATE_REJECTED
                or not row.exchange_order_id):
            return False
        extras = row.extras or {}
        if extras.get('kind') != ENTRY_KIND_WORKING or extras.get('entry_filled_at') is not None:
            return False
        order = working_by_deal.get(row.exchange_order_id)
        if order is None:
            return False
        size = _wire_float((order.get('workingOrderData') or {}).get('orderSize'), finite=True)
        rules = self._instrument_rules_cache.get(row.symbol)
        tolerance = max(0.5 * rules.lot_step if rules is not None else 0.0,
                        1e-9 * max(1.0, row.qty))
        return size is not None and size >= row.qty - tolerance

    def _settle_netting_pending(self, pending: dict, verdict: str, *, proven: bool) -> None:
        """Apply the verdict :meth:`_resolve_netting_pending` reached for one close."""
        store_ctx = self.store_ctx
        assert store_ctx is not None
        close_id = str(pending.get('close_id') or '')
        coid = str(pending.get('client_order_id') or '')
        deal_id = pending.get('deal_id')
        payload = dict(pending.get('payload') or {})
        store_ctx.log_event(
            'user_close_netting_resolved',
            client_order_id=coid or None,
            exchange_order_id=deal_id,
            payload={'close_id': close_id, 'verdict': verdict, 'proven': proven},
        )

        def keep(extras: dict) -> None:
            verdicts = dict(extras.get(EXTRAS_KEY_CLOSE_VERDICTS) or {})
            verdicts[close_id] = verdict
            extras[EXTRAS_KEY_CLOSE_VERDICTS] = dict(
                list(verdicts.items())[-_CLOSE_VERDICTS_RETAINED:])

        self._rewrite_extras(coid, keep)
        if verdict != CLOSE_VERDICT_FOREIGN:
            logger.error(
                "close of bot position %s at %s matches no own close and no own "
                "fill; resting own working order(s) %s may have netted it (%s) — "
                "possibly a manual close, not reported, check the venue history",
                deal_id, payload.get('dateUTC'), pending.get('candidates'),
                'an order left or shrank' if proven else 'no later order read decided',
            )
            store_ctx.log_event(
                'user_close_ambiguous_netting',
                client_order_id=coid or None,
                exchange_order_id=deal_id,
                payload={**payload, 'candidates': pending.get('candidates'),
                         'proven': proven},
            )
            return
        # The resting order(s) never executed: someone else closed (part of)
        # the bot position. Its exposure cursor drops by the closed size, as
        # it does when such a close is judged at once.
        size = _wire_float(pending.get('size'), finite=True)
        row = store_ctx.get_order(coid) if coid else None
        if (row is not None and row.closed_ts_ms is None and size is not None
                and (row.extras or {}).get('natural_close_at') is None
                and close_id in ((row.extras or {}).get(EXTRAS_KEY_BOOKED_CLOSES) or ())):
            remaining = max(0.0, row.filled_qty - size)

            def retire(extras: dict) -> None:
                extras['partial_close_retired_at'] = epoch_time()

            self._rewrite_extras(coid, retire)
            store_ctx.upsert_order(coid, filled_qty=remaining)
            store_ctx.log_event(
                'partial_close_journal_retired',
                client_order_id=coid,
                exchange_order_id=deal_id,
                payload={'retired_exposure': size, 'remaining_exposure': remaining,
                         'source': payload.get('source'), 'verdict': verdict},
            )
        report = dict(pending.get('report') or {})
        store_ctx.log_event(
            'manual_close_report_pending',
            client_order_id=coid or None,
            exchange_order_id=deal_id,
            payload=report,
        )
        self._send_manual_close_report(report)

    def _reload_netting_pending(self) -> None:
        """Pick up the closes an earlier process left waiting for a working-order read."""
        store_ctx = self.store_ctx
        if store_ctx is None or self._connection_generation > 0:
            return
        resolved = {
            payload.get('close_id')
            for _, _, _, payload in store_ctx.iter_events_by_kind_for_run_id(
                'user_close_netting_resolved')
        }
        now = epoch_time()
        for _, _, _, pending in store_ctx.iter_events_by_kind_for_run_id(
                'user_close_netting_pending'):
            close_id = pending.get('close_id')
            if not isinstance(close_id, str) or close_id in resolved:
                continue
            self._netting_pending.setdefault(close_id, (now, pending))

    @staticmethod
    def _manual_close_report(row: 'OrderRow', activity: dict) -> dict:
        """The report of a ``USER`` close no own write explains.

        ``changed_at_ms`` is the close's venue time, so a close an operator
        resume already accepted is not reported again
        (:meth:`_send_manual_close_report`).
        """
        deal_id = row.exchange_order_id or _wire_id(activity.get('dealId'))
        details = activity.get('details') or {}
        size = details.get('size', activity.get('size'))
        closed_at = _parse_iso_timestamp(str(activity.get('dateUTC') or ''))
        return {
            'report_id': _close_identity(activity),
            'client_order_id': row.client_order_id,
            'deal_id': deal_id,
            'changed_at_ms': int(closed_at * 1000.0) if closed_at > 0.0 else None,
            'reason': (
                f"bot position {deal_id} ({row.symbol}) was closed on the venue by "
                f"someone else: USER close of {size} at {activity.get('dateUTC')} "
                f"matches no own close"
            ),
            'context': {
                'deal_id': deal_id,
                'client_order_id': row.client_order_id,
                'activity': {k: activity.get(k) for k in (
                    'dateUTC', 'dealId', 'type', 'status', 'source')},
                'size': size,
                'direction': details.get('direction'),
            },
        }

    def _report_manual_close(self, row: 'OrderRow', activity: dict) -> None:
        """Report a ``USER`` close no own write explains."""
        self._send_manual_close_report(self._manual_close_report(row, activity))

    def _send_manual_close_report(self, report: dict) -> None:
        """Send one persisted manual-close report and mark it sent.

        A report sent already, by this process or an earlier one of the run
        (:meth:`_send_pending_manual_close_reports`), is not sent again: a
        re-issued activity of the same close, a booking deferred for its
        price, a close a startup pass judges again. The mark follows the
        policy's action, so a process that dies in between sends it again on
        restart (the quarantine latch is idempotent). A halting policy's
        exception counts as sent: the halt is the action, and repeating it
        would stop every restart. The close's venue time goes with the
        report: a close dated before the operator resume this process applies
        was accepted by that resume and applies no policy.
        """
        report_id = report.get('report_id')
        if isinstance(report_id, str) and report_id in self._manual_close_reports_sent:
            return
        reason = str(report.get('reason') or 'bot position was closed on the venue by someone else')
        changed_at_ms = report.get('changed_at_ms')
        if not isinstance(changed_at_ms, int):
            changed_at_ms = None
        resume_ts_ms = self.operator_resume_ts_ms
        if changed_at_ms is None or resume_ts_ms is None or changed_at_ms > resume_ts_ms:
            logger.error("%s", reason)
        try:
            self.report_external_intervention(
                reason,
                report.get('context') or {},
                client_order_id=report.get('client_order_id'),
                exchange_order_id=report.get('deal_id'),
                changed_at_ms=changed_at_ms,
            )
        except UnexpectedCancelError:
            self._mark_manual_close_report_sent(report)
            raise
        self._mark_manual_close_report_sent(report)

    def _mark_manual_close_report_sent(self, report: dict) -> None:
        """Persist that ``report`` reached the policy."""
        report_id = report.get('report_id')
        if isinstance(report_id, str):
            self._manual_close_reports_sent.add(report_id)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'manual_close_report_sent',
                client_order_id=report.get('client_order_id'),
                exchange_order_id=report.get('deal_id'),
                payload={'report_id': report_id},
            )

    def _send_pending_manual_close_reports(self) -> None:
        """Send the manual-close reports an earlier process booked but never sent.

        Reads every instance of the logical run: the reports were persisted
        by the process that died. Every report an earlier process sent is
        remembered as sent, so a close the startup pass judges again (one an
        earlier process reported before it booked it) is not reported twice.
        """
        store_ctx = self.store_ctx
        if store_ctx is None or self._connection_generation > 0:
            return
        sent = {
            payload.get('report_id')
            for _, _, _, payload in store_ctx.iter_events_by_kind_for_run_id(
                'manual_close_report_sent')
        }
        self._manual_close_reports_sent.update(
            report_id for report_id in sent if isinstance(report_id, str))
        for _, _, _, report in store_ctx.iter_events_by_kind_for_run_id(
                'manual_close_report_pending'):
            report_id = report.get('report_id')
            if report_id in sent:
                continue
            sent.add(report_id)
            self._send_manual_close_report(report)
