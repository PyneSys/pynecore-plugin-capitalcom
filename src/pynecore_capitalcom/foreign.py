"""Foreign exposure on the run's epic: deals this run did not open.

A Capital.com account can carry deals on the bot's own epic that the bot never
opened: a trade from the web platform, another API client, another Pyne run on
the same account. Such a deal is never the bot's, whatever its size or
direction:

* **Never read as own exposure.** ``get_position`` and ``fetch_raw_positions``
  count only the deals this run's journal produced
  (:meth:`_ForeignExposureMixin._owned_deal_ids`). A foreign deal can then
  neither mask the disappearance of an own one nor be closed, amended or
  bracketed by the one-way emulator. Each foreign deal is logged once (INFO
  plus a ``foreign_deal_on_epic`` audit event). A deal an own entry still
  waiting for its ``dealId`` may have opened is not provably foreign: it is
  logged only once that entry resolved elsewhere.
* **Never netted.** A netting ``POST /positions`` cannot name a ``dealId``: the
  venue applies a reduce POST or an opposite market entry FIFO to the oldest
  deal on the side it reduces (measured 2026-05-28, research dossier §9 #13).
  An entry is refused before it leaves while a foreign deal sits on the side
  it would net. A partial close never takes its whole target, so a foreign
  deal strictly younger than the target is never reached and does not block
  it (:meth:`_ForeignExposureMixin._exit_blockers`); one as old as the target
  or older is, and the POST is refused
  (:class:`~pynecore_capitalcom.exceptions.ForeignDealBlocksWriteError`). A
  refused exit is reported once (ERROR plus a ``foreign_deal_blocks_own_write``
  audit event) and retried; it goes through once the foreign deal is gone. A
  foreign deal that comes and goes is someone else's own business, so the
  refusal alone latches nothing. Only an exit that stays blocked, without a
  gap, for :data:`_FOREIGN_EXIT_LATCH_AFTER_S` — the bot cannot leave its own
  position without touching someone else's — latches an ``ENTRY_BLOCK``
  quarantine with that reason. The latch rests on a fresh ``/positions`` read,
  never on the poll's cached snapshot alone; a poll snapshot that no longer
  shows the block ends it, so a later block is timed from its own first
  refusal. An exit blocked only by deals an unconfirmed own entry may have
  opened waits without a latch. A refused opposite entry or resting entry
  order only drops that signal: a separate deal is no reason to stop the bot.
"""
import logging
from abc import ABC
from collections.abc import Iterable
from time import time as epoch_time
from typing import TYPE_CHECKING

from pynecore.core.broker.exceptions import OrderSkippedByPlugin
from pynecore.core.broker.models import QuarantineKind
from pynecore.core.broker.store_helpers import (
    ENTRY_KIND_POSITION,
    ENTRY_KIND_WORKING,
    EXTRAS_KEY_LEG_KIND,
    STATE_REJECTED,
)
from pynecore.types.strategy import ADOPTED_STARTUP_EXTRA_KEY

from ._base import _CapitalComBase
from .helpers import (
    EXTRAS_KEY_NETTED_FOREIGN_DEAL,
    _parse_iso_timestamp,
    _wire_float,
    _wire_id,
)
from .models import _ForeignExitBlock

if TYPE_CHECKING:
    from pynecore.core.broker.storage import OrderRow

logger = logging.getLogger(__name__)

#: How old the last poll's ``/positions`` snapshot may be and still refuse a
#: netting POST without a fresh read (ten poll intervals). Only a refusal may
#: rest on it: a POST is allowed only by the snapshot read under the account
#: write lock right before it.
_FOREIGN_SNAPSHOT_MAX_AGE_S = 15.0

#: How long a deal that left ``/positions`` is still remembered, so a late
#: activity that names it can tell a deal opened by an own working order from
#: one that existed before it.
_SEEN_DEAL_RETAIN_S = 600.0

#: A deal created more than this long before the activity that links it to an
#: own working order existed before that order executed. Both times are the
#: venue's own clock (``createdDateUTC`` and ``dateUTC``).
_PROMOTION_CREATED_SKEW_S = 5.0

#: A deal created more than this long before or after an unconfirmed own entry
#: was sent is not that entry's result (an entry that never resolves must not
#: shadow later deals for good). ``createdDateUTC`` is the venue clock, the send
#: time the local one: generous, a deal wrongly taken for a possible own one
#: only delays its foreign-deal log and keeps a blocked exit from latching.
_UNCONFIRMED_ENTRY_SKEW_S = 300.0

#: How long an own exit may stay blocked by deals this run did not open before
#: the block latches ``ENTRY_BLOCK``. Measured from the first refusal a fresh
#: ``/positions`` read established. A same-direction deal another party opens
#: and removes again within it (an add held for a few seconds, another run's
#: short trade) only delays the exit; an exit still blocked after it cannot
#: leave the bot's position without touching someone else's.
_FOREIGN_EXIT_LATCH_AFTER_S = 120.0

#: A block no read showed for this long is not continuous: the next refusal
#: starts its clock afresh. Sixty poll intervals, rate-limit backoffs included;
#: the poll's snapshots keep a lasting block continuous between the refusals.
_FOREIGN_EXIT_CONTINUITY_S = 90.0


def _describe_deal(position: dict) -> dict:
    """The audit view of one ``/positions`` row's ``position`` object."""
    return {
        'deal_id': _wire_id(position.get('dealId')),
        'direction': str(position.get('direction') or '').upper() or None,
        'size': _wire_float(position.get('size'), finite=True),
        'created': position.get('createdDateUTC'),
    }


class _ForeignExposureMixin(_CapitalComBase, ABC):
    """Ownership of venue deals and refusal of writes that would net a foreign one."""

    def _owned_deal_ids(self, positions: Iterable[dict]) -> set[str] | None:
        """The deals among ``positions`` this run's journal produced.

        A deal is the run's own when a live entry row of this run carries it
        as its ``dealId``, or when the deal's ``workingOrderId`` back-links an
        own row (a working order, or a market entry the venue executed through
        its working-order machinery under a fresh ``dealId``). Rows a previous
        design seeded for untracked venue deals at startup are not evidence of
        ownership.

        :param positions: ``GET /positions`` rows.
        :return: The owned ``dealId`` values, or ``None`` without a journal
            (a raw read, every deal counts).
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return None
        own_ids: set[str] = set()
        for row in store_ctx.iter_live_orders():
            extras = row.extras or {}
            if (extras.get(EXTRAS_KEY_LEG_KIND) is not None
                    or extras.get(ADOPTED_STARTUP_EXTRA_KEY)
                    or extras.get('kind') not in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)):
                continue
            if row.exchange_order_id:
                own_ids.add(row.exchange_order_id)
            promoted_from = _wire_id(extras.get('promoted_from_working_order_id'))
            if promoted_from is not None:
                own_ids.add(promoted_from)
        owned: set[str] = set()
        for raw in positions:
            position = raw.get('position') or {}
            deal_id = _wire_id(position.get('dealId'))
            if deal_id is None:
                continue
            working_order_id = _wire_id(position.get('workingOrderId'))
            if (deal_id in own_ids
                    or (working_order_id is not None and working_order_id in own_ids)):
                owned.add(deal_id)
        return owned

    def _unconfirmed_own_entries(self, symbol: str) -> list[tuple[str, float]]:
        """Own entries on ``symbol`` sent to the venue whose ``dealId`` is not known yet.

        Between the entry ``POST`` and its confirm (or for as long as an
        ambiguous confirm stays parked) the deal the entry opened is on the
        venue without anything in the journal naming it: a ``/positions`` row
        only carries its own ``p_<dealId>`` reference, never the ``POST``'s.

        :return: ``(direction, sent_at)`` per such entry: the venue direction
            (``'BUY'`` / ``'SELL'``) and the row's creation time.
        """
        store_ctx = self.store_ctx
        if store_ctx is None:
            return []
        entries: list[tuple[str, float]] = []
        for row in store_ctx.iter_live_orders(symbol=symbol):
            extras = row.extras or {}
            if (row.exchange_order_id
                    or row.state == STATE_REJECTED
                    or extras.get(EXTRAS_KEY_LEG_KIND) is not None
                    or extras.get(ADOPTED_STARTUP_EXTRA_KEY)
                    or extras.get('kind') not in (ENTRY_KIND_POSITION, ENTRY_KIND_WORKING)):
                continue
            entries.append(('BUY' if row.side == 'buy' else 'SELL',
                            row.created_ts_ms / 1000.0))
        return entries

    def _foreign_deals(
            self, positions: Iterable[dict], symbol: str, *, direction: str | None = None,
    ) -> list[dict]:
        """The deals on ``symbol`` this run does not own, as audit dicts.

        A deal that may be the result of an own entry still waiting for its
        ``dealId`` (:meth:`_unconfirmed_own_entries`: same direction, created
        around the time the entry was sent) is not provably foreign: its dict
        carries ``ownership_pending=True``.

        :param positions: ``GET /positions`` rows (account-wide).
        :param symbol: The epic.
        :param direction: Only deals of this direction (``'BUY'`` /
            ``'SELL'``); ``None`` for every direction.
        :return: One :func:`_describe_deal` dict per foreign deal; empty
            without a journal.
        """
        rows = [raw for raw in positions if (raw.get('market') or {}).get('epic') == symbol]
        owned = self._owned_deal_ids(rows)
        if owned is None:
            return []
        foreign: list[dict] = []
        unconfirmed: list[tuple[str, float]] | None = None
        for raw in rows:
            position = raw.get('position') or {}
            deal_id = _wire_id(position.get('dealId'))
            if deal_id is None or deal_id in owned:
                continue
            deal_direction = str(position.get('direction') or '').upper()
            if direction is not None and deal_direction != direction:
                continue
            deal = _describe_deal(position)
            if unconfirmed is None:
                unconfirmed = self._unconfirmed_own_entries(symbol)
            created = _parse_iso_timestamp(str(position.get('createdDateUTC') or ''))
            if any(entry_direction == deal_direction
                   and (created <= 0.0 or abs(created - sent_at) <= _UNCONFIRMED_ENTRY_SKEW_S)
                   for entry_direction, sent_at in unconfirmed):
                deal['ownership_pending'] = True
            foreign.append(deal)
        return foreign

    def _note_foreign_deals(self, positions: Iterable[dict], symbol: str) -> None:
        """Log each foreign deal on ``symbol`` once: INFO plus an audit event.

        A deal whose ownership is still pending is not logged yet.
        """
        store_ctx = self.store_ctx
        if store_ctx is None or not symbol:
            return
        for deal in self._foreign_deals(positions, symbol):
            deal_id = deal['deal_id']
            if deal_id in self._foreign_deals_logged or deal.get('ownership_pending'):
                # A deal an unconfirmed own entry may have opened is logged
                # only once that entry's ``dealId`` shows it is not the run's.
                continue
            self._foreign_deals_logged.add(deal_id)
            logger.info(
                "foreign deal %s on %s (%s %s, created %s): not opened by this "
                "run, never closed, amended or netted by it",
                deal_id, symbol, deal['direction'], deal['size'], deal['created'],
            )
            store_ctx.log_event(
                'foreign_deal_on_epic',
                exchange_order_id=deal_id,
                payload={'symbol': symbol, **deal},
            )

    def _remember_positions_snapshot(
            self, positions_by_deal: dict[str, dict], read_started_at: float,
    ) -> None:
        """Keep the poll's ``/positions`` snapshot for foreign-deal checks.

        The snapshot may refuse a netting POST without a fresh read while it
        is younger than :data:`_FOREIGN_SNAPSHOT_MAX_AGE_S`. Every deal seen is
        remembered (creation time, direction) for
        :data:`_SEEN_DEAL_RETAIN_S` after it was last seen. A blocked own exit
        the snapshot still shows blocked stays continuous; one it shows free
        (or whose target is gone) ends, so a later block starts its own clock.
        """
        self._positions_snapshot = (read_started_at, dict(positions_by_deal))
        for intent_key, block in list(self._foreign_exit_blocks.items()):
            if read_started_at < block.seen_at:
                # Read before the newest refusal: older than its evidence.
                continue
            if block.targets.isdisjoint(positions_by_deal):
                self._foreign_exit_unblocked(intent_key)
                continue
            blockers = self._exit_blockers(
                positions_by_deal.values(), block.symbol, block.direction, block.targets,
            )
            if any(not deal.get('ownership_pending') for deal in blockers):
                block.seen_at = max(block.seen_at, read_started_at)
            else:
                self._foreign_exit_unblocked(intent_key)
        seen = self._seen_position_deals
        for deal_id, raw in positions_by_deal.items():
            position = raw.get('position') or {}
            created = _parse_iso_timestamp(str(position.get('createdDateUTC') or ''))
            previous = seen.get(deal_id)
            seen[deal_id] = (
                previous[0] if previous is not None and previous[0] > 0.0 else created,
                str(position.get('direction') or '').upper(),
                read_started_at,
            )
        for deal_id in [d for d, (_, _, last) in seen.items()
                        if read_started_at - last > _SEEN_DEAL_RETAIN_S]:
            del seen[deal_id]

    def _cached_foreign_deals(self, symbol: str, direction: str) -> list[dict]:
        """Foreign deals of ``direction`` on ``symbol`` in the last poll's snapshot.

        Empty when the snapshot is missing or older than
        :data:`_FOREIGN_SNAPSHOT_MAX_AGE_S`: only a refusal may rest on it,
        and a quarantine only on one a fresh read already established
        (:meth:`_foreign_block_reported`).
        """
        snapshot = self._positions_snapshot
        if snapshot is None:
            return []
        read_at, positions_by_deal = snapshot
        if epoch_time() - read_at > _FOREIGN_SNAPSHOT_MAX_AGE_S:
            return []
        return self._foreign_deals(positions_by_deal.values(), symbol, direction=direction)

    def _exit_blockers(
            self, positions: Iterable[dict], symbol: str, direction: str,
            target_deal_ids: Iterable[str],
    ) -> list[dict]:
        """The deals this run did not open that a reduce POST of the targets could net.

        The venue nets a reduce POST FIFO, oldest deal of the side first, and
        moves on to the next one only once a deal is gone. A partial close
        never takes its whole target, so a deal strictly younger than every
        target is never reached. A foreign deal as old as a target or older,
        or one whose age or a target's is unknown, can be: it blocks.

        :param positions: ``GET /positions`` rows (account-wide).
        :param symbol: The epic.
        :param direction: The direction of the deals the exit reduces.
        :param target_deal_ids: The own deals the exit reduces.
        :return: The blocking deals as :meth:`_foreign_deals` dicts.
        """
        rows = [raw for raw in positions if (raw.get('market') or {}).get('epic') == symbol]
        foreign = self._foreign_deals(rows, symbol, direction=direction)
        if not foreign:
            return []
        targets = set(target_deal_ids)
        target_created = [
            _parse_iso_timestamp(str((raw.get('position') or {}).get('createdDateUTC') or ''))
            for raw in rows
            if _wire_id((raw.get('position') or {}).get('dealId')) in targets
        ]
        if not targets or len(target_created) != len(targets) or min(target_created) <= 0.0:
            return foreign
        youngest_target = max(target_created)
        return [
            deal for deal in foreign
            if _parse_iso_timestamp(str(deal.get('created') or '')) <= youngest_target
        ]

    def _cached_exit_blockers(
            self, symbol: str, direction: str, target_deal_ids: Iterable[str],
    ) -> list[dict]:
        """:meth:`_exit_blockers` in the last poll's snapshot.

        Empty when the snapshot is missing or older than
        :data:`_FOREIGN_SNAPSHOT_MAX_AGE_S`: only a refusal may rest on it.
        """
        snapshot = self._positions_snapshot
        if snapshot is None:
            return []
        read_at, positions_by_deal = snapshot
        if epoch_time() - read_at > _FOREIGN_SNAPSHOT_MAX_AGE_S:
            return []
        return self._exit_blockers(positions_by_deal.values(), symbol, direction,
                                   target_deal_ids)

    def _cached_deal_units(self, deal_ids: set[str], lot_step: float) -> int | None:
        """The lot units the last poll's snapshot showed for ``deal_ids`` together.

        ``None`` when the snapshot is stale, a deal is missing from it, or the
        lot step is unknown.
        """
        snapshot = self._positions_snapshot
        if snapshot is None or not deal_ids or lot_step <= 0.0:
            return None
        read_at, positions_by_deal = snapshot
        if epoch_time() - read_at > _FOREIGN_SNAPSHOT_MAX_AGE_S:
            return None
        units = 0
        for deal_id in deal_ids:
            raw = positions_by_deal.get(deal_id)
            if raw is None:
                return None
            size = _wire_float((raw.get('position') or {}).get('size'), finite=True)
            if size is None:
                return None
            units += round(size / lot_step)
        return units

    @staticmethod
    def _partial_close_block(foreign: list[dict]) -> str:
        """The refusal reason of a partial close the deals ``foreign`` block.

        ``foreign_deal_blocks_partial_close`` when a blocking deal is provably
        not the run's: held long enough, the block latches
        (:meth:`_foreign_exit_skip`). ``unconfirmed_deal_blocks_partial_close``
        when every blocking deal may still be an unconfirmed own entry's: the
        close only waits.
        """
        if foreign and all(deal.get('ownership_pending') for deal in foreign):
            return 'unconfirmed_deal_blocks_partial_close'
        return 'foreign_deal_blocks_partial_close'

    def _foreign_block_reported(self, reason: str, key: str, foreign: list[dict]) -> bool:
        """Whether this exact refusal was already reported in this process."""
        foreign_ids = sorted(str(deal.get('deal_id')) for deal in foreign)
        return (reason, key, tuple(foreign_ids)) in self._foreign_blocks_reported

    def _foreign_exit_cached_refusal(self, reason: str, key: str, foreign: list[dict]) -> bool:
        """Whether the last poll's snapshot may repeat this exit refusal without a fresh read.

        Only a refusal this process already reported from a fresh read may be
        repeated so. A deal provably not the run's blocks the exit only while
        its block clock runs and is not yet due to latch: a block the poll
        ended (the target left, the blockers turned out possibly own) is
        established again by a fresh read, which starts a new clock, so the
        same deal blocking the same exit later still latches.
        """
        if not self._foreign_block_reported(reason, key, foreign):
            return False
        if reason != 'foreign_deal_blocks_partial_close':
            return True
        return key in self._foreign_exit_blocks and not self._foreign_exit_latch_due(key)

    def _foreign_exit_latch_due(self, intent_key: str) -> bool:
        """Whether the exit ``intent_key`` has been blocked, without a gap, long enough to latch."""
        block = self._foreign_exit_blocks.get(intent_key)
        now = epoch_time()
        return (block is not None
                and intent_key not in self._foreign_exit_latched
                and now - block.seen_at <= _FOREIGN_EXIT_CONTINUITY_S
                and now - block.since >= _FOREIGN_EXIT_LATCH_AFTER_S)

    def _foreign_exit_unblocked(self, intent_key: str) -> None:
        """The exit ``intent_key`` is no longer blocked: its block clock restarts."""
        self._foreign_exit_blocks.pop(intent_key, None)
        self._foreign_exit_latched.discard(intent_key)

    def _foreign_exit_skip(
            self, *, reason: str, intent_key: str, symbol: str, foreign: list[dict],
            write: dict, fresh: bool, direction: str, targets: Iterable[str],
    ) -> OrderSkippedByPlugin:
        """Refuse an own exit that could net a deal this run did not open.

        Reported once per blocking set (:meth:`_report_foreign_block`). A
        refusal a fresh ``/positions`` read (or the snapshot under the
        account write lock) established starts the exit's block clock, or
        continues it while no read showed the exit free in between
        (:meth:`_remember_positions_snapshot`) and one showed it blocked
        within :data:`_FOREIGN_EXIT_CONTINUITY_S`. Such a refusal at least
        :data:`_FOREIGN_EXIT_LATCH_AFTER_S` after the clock started latches
        ``ENTRY_BLOCK`` once, naming the deals and whether another Pyne run
        on the account owns them. An exit blocked only by deals an
        unconfirmed own entry may have opened never latches and restarts the
        clock.

        :param reason: The refusal reason (:meth:`_partial_close_block`).
        :param intent_key: The blocked close intent.
        :param symbol: The epic.
        :param foreign: The blocking deals.
        :param write: What the bot was about to send, for the audit event.
        :param fresh: The refusal rests on a read made for this exit, not on
            the poll's cached snapshot.
        :param direction: The direction of the deals the exit reduces.
        :param targets: The own deals the exit reduces.
        :return: The skip the caller raises.
        """
        provable = reason == 'foreign_deal_blocks_partial_close'
        now = epoch_time()
        if not provable:
            self._foreign_exit_unblocked(intent_key)
        elif fresh:
            block = self._foreign_exit_blocks.get(intent_key)
            if block is None or now - block.seen_at > _FOREIGN_EXIT_CONTINUITY_S:
                self._foreign_exit_unblocked(intent_key)
                self._foreign_exit_blocks[intent_key] = _ForeignExitBlock(
                    since=now, seen_at=now, symbol=symbol, direction=direction,
                    targets=frozenset(targets),
                )
            else:
                block.seen_at = now
        message = self._report_foreign_block(
            reason=reason, key=intent_key, symbol=symbol, foreign=foreign,
            write=write, latch=False,
        )
        if provable and fresh and self._foreign_exit_latch_due(intent_key):
            self._foreign_exit_latched.add(intent_key)
            since = self._foreign_exit_blocks[intent_key].since
            foreign_ids = sorted(str(deal.get('deal_id')) for deal in foreign)
            other_runs = self._other_run_deal_ids(symbol, foreign_ids)
            latch_message = (
                f"{reason}: own exit {write} on {symbol} has been blocked for "
                f"{now - since:.0f} s by deal(s) {foreign_ids} this run did not "
                f"open{' (owned by another Pyne run on this account)' if other_runs else ''}; "
                f"its partial close would net them, new entries are blocked"
            )
            logger.error("%s", latch_message)
            context = {
                'reason': reason, 'key': intent_key, 'symbol': symbol,
                'foreign_deals': foreign, 'write': write,
                'blocked_for_s': now - since, 'other_pyne_run': other_runs,
            }
            if self.store_ctx is not None:
                self.store_ctx.log_event(
                    'foreign_deal_blocks_exit_latched',
                    exchange_order_id=','.join(foreign_ids) or None,
                    intent_key=intent_key,
                    payload=context,
                )
            if self.quarantine_sink is not None:
                self.quarantine_sink(latch_message, context, kind=QuarantineKind.ENTRY_BLOCK)
        return OrderSkippedByPlugin(
            message, intent_key=intent_key, reason=reason,
            context={'symbol': symbol, 'foreign_deals': foreign, 'write': write},
        )

    def _other_run_deal_ids(self, symbol: str, deal_ids: Iterable[str]) -> list[str]:
        """The deals among ``deal_ids`` another Pyne run on this account owns."""
        store_ctx = self.store_ctx
        if store_ctx is None:
            return []
        owned = store_ctx.foreign_live_exchange_order_ids(symbol=symbol)
        return sorted(deal_id for deal_id in deal_ids if deal_id in owned)

    def _promotion_conflict(
            self, side: str, deal_id: str, activity_ts: float,
            positions_by_deal: dict[str, dict] | None,
    ) -> str | None:
        """Why ``deal_id`` cannot be the deal an own working order opened.

        A working order's fill opens a fresh deal in the order's own
        direction. A deal of the other direction, or one created well before
        the activity that names it, is a deal the order netted when it
        executed — on a netting account an opposite working order reduces the
        oldest deal on that side, and that may be someone else's.

        :param side: The own working order's side (``'buy'`` / ``'sell'``).
        :param deal_id: The candidate ``dealId``.
        :param activity_ts: Venue time of the activity linking the two, ``0``
            when unknown.
        :param positions_by_deal: The poll's ``/positions`` rows by ``dealId``.
        :return: ``'direction_mismatch'`` / ``'pre_existing_deal'``, or
            ``None`` when nothing speaks against the deal.
        """
        raw = (positions_by_deal or {}).get(deal_id)
        if raw is not None:
            position = raw.get('position') or {}
            direction = str(position.get('direction') or '').upper()
            created = _parse_iso_timestamp(str(position.get('createdDateUTC') or ''))
        else:
            remembered = self._seen_position_deals.get(deal_id)
            if remembered is None:
                return None
            created, direction, _ = remembered
        expected = 'BUY' if side == 'buy' else 'SELL'
        if direction in ('BUY', 'SELL') and direction != expected:
            return 'direction_mismatch'
        if activity_ts > 0.0 and 0.0 < created < activity_ts - _PROMOTION_CREATED_SKEW_S:
            return 'pre_existing_deal'
        return None

    def _report_own_order_netted_foreign_deal(
            self, wo_row: 'OrderRow', wo_id: str, deal_id: str, conflict: str,
            positions_by_deal: dict[str, dict] | None,
    ) -> None:
        """Report that an own working order executed into a deal it did not open.

        The order netted someone else's deal: the bot's own action changed
        foreign exposure, which is never an intervention against the bot, but
        the bot can no longer trust its exposure on this epic. One ERROR, an
        audit event and an ``ENTRY_BLOCK`` quarantine; the deal's activity is
        left alone. The order's row remembers the netted deal, so its own
        execution is not booked as the run's exposure.
        """
        raw = (positions_by_deal or {}).get(deal_id)
        if raw is not None:
            deal = _describe_deal(raw.get('position') or {})
        else:
            remembered = self._seen_position_deals.get(deal_id)
            deal = {'deal_id': deal_id,
                    'direction': remembered[1] if remembered is not None else None,
                    'size': None, 'created': None}
        if (wo_row.extras or {}).get(EXTRAS_KEY_NETTED_FOREIGN_DEAL) is None:
            netted = {'deal_id': deal_id, 'conflict': conflict}
            self._rewrite_extras(
                wo_row.client_order_id,
                lambda extras: extras.__setitem__(EXTRAS_KEY_NETTED_FOREIGN_DEAL, netted),
            )
        self._report_foreign_block(
            reason='own_working_order_netted_foreign_deal',
            key=wo_row.client_order_id,
            symbol=wo_row.symbol,
            foreign=[deal],
            write={'working_order': wo_id, 'side': wo_row.side, 'qty': wo_row.qty,
                   'conflict': conflict},
            latch=True,
        )

    def _report_unattributed_opposite(
            self, *, coid: str, intent_key: str, symbol: str,
            opposite_deal_ids: list[str], attributable: bool, context: dict,
    ) -> None:
        """Report opposite deals that appeared during an own partial close.

        On a netting account an opposite deal next to the bot's reduce POST
        is either the POST's own reverse leg (the target was reduced or
        closed in the meantime) or someone else's opposite POST that consumed
        the target first. The POST's confirm names the deals it touched: a
        deal it does not name is not the bot's; when the confirm named no
        deal, nothing tells the two apart. Neither proves that someone else
        changed the bot's own state, so nothing is latched: ERROR plus a
        ``partial_close_unattributed_opposite`` audit event, the deal is never
        deleted, and the close stays parked until its own activity settles it
        or the fill-report grace passes without one
        (:meth:`~pynecore_capitalcom.reconcile._ReconcileMixin._resolve_partial_close_dispositions`).
        What happened to the target is booked from its own activities, and
        someone else's close of the bot's deal is reported as an intervention
        there.

        :param coid: The partial-close command row.
        :param intent_key: The close intent.
        :param symbol: The epic.
        :param opposite_deal_ids: The opposite deals the POST's confirm does not name.
        :param attributable: The confirm named the deals the POST touched.
        :param context: Snapshot details for the audit event.
        """
        message = (
            f"own partial close {coid} on {symbol}: opposite deal(s) "
            f"{opposite_deal_ids} appeared that "
            + ("the close's confirm does not name — someone else's"
               if attributable else
               "nothing attributes (the confirm names no deal) — someone else's "
               "or the close's own reverse leg")
            + "; the close is parked and the target's change is judged from its activity"
        )
        logger.error("%s", message)
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'partial_close_unattributed_opposite',
                client_order_id=coid,
                exchange_order_id=','.join(opposite_deal_ids) or None,
                intent_key=intent_key,
                payload={'opposite_deal_ids': opposite_deal_ids,
                         'attributable': attributable, 'symbol': symbol, **context},
            )

    def _report_foreign_block(
            self, *, reason: str, key: str, symbol: str, foreign: list[dict],
            write: dict, latch: bool,
    ) -> str:
        """Report once that an own write stopped short of a foreign deal.

        Logged ERROR and audited as ``foreign_deal_blocks_own_write`` once per
        ``(reason, key, foreign deals)``; with ``latch`` an ``ENTRY_BLOCK``
        quarantine carries the same message, because the bot cannot do what
        its strategy asks without touching someone else's deal.

        :param reason: Stable reason token.
        :param key: What was blocked (intent key, working-order dealId).
        :param symbol: The epic.
        :param foreign: The foreign deals involved.
        :param write: What the bot was about to do, for the audit event.
        :param latch: Latch an ``ENTRY_BLOCK`` quarantine.
        :return: The operator message.
        """
        foreign_ids = sorted(str(deal.get('deal_id')) for deal in foreign)
        message = (
            f"{reason}: own write {write} on {symbol} stopped, it could net "
            f"deal(s) {foreign_ids} this run did not open (account mode "
            f"{'hedging' if self._hedging_enabled else 'netting'})"
        )
        marker = (reason, key, tuple(foreign_ids))
        if marker in self._foreign_blocks_reported:
            return message
        self._foreign_blocks_reported.add(marker)
        other_runs = self._other_run_deal_ids(symbol, foreign_ids)
        if other_runs:
            message += f"; deal(s) {other_runs} are owned by another Pyne run on this account"
        logger.error("%s", message)
        context = {
            'reason': reason,
            'key': key,
            'symbol': symbol,
            'foreign_deals': foreign,
            'write': write,
            'account_mode': 'hedging' if self._hedging_enabled else 'netting',
            'other_pyne_run': other_runs,
        }
        if self.store_ctx is not None:
            self.store_ctx.log_event(
                'foreign_deal_blocks_own_write',
                exchange_order_id=','.join(foreign_ids) or None,
                payload=context,
            )
        if latch and self.quarantine_sink is not None:
            self.quarantine_sink(message, context, kind=QuarantineKind.ENTRY_BLOCK)
        return message

    def _foreign_block_skip(
            self, *, reason: str, intent_key: str, symbol: str, foreign: list[dict],
            write: dict, latch: bool,
    ) -> OrderSkippedByPlugin:
        """Report a foreign-deal refusal and build the skip the caller raises."""
        message = self._report_foreign_block(
            reason=reason, key=intent_key, symbol=symbol, foreign=foreign,
            write=write, latch=latch,
        )
        return OrderSkippedByPlugin(
            message, intent_key=intent_key, reason=reason,
            context={'symbol': symbol, 'foreign_deals': foreign, 'write': write},
        )
