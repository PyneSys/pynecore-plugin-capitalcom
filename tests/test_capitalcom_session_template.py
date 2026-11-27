"""
@pyne
"""
import json
from datetime import UTC, datetime, time
from pathlib import Path

import pytest

import pynecore_capitalcom.provider as provider_module
from pynecore.core.syminfo import SymInfoAnchor
from pynecore_capitalcom import CapitalCom, CapitalComConfig
from pynecore_capitalcom.helpers import _regular_week_opening_hours

#: Capital.com ``openingHours`` of 18 epics, fetched on 2026-11-27 (Thanksgiving week).
__test_helper_HOLIDAY_WEEK = json.loads(
    (Path(__file__).parent / 'capitalcom_holiday_week_opening_hours.json').read_text())


def main():
    pass


def __test_helper_symbol_info(monkeypatch, epic, opening_hours, now):
    """Build the symbol info of ``epic`` from mocked market details, at instant ``now``."""
    broker = CapitalCom(symbol=epic, timeframe='60', config=CapitalComConfig(
        demo=True, user_email="test@example.com", api_key="test-key",
        api_password="test-password"))

    def market_details():
        return {
            'instrument': {
                'name': epic, 'epic': epic, 'currency': "USD", 'symbol': epic,
                'type': "INDICES", 'lotSize': 1.0, 'openingHours': opening_hours,
            },
            'dealingRules': {'minStepDistance': {'value': 0.1}},
        }

    monkeypatch.setattr(broker, 'get_single_market_details', market_details)
    monkeypatch.setattr(broker, 'get_historical_prices', lambda **_kw: {
        'prices': [{'closePrice': {'bid': 100.0, 'ask': 100.5}}]})
    monkeypatch.setattr(provider_module, '_utc_now', lambda: now)
    return broker.update_symbol_info()


#: A Wednesday after both DST changes of autumn 2026: Sydney +11, New York -5.
__test_helper_NOV_WEEK = datetime(2026, 11, 25, 12, 0, tzinfo=UTC)
#: A Wednesday between Sydney's DST start (2026-10-04) and New York's DST end
#: (2026-11-01): Sydney +11, New York -4.
__test_helper_OCT_WEEK = datetime(2026, 10, 7, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize('now, local', [
    (__test_helper_NOV_WEEK, time(17, 30)),
    (__test_helper_OCT_WEEK, time(18, 30)),
])
def __test_au200_anchors_follow_both_dst_rules__(monkeypatch, now, local):
    """Sydney 09:30 Mon-Fri lands on the previous New York evening, Sun-Thu, at the
    offset of the fetch week's real dates: 16 hours in November, 15 in early October."""
    info = __test_helper_symbol_info(
        monkeypatch, 'AU200', __test_helper_HOLIDAY_WEEK['AU200'], now)
    assert info.session_anchors == sorted(
        SymInfoAnchor(day, local) for day in (6, 0, 1, 2, 3))


def __test_btcusd_anchors_every_day_at_17__(monkeypatch):
    info = __test_helper_symbol_info(
        monkeypatch, 'BTCUSD', __test_helper_HOLIDAY_WEEK['BTCUSD'], __test_helper_NOV_WEEK)
    assert info.session_anchors == [SymInfoAnchor(day, time(17, 0)) for day in range(7)]


def __test_aapl_anchors_at_the_new_york_open__(monkeypatch):
    info = __test_helper_symbol_info(
        monkeypatch, 'AAPL', __test_helper_HOLIDAY_WEEK['AAPL'], __test_helper_OCT_WEEK)
    assert info.session_anchors == [SymInfoAnchor(day, time(9, 30)) for day in range(5)]


def __test_unlisted_epic_gets_no_anchors__(monkeypatch):
    info = __test_helper_symbol_info(
        monkeypatch, 'US30', __test_helper_HOLIDAY_WEEK['US30'], __test_helper_NOV_WEEK)
    assert info.session_anchors == []
    assert info.session_starts


@pytest.mark.parametrize('epic', [
    'US30', 'US100', 'US500', 'DE40', 'UK100', 'J225', 'HK50',
    'GOLD', 'SILVER', 'OIL_CRUDE', 'NATURALGAS',
])
def __test_holiday_thursday_and_friday_take_the_regular_shape__(epic):
    """Thanksgiving's Thursday closure and Friday early close stay out of the template:
    Thursday takes the Monday-Wednesday shape, Friday closes at the daily close
    (measured on the regular week of 2026-11-16 for US30, GOLD and OIL_CRUDE)."""
    fetched = __test_helper_HOLIDAY_WEEK[epic]
    result, replaced = _regular_week_opening_hours(fetched)
    assert replaced == ['thu', 'fri']
    assert result['thu'] == fetched['wed']
    assert result['fri'] == fetched['wed'][:1]
    for day in ('mon', 'tue', 'wed', 'sat', 'sun', 'zone'):
        assert result[day] == fetched[day]


@pytest.mark.parametrize('epic', ['AU200', 'EURUSD', 'USDJPY', 'GBPUSD', 'BTCUSD'])
def __test_regular_week_stays_as_fetched__(epic):
    fetched = __test_helper_HOLIDAY_WEEK[epic]
    result, replaced = _regular_week_opening_hours(fetched)
    assert replaced == []
    assert result == fetched


def __test_share_cfd_holiday_closure_is_bridged__():
    """AAPL: the Thursday 05:00 - Friday 01:00 closure goes away; the Friday early
    close cannot be told from the regular close without a daily break and stays."""
    fetched = __test_helper_HOLIDAY_WEEK['AAPL']
    result, replaced = _regular_week_opening_hours(fetched)
    assert replaced == ['thu', 'fri']
    assert result['thu'] == fetched['wed']
    assert result['fri'] == ['00:00 - 01:15', '01:15 - 18:00']
    assert result['mon'] == fetched['mon']


def __test_friday_only_deviation_stays_as_fetched__():
    """Without a deviating Monday-Thursday the week has not proved to hold an
    exception, so a Friday that closes before the daily close is kept."""
    fetched = dict(__test_helper_HOLIDAY_WEEK['US30'], thu=__test_helper_HOLIDAY_WEEK['US30']['wed'])
    result, replaced = _regular_week_opening_hours(fetched)
    assert replaced == []
    assert result['fri'] == ['00:00 - 21:00']


def __test_daytime_market_is_not_touched__():
    hours = {'zone': "UTC", 'mon': ["08:00 - 16:30"], 'tue': ["08:00 - 16:30"],
             'wed': ["08:00 - 16:30"], 'thu': [], 'fri': ["08:00 - 12:30"],
             'sat': [], 'sun': []}
    assert _regular_week_opening_hours(hours) == (hours, [])


def __test_no_majority_keeps_the_fetched_week__():
    hours = {'zone': "UTC", 'mon': ["00:00 - 22:00", "22:05 - 00:00"],
             'tue': ["00:00 - 21:00", "22:05 - 00:00"], 'wed': ["00:00 - 20:00", "22:05 - 00:00"],
             'thu': ["00:00 - 19:00", "22:05 - 00:00"], 'fri': ["00:00 - 18:00"],
             'sat': [], 'sun': ["23:00 - 00:00"]}
    assert _regular_week_opening_hours(hours) == (hours, [])


def __test_holiday_template_reaches_the_symbol_info__(monkeypatch):
    """Through ``update_symbol_info``: US30's local Thursday matches its Wednesday and
    the week ends at Friday 17:00 New York, not at the holiday's 16:00."""
    info = __test_helper_symbol_info(
        monkeypatch, 'US30', __test_helper_HOLIDAY_WEEK['US30'], __test_helper_NOV_WEEK)

    def local_day(day):
        return sorted((iv.start, iv.end) for iv in info.opening_hours if iv.day == day)

    assert local_day(3) == local_day(2)
    assert local_day(4) == [(time(0, 0), time(17, 0))]
    assert max(e for e in info.session_ends if e.day == 4).time == time(17, 0)
