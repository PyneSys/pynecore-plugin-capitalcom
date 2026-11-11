"""Every REST request of a Capital.com user is spaced below the venue's rate limit.

Capital.com answers more than 10 requests per second per user with
``error.too-many.requests``. The pacing spaces the start of every request of
one user, across threads and processes, on a shared slot file.
"""

import asyncio
import os
import threading

import httpx
import pytest

from pynecore_capitalcom import CapitalCom, CapitalComConfig, rest

_REAL_PACE_REQUEST = rest._pace_request


@pytest.fixture
def paced_clock(monkeypatch, tmp_path):
    """A frozen monotonic clock, recorded waits and a private slot directory."""
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(rest, '_pace_request', _REAL_PACE_REQUEST)
    clock = {'now': 1000.0, 'waits': []}
    waits_lock = threading.Lock()

    def fake_sleep(seconds: float) -> None:
        with waits_lock:
            clock['waits'].append(seconds)

    monkeypatch.setattr('pynecore_capitalcom.rest.monotonic', lambda: clock['now'])
    monkeypatch.setattr('pynecore_capitalcom.rest.sleep', fake_sleep)
    _forget_slots()
    yield clock
    _forget_slots()


def _forget_slots() -> None:
    """Drop the process's open slots, as a separate process would not share them."""
    with rest._REQUEST_PACE_GUARD:
        for _lock, descriptor in rest._REQUEST_PACE_SLOTS.values():
            os.close(descriptor)
        rest._REQUEST_PACE_SLOTS.clear()


_KEY = rest._request_pace_key('u@example.com', 'k')


def __test_back_to_back_requests_are_spaced__(paced_clock):
    spacing = rest._REQUEST_MIN_SPACING_S
    for _ in range(3):
        rest._pace_request(_KEY)
    assert paced_clock['waits'] == [pytest.approx(spacing), pytest.approx(2 * spacing)]


def __test_spacing_stays_under_the_venue_limit__():
    assert 1.0 / rest._REQUEST_MIN_SPACING_S < 10.0


def __test_a_request_after_the_spacing_does_not_wait__(paced_clock):
    rest._pace_request(_KEY)
    paced_clock['now'] += rest._REQUEST_MIN_SPACING_S
    rest._pace_request(_KEY)
    assert paced_clock['waits'] == []


def __test_another_process_shares_the_slot_file__(paced_clock):
    rest._pace_request(_KEY)
    _forget_slots()
    rest._pace_request(_KEY)
    assert paced_clock['waits'] == [pytest.approx(rest._REQUEST_MIN_SPACING_S)]


def __test_users_are_paced_independently__(paced_clock):
    rest._pace_request(_KEY)
    rest._pace_request(rest._request_pace_key('other@example.com', 'k'))
    assert paced_clock['waits'] == []


def __test_concurrent_threads_take_distinct_slots__(paced_clock):
    spacing = rest._REQUEST_MIN_SPACING_S
    threads = [
        threading.Thread(target=rest._pace_request, args=(_KEY,))
        for _ in range(8)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(paced_clock['waits']) == [
        pytest.approx(spacing * slot) for slot in range(1, 8)
    ]


def __test_a_slot_from_before_a_reboot_is_dropped__(paced_clock):
    rest._pace_request(_KEY)
    paced_clock['now'] -= 10_000.0
    rest._pace_request(_KEY)
    assert paced_clock['waits'] == []


def __test_pace_key_is_the_login_user__():
    assert rest._request_pace_key(' U@Example.com ', 'k1') == rest._request_pace_key(
        'u@example.com', 'k2')
    assert rest._request_pace_key('', 'k1') != rest._request_pace_key('', 'k2')


def __test_every_rest_call_is_paced_before_the_send__(monkeypatch):
    broker = CapitalCom(config=CapitalComConfig(
        demo=True, user_email="u@example.com", api_key="k", api_password="p"))
    events: list[str] = []
    monkeypatch.setattr(
        rest, '_pace_request',
        lambda pace_key: events.append('pace' if pace_key == _KEY else 'wrong key'),
    )

    def fake_get(_url, **_kwargs):
        events.append('send')
        return httpx.Response(200, json={'positions': []})

    monkeypatch.setattr(rest._HTTP_CLIENT, 'get', fake_get)
    monkeypatch.setattr(broker, '_refresh_session_if_stale', lambda: None)
    asyncio.run(broker._call("positions", method="get"))
    assert events == ['pace', 'send']
