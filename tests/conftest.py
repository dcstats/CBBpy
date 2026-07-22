import gzip
import json
from pathlib import Path

import pytest

from cbbpy.utils import cbbpy_utils


FIXTURE_DIR = Path(__file__).parent / "fixtures"
PAGES_DIR = FIXTURE_DIR / "pages"
SNAPSHOT_DIR = FIXTURE_DIR / "snapshots"


# Entities recorded by tests/record_fixtures.py and replayed by tests/test_offline.py.
# Game pages cover: a recent neutral-site game, a 2013 edge case, a
# conference-tournament game, an OT game, an NCAA-tournament game, a
# substitution-era game (ESPN added sub plays to PBP mid-Feb 2025), and
# (womens) a pre-2015 halves-era game. The scoreboard dates double as the
# offline get_games_range input, so they are championship/Final Four dates
# with very few games; those games' pages are recorded too (see games.json).
STATIC_GAMES = {
    "mens": ["401581583", "400498476", "400871140", "401745972"],
    "womens": ["401486000", "303442739", "401487896"],
}
# 2021-04-03 mens: Final Four (Gonzaga-UCLA went to OT; both games postseason)
# 2023-04-02 womens: national championship (LSU-Iowa)
SCOREBOARD_DATES = {"mens": "2021-04-03", "womens": "2023-04-02"}
PLAYERS = {"mens": ["4433093"], "womens": ["14670"]}
SCHEDULES = {
    "mens": [(2017, "Pacific"), (2010, "Alabama A&M")],
    "womens": [(2014, "TCU"), (2021, "Cal Poly")],
}


def recorded_games(gender):
    """All recorded game ids for a gender: STATIC_GAMES + the scoreboard date's games."""
    games = json.loads((FIXTURE_DIR / "games.json").read_text())
    return games[gender]


class FakeResponse:
    """Stands in for requests.Response.

    The HTML path reads .content; the API path reads .json().
    """

    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def json(self):
        return json.loads(self.content)


class FixtureStore:
    """Serves recorded ESPN pages keyed by URL, tracking any unrecorded requests."""

    def __init__(self, root: Path):
        self.root = root
        self.manifest = json.loads((root / "manifest.json").read_text())
        self.misses = []

    def get(self, url: str) -> bytes:
        entry = self.manifest.get(url)
        if entry is None:
            self.misses.append(url)
            raise KeyError(f"No recorded fixture for {url}")
        return gzip.decompress((self.root / "pages" / entry["file"]).read_bytes())


class SerialParallel:
    """Drop-in for joblib.Parallel that runs in-process.

    The real Parallel defaults to the loky (process) backend, where a
    monkeypatched requests.get would not propagate to workers. delayed(f)(*a, **kw)
    returns (f, args, kwargs), so calling each tuple serially is equivalent.
    """

    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, iterable):
        return [func(*args, **kwargs) for func, args, kwargs in iterable]


@pytest.fixture
def offline_espn(monkeypatch):
    """Replay recorded ESPN pages with no network access.

    Patches the scraper module so that:
    - r.get serves gzipped pages from tests/fixtures/ keyed by exact URL
    - retries collapse to a single attempt with no sleeps, so a missing
      fixture fails in milliseconds instead of silently returning an empty
      DataFrame after ~45s of retries
    - joblib.Parallel runs serially in-process

    Note: a page-not-found game raises PageNotFoundError from the scraper
    rather than being cached, so there is no module-level state to reset here.
    """
    store = FixtureStore(FIXTURE_DIR)

    def fake_get(url, *args, **kwargs):
        return FakeResponse(store.get(url))

    monkeypatch.setattr(cbbpy_utils.r, "get", fake_get)
    monkeypatch.setattr(cbbpy_utils.time, "sleep", lambda *_: None)
    monkeypatch.setattr(cbbpy_utils, "ATTEMPTS", 1)
    monkeypatch.setattr(cbbpy_utils, "Parallel", SerialParallel)

    yield store

    assert not store.misses, (
        "Offline test requested URLs with no recorded fixture "
        f"(re-record with tests/record_fixtures.py?): {store.misses}"
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "live: makes live requests to ESPN; deselect with -m 'not live'"
    )
