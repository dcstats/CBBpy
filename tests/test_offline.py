"""Deterministic tests against recorded ESPN pages — no network.

Every test here replays pages recorded by tests/record_fixtures.py through the
full scraper (HTML parsing, JSON extraction, all transformation logic) and
compares the result exactly against a snapshot frozen at record time. The
input cannot drift, so a failure always means the code changed behavior.
If ESPN intentionally changes its format, re-record with
`python tests/record_fixtures.py` and review the diff.
"""

import numpy as np
import pandas as pd
import pytest

from cbbpy import mens_scraper as ms, womens_scraper as ws
from cbbpy.utils.cbbpy_utils import InvalidDateRangeError

from tests.conftest import (
    PLAYERS,
    SCHEDULES,
    SCOREBOARD_DATES,
    SNAPSHOT_DIR,
    recorded_games,
)

SCRAPERS = {"mens": ms, "womens": ws}
GENDERS = ["mens", "womens"]


def assert_matches_snapshot(result, name):
    expected = pd.read_parquet(SNAPSHOT_DIR / f"{name}.parquet")
    # the parquet round-trip stores missing object values as None where the
    # scraper produces np.nan; normalize both sides before exact comparison
    for df in (result, expected):
        obj_cols = df.select_dtypes(include="object").columns
        df[obj_cols] = df[obj_cols].where(pd.notna(df[obj_cols]), np.nan)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize("gender", GENDERS)
def test_game_info_offline(offline_espn, gender):
    sc = SCRAPERS[gender]
    result = pd.concat(
        [sc.get_game_info(g) for g in recorded_games(gender)], ignore_index=True
    )
    assert_matches_snapshot(result, f"{gender}_game_info")


@pytest.mark.parametrize("gender", GENDERS)
def test_game_boxscore_offline(offline_espn, gender):
    sc = SCRAPERS[gender]
    result = pd.concat(
        [sc.get_game_boxscore(g) for g in recorded_games(gender)], ignore_index=True
    )
    assert_matches_snapshot(result, f"{gender}_game_boxscore")


@pytest.mark.parametrize("gender", GENDERS)
def test_game_pbp_offline(offline_espn, gender):
    sc = SCRAPERS[gender]
    result = pd.concat(
        [sc.get_game_pbp(g) for g in recorded_games(gender)], ignore_index=True
    )
    assert_matches_snapshot(result, f"{gender}_game_pbp")


@pytest.mark.parametrize("gender", GENDERS)
def test_player_info_offline(offline_espn, gender):
    sc = SCRAPERS[gender]
    result = pd.concat(
        [sc.get_player_info(p) for p in PLAYERS[gender]], ignore_index=True
    )
    assert_matches_snapshot(result, f"{gender}_player")


@pytest.mark.parametrize("gender", GENDERS)
def test_team_schedule_offline(offline_espn, gender):
    sc = SCRAPERS[gender]
    result = pd.concat(
        [sc.get_team_schedule(team, season) for season, team in SCHEDULES[gender]],
        ignore_index=True,
    )
    assert_matches_snapshot(result, f"{gender}_team_schedule")


@pytest.mark.parametrize("gender", GENDERS)
def test_games_range_offline(offline_espn, gender):
    sc = SCRAPERS[gender]
    d = SCOREBOARD_DATES[gender]
    info, box, pbp = sc.get_games_range(d, d)
    assert_matches_snapshot(info, f"{gender}_range_info")
    assert_matches_snapshot(box, f"{gender}_range_boxscore")
    assert_matches_snapshot(pbp, f"{gender}_range_pbp")


def test_teams_from_conference_offline():
    # no fixtures needed: reads the bundled team-map CSVs and fuzzy-matches
    assert ms.get_teams_from_conference("Pac-12", 2017) == [
        "Arizona", "Oregon", "UCLA", "Utah", "California", "USC", "Colorado",
        "Arizona State", "Stanford", "Washington State", "Washington", "Oregon State",
    ]
    # fuzzy match and abbreviation resolve to the same conference
    assert ms.get_teams_from_conference("pac 12", 2017) == ms.get_teams_from_conference(
        "Pac-12 Conference", 2017
    )
    assert ms.get_teams_from_conference("a10", 2022) == [
        "Davidson", "VCU", "Dayton", "St. Bonaventure", "Saint Louis", "Richmond",
        "George Washington", "Fordham", "George Mason", "Massachusetts",
        "Rhode Island", "La Salle", "Saint Joseph's", "Duquesne",
    ]
    assert ws.get_teams_from_conference("big east", 2018) == [
        "DePaul", "Marquette", "Villanova", "Creighton", "St. John's",
        "Georgetown", "Seton Hall", "Butler", "Xavier", "Providence",
    ]


@pytest.mark.parametrize("func", [ms.get_games_season, ws.get_games_season])
def test_season_future_raises(func):
    # raises before any network request is made
    with pytest.raises(
        InvalidDateRangeError, match="The start date must not be in the future."
    ):
        func(3000)


def test_missing_fixture_fails_loudly(offline_espn):
    # an unrecorded URL must register as a miss instead of silently retrying;
    # the scraper still returns its empty-DataFrame fallback
    df = ms.get_game_info("999999999")
    assert df.empty
    assert any("999999999" in url for url in offline_espn.misses)
    # clear so the fixture's teardown assertion (which guards real tests
    # against unrecorded URLs) doesn't fail this deliberate miss
    offline_espn.misses.clear()
