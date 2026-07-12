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
# both transports are exercised against their own frozen snapshots; the API is
# the default source, HTML remains available and is pinned unchanged
SOURCES = ["html", "api"]


def _snapshot_name(gender, kind, source):
    suffix = "" if source == "html" else "_api"
    return f"{gender}_{kind}{suffix}"


def assert_matches_snapshot(result, name):
    expected = pd.read_parquet(SNAPSHOT_DIR / f"{name}.parquet")
    # the parquet round-trip stores missing object values as None where the
    # scraper produces np.nan; normalize both sides before exact comparison
    for df in (result, expected):
        obj_cols = df.select_dtypes(include="object").columns
        df[obj_cols] = df[obj_cols].where(pd.notna(df[obj_cols]), np.nan)
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize("gender", GENDERS)
@pytest.mark.parametrize("source", SOURCES)
def test_game_info_offline(offline_espn, gender, source):
    sc = SCRAPERS[gender]
    result = pd.concat(
        [sc.get_game_info(g, source=source) for g in recorded_games(gender)],
        ignore_index=True,
    )
    assert_matches_snapshot(result, _snapshot_name(gender, "game_info", source))


@pytest.mark.parametrize("gender", GENDERS)
@pytest.mark.parametrize("source", SOURCES)
def test_game_boxscore_offline(offline_espn, gender, source):
    sc = SCRAPERS[gender]
    result = pd.concat(
        [sc.get_game_boxscore(g, source=source) for g in recorded_games(gender)],
        ignore_index=True,
    )
    assert_matches_snapshot(result, _snapshot_name(gender, "game_boxscore", source))


@pytest.mark.parametrize("gender", GENDERS)
@pytest.mark.parametrize("source", SOURCES)
def test_game_pbp_offline(offline_espn, gender, source):
    sc = SCRAPERS[gender]
    result = pd.concat(
        [sc.get_game_pbp(g, source=source) for g in recorded_games(gender)],
        ignore_index=True,
    )
    assert_matches_snapshot(result, _snapshot_name(gender, "game_pbp", source))


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
@pytest.mark.parametrize("source", SOURCES)
def test_games_range_offline(offline_espn, gender, source):
    sc = SCRAPERS[gender]
    d = SCOREBOARD_DATES[gender]
    info, box, pbp = sc.get_games_range(d, d, source=source)
    assert_matches_snapshot(info, _snapshot_name(gender, "range_info", source))
    assert_matches_snapshot(box, _snapshot_name(gender, "range_boxscore", source))
    assert_matches_snapshot(pbp, _snapshot_name(gender, "range_pbp", source))


# --- cross-source parity ---------------------------------------------------
# Both transports must emit the same schema and the same values, except where
# one source legitimately carries data the other omits. Each exclusion below is
# justified; any OTHER mismatch is a real bug, not something to loosen.

# game info: HTML leaves the point spread blank on archived games; the API fills
# it from pickcenter. API is strictly better here, so it is not compared.
INFO_PARITY_EXCLUDE = ["home_point_spread"]

# pbp: the output `id` is a source-specific play identifier (different formats).
PBP_PARITY_EXCLUDE = ["id"]


def _normalize_nan(df):
    df = df.copy()
    obj_cols = df.select_dtypes(include="object").columns
    df[obj_cols] = df[obj_cols].where(pd.notna(df[obj_cols]), np.nan)
    return df


def _canonical_pbp(df):
    # row order at equal timestamps is not a cross-source guarantee (buzzer
    # plays vs "End of period" can be sequenced either way); sort on content
    key = ["home_score", "away_score", "secs_left_reg", "play_desc", "play_type"]
    return _normalize_nan(df.sort_values(key, na_position="first").reset_index(drop=True))


@pytest.mark.parametrize("gender", GENDERS)
def test_game_info_source_parity(offline_espn, gender):
    sc = SCRAPERS[gender]
    html = pd.concat(
        [sc.get_game_info(g, source="html") for g in recorded_games(gender)],
        ignore_index=True,
    )
    api = pd.concat(
        [sc.get_game_info(g, source="api") for g in recorded_games(gender)],
        ignore_index=True,
    )
    assert list(html.columns) == list(api.columns)
    cols = [c for c in html.columns if c not in INFO_PARITY_EXCLUDE]
    pd.testing.assert_frame_equal(
        _normalize_nan(api[cols]), _normalize_nan(html[cols]), check_dtype=False
    )


@pytest.mark.parametrize("gender", GENDERS)
def test_boxscore_source_parity(offline_espn, gender):
    sc = SCRAPERS[gender]
    sort_key = ["game_id", "team", "player_id"]
    html = pd.concat(
        [sc.get_game_boxscore(g, source="html") for g in recorded_games(gender)],
        ignore_index=True,
    ).sort_values(sort_key).reset_index(drop=True)
    api = pd.concat(
        [sc.get_game_boxscore(g, source="api") for g in recorded_games(gender)],
        ignore_index=True,
    ).sort_values(sort_key).reset_index(drop=True)
    assert list(html.columns) == list(api.columns)
    pd.testing.assert_frame_equal(
        _normalize_nan(api), _normalize_nan(html), check_dtype=False
    )


@pytest.mark.parametrize("gender", GENDERS)
def test_pbp_source_parity(offline_espn, gender):
    sc = SCRAPERS[gender]
    for gid in recorded_games(gender):
        html = _canonical_pbp(sc.get_game_pbp(gid, source="html"))
        api = _canonical_pbp(sc.get_game_pbp(gid, source="api"))
        # `id` aside, the schemas match. HTML drops its `id` column only for
        # games that carry a shot chart, so the column set can differ by that one
        # source-specific, excluded column; compare on the shared columns.
        common = [c for c in html.columns if c in api.columns]

        # id-derived athlete columns: the HTML __espnfitt__ embed omits per-play
        # athlete/participant data for many games; the API supplies it for every
        # play. Require agreement wherever HTML has a value, and API coverage at
        # least as good as HTML.
        coverage_cols = ["player_id", "assist_player_id", "shot_x", "shot_y"]
        exact = [
            c
            for c in common
            if c not in PBP_PARITY_EXCLUDE and c not in coverage_cols
        ]
        pd.testing.assert_frame_equal(
            api[exact], html[exact], check_dtype=False, obj=f"{gid} pbp"
        )

        for col in ("player_id", "assist_player_id"):
            html_present = html[col].astype(str).str.len() > 0
            assert (
                api[col].astype(str)[html_present]
                == html[col].astype(str)[html_present]
            ).all(), f"{gid}: {col} disagrees where HTML has a value"
            api_cov = (api[col].astype(str).str.len() > 0).sum()
            assert api_cov >= html_present.sum(), f"{gid}: {col} API coverage regressed"

        # shot coordinates: coverage is era/source-dependent (HTML shot chart vs
        # API play coordinates); compare only where both sources have a value.
        for col in ("shot_x", "shot_y"):
            both = html[col].notna() & api[col].notna()
            assert (api[col][both] == html[col][both]).all(), (
                f"{gid}: {col} disagrees where both sources have a value"
            )


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
