"""Deterministic tests against recorded ESPN pages — no network.

Every test here replays pages recorded by tests/record_fixtures.py through the
full scraper (HTML parsing, JSON extraction, all transformation logic) and
compares the result exactly against a snapshot frozen at record time. The
input cannot drift, so a failure always means the code changed behavior.
If ESPN intentionally changes its format, re-record with
`python tests/record_fixtures.py` and review the diff.
"""

import logging
import subprocess
import sys
import textwrap
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
from bs4 import BeautifulSoup as bs

import cbbpy
from cbbpy import mens_scraper as ms, womens_scraper as ws
from cbbpy.utils import cbbpy_utils
from cbbpy.utils import espn_api
from cbbpy.utils.cbbpy_utils import (
    CBBpyWarning,
    DEPRECATED_COLUMNS,
    InvalidDateRangeError,
    PageNotFoundError,
    _classify_page_failure,
    _get_game_pbp_helper,
    _get_id_from_team,
    _get_team_map,
)

from tests.conftest import (
    FakeResponse,
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

# game info: the odds columns come from the API's pickcenter block, which the
# archived HTML embed lacks entirely, so the API is strictly better here and none
# of these are value-compared (HTML leaves them blank/NaN).
INFO_PARITY_EXCLUDE = [
    "home_point_spread",
    "over_under",
    "home_moneyline",
    "away_moneyline",
]

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
        # `id` aside, the schemas match; its values are source-specific formats,
        # so it is excluded from value comparison. Compare on the shared columns.
        common = [c for c in html.columns if c in api.columns]

        # id-derived athlete columns: the HTML __espnfitt__ embed omits per-play
        # athlete/participant data for many games; the API supplies it for every
        # play. Require agreement wherever HTML has a value, and API coverage at
        # least as good as HTML.
        id_coverage_cols = ["player_id", "assist_player_id"]
        # name columns: same coverage story, but the two sources spell the name
        # differently (HTML embed athlete.name vs API boxscore displayName for
        # the same player_id), so only coverage is comparable, not the value.
        name_coverage_cols = ["player_name", "assist_player"]
        # home_win_prob: both sources derive it from the same ESPN win-prob model
        # (API summary["winprobability"] vs the game page's wnPrb block), so values
        # match within float tolerance; compared separately with a numeric atol.
        coverage_cols = (
            id_coverage_cols
            + name_coverage_cols
            + ["shot_x", "shot_y", "home_win_prob"]
        )
        exact = [
            c
            for c in common
            if c not in PBP_PARITY_EXCLUDE and c not in coverage_cols
        ]
        pd.testing.assert_frame_equal(
            api[exact], html[exact], check_dtype=False, obj=f"{gid} pbp"
        )

        for col in id_coverage_cols:
            html_present = html[col].astype(str).str.len() > 0
            assert (
                api[col].astype(str)[html_present]
                == html[col].astype(str)[html_present]
            ).all(), f"{gid}: {col} disagrees where HTML has a value"
            api_cov = (api[col].astype(str).str.len() > 0).sum()
            assert api_cov >= html_present.sum(), f"{gid}: {col} API coverage regressed"

        for col in name_coverage_cols:
            html_cov = (html[col].astype(str).str.len() > 0).sum()
            api_cov = (api[col].astype(str).str.len() > 0).sum()
            assert api_cov >= html_cov, f"{gid}: {col} API coverage regressed"

        # shot coordinates: coverage is era/source-dependent (HTML shot chart vs
        # API play coordinates); compare only where both sources have a value.
        for col in ("shot_x", "shot_y"):
            both = html[col].notna() & api[col].notna()
            assert (api[col][both] == html[col][both]).all(), (
                f"{gid}: {col} disagrees where both sources have a value"
            )

        # home win probability: same underlying model, compared with a numeric
        # tolerance where both sources have a value for the play.
        both = html["home_win_prob"].notna() & api["home_win_prob"].notna()
        assert np.allclose(
            api["home_win_prob"][both], html["home_win_prob"][both], rtol=0, atol=1e-6
        ), f"{gid}: home_win_prob disagrees where both sources have a value"


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


def test_team_map_season_fallback():
    # no fixtures needed: reads the bundled team-map CSVs only
    assert _get_id_from_team("UConn", 2025, "mens") == (41, "UConn")
    # a season past the map falls back to the latest available season
    # instead of crashing on an empty candidate list
    latest = int(_get_team_map("mens").season.max())
    assert _get_id_from_team("UConn", latest + 1, "mens") == (41, "UConn")
    assert ms.get_teams_from_conference("ACC", latest + 1) == ms.get_teams_from_conference(
        "ACC", latest
    )


@pytest.mark.parametrize("gender", ["mens", "womens"])
def test_team_map_conference_abbs(gender):
    # guards the static CSVs against ESPN abbreviation drift when a new
    # season is appended (see update_team_map.py's normalization step)
    df = _get_team_map(gender)
    seasons = sorted(df.season.unique())

    # a conference keeps its abbreviation across the two most recent seasons
    prev, last = seasons[-2], seasons[-1]
    maps = {
        s: df[df.season == s][["conference", "conference_abb"]]
        .drop_duplicates()
        .set_index("conference")
        .conference_abb.to_dict()
        for s in (prev, last)
    }
    drift = {
        c: (maps[prev][c], maps[last][c])
        for c in maps[prev].keys() & maps[last].keys()
        if maps[prev][c] != maps[last][c]
    }
    assert not drift, f"conference abbs drifted between {prev} and {last}: {drift}"

    # within a season, no abbreviation is shared by two conferences (case-
    # insensitive, since lookups lowercase); 2021 is grandfathered — ESPN
    # split conferences into divisions that year (Patriot 'South' vs SoCon 'south')
    for season, grp in df.groupby("season"):
        if season == 2021:
            continue
        pairs = grp[["conference", "conference_abb"]].drop_duplicates()
        n_confs = pairs.groupby(pairs.conference_abb.str.lower()).conference.nunique()
        clashes = n_confs[n_confs > 1]
        assert clashes.empty, f"{season}: abb shared by multiple conferences: {list(clashes.index)}"


@pytest.mark.parametrize("func", [ms.get_games_season, ws.get_games_season])
def test_season_future_raises(func):
    # raises before any network request is made
    with pytest.raises(
        InvalidDateRangeError, match="The start date must not be in the future."
    ):
        func(3000)


@pytest.mark.parametrize("func", [ms.get_games_season, ws.get_games_season])
def test_season_accepts_string(monkeypatch, func):
    # #87: a string season must not raise TypeError from `season-1` arithmetic,
    # and must produce the same date window as the int season
    captured = []
    monkeypatch.setattr(
        cbbpy_utils,
        "_get_games_range",
        lambda start, end, *a, **kw: captured.append((start, end)),
    )
    func("2024")
    func(2024)
    assert captured[0] == captured[1] == ("2023-11-01", "2024-05-01")


def test_pbp_malformed_clock_does_not_crash():
    # no fixtures needed: a play with no clock, and one with a colon-less
    # clock, degrade to 0:00 instead of killing the whole game's parse (#82)
    gamepackage = {
        "pbp": {
            "tms": {"home": {"nm": "Home U"}, "away": {"nm": "Away U"}},
            "plays": [
                {"id": 1, "text": "Foul on Someone.", "period": {"number": 1}},
                {
                    "id": 2,
                    "text": "Someone made Jumper.",
                    "clock": {"displayValue": "35.5"},
                    "period": {"number": 2},
                },
                {
                    "id": 3,
                    "text": "Someone made Layup.",
                    "clock": {"displayValue": "12:34"},
                    "period": {"number": 2},
                },
            ],
        },
        "gmInfo": {"dtTm": "2024-01-15T00:00Z"},
        "win_prob": {},
    }
    df = _get_game_pbp_helper(gamepackage, "0", "mens")
    assert len(df) == 3
    assert df.set_index("id").loc["1", "secs_left_period"] == 0
    assert df.set_index("id").loc["2", "secs_left_period"] == 0
    assert df.set_index("id").loc["3", "secs_left_period"] == 12 * 60 + 34
    # deprecated alias mirrors the canonical column
    assert (df["secs_left_half"] == df["secs_left_period"]).all()
    assert (df["half"] == df["period"]).all()


@pytest.mark.parametrize("source", ["html", "api"])
def test_game_ids_failure_returns_list(offline_espn, source):
    # a persistent fetch failure returns the documented type (a list),
    # not an empty DataFrame (#83)
    ids = ms.get_game_ids("1999-01-01", source=source)
    assert ids == []
    offline_espn.misses.clear()


def test_games_team_failed_schedule_returns_empty(offline_espn):
    # a failed schedule fetch returns three empty DataFrames instead of
    # raising AttributeError on the empty schedule (#83)
    info, box, pbp = ms.get_games_team("UConn", 1999)
    assert info.empty and box.empty and pbp.empty
    offline_espn.misses.clear()


def test_games_range_all_games_fail_returns_empty(offline_espn, monkeypatch):
    # game-id discovery succeeds but every _get_game call fails, so each game
    # yields three column-less empty frames. The concat then has no columns to
    # sort by, which used to raise KeyError: 'game_datetime' in _sort_results.
    # The all-failures path must return three empty DataFrames instead (#89).
    empty = (pd.DataFrame([]), pd.DataFrame([]), pd.DataFrame([]))
    monkeypatch.setattr(cbbpy_utils, "_get_game", lambda *a, **k: empty)
    d = SCOREBOARD_DATES["mens"]
    info, box, pbp = ms.get_games_range(d, d)
    assert info.empty and box.empty and pbp.empty


@pytest.mark.parametrize(
    "status, body, expected",
    [
        # status code is the primary signal (#74)
        (404, b"<html><body>no marker</body></html>", "Page not found error"),
        (202, b"", "WAF challenge"),
        (400, b"<html><body>no marker</body></html>", "HTTP 400"),
        # body text stays a fallback: ESPN has served a 200 whose body is an
        # error page for games whose data pipeline broke
        (200, b"<html><body>Page not found.</body></html>", "Page not found error"),
        (200, b"<html><body>Page error</body></html>", "Page error"),
        # a healthy response means the failure was in parsing, not fetching
        (200, b"<html><body>fine</body></html>", None),
    ],
)
def test_classify_page_failure(status, body, expected):
    soup = bs(body, "lxml")
    page = SimpleNamespace(content=body, status_code=status)
    assert _classify_page_failure(page, soup) == expected


def test_classify_page_failure_without_status_falls_back_to_body():
    # a response object carrying no status_code degrades to the text checks
    body = b"<html><body>Page not found.</body></html>"
    assert _classify_page_failure(None, bs(body, "lxml")) == "Page not found error"


def test_html_404_raises_page_not_found_without_body_text(monkeypatch):
    # a real 404 must be caught by status code alone, even when the body
    # carries no "Page not found." marker (#74). The private scraper raises
    # PageNotFoundError; the public wrapper swallows it to an empty DF.
    resp = SimpleNamespace(content=b"<html><body>no marker</body></html>", status_code=404)
    monkeypatch.setattr(cbbpy_utils.r, "get", lambda url, *a, **k: resp)
    monkeypatch.setattr(cbbpy_utils.time, "sleep", lambda *_: None)
    monkeypatch.setattr(cbbpy_utils, "ATTEMPTS", 1)

    assert ms.get_game_info("401999999", source="html").empty

    with pytest.raises(PageNotFoundError):
        cbbpy_utils._get_game_info("401999999", "mens", "html")


def test_api_404_raises_page_not_found(monkeypatch):
    # the API path bails on a 404 status without parsing the body (#74). The
    # transport raises PageNotFoundError; the public wrapper swallows it.
    resp = SimpleNamespace(content=b"", status_code=404)
    monkeypatch.setattr(cbbpy_utils.r, "get", lambda url, *a, **k: resp)
    monkeypatch.setattr(cbbpy_utils.time, "sleep", lambda *_: None)
    monkeypatch.setattr(cbbpy_utils, "ATTEMPTS", 1)

    assert ms.get_game_info("401999999", source="api").empty

    with pytest.raises(PageNotFoundError):
        espn_api._fetch_summary("401999999", "mens")


def test_html_404_info_skips_box_and_pbp(monkeypatch):
    # within one _get_game call, an info 404 must skip the boxscore and pbp
    # scrapes: only the info GET is issued and all three DFs come back empty
    resp = SimpleNamespace(content=b"<html><body>no marker</body></html>", status_code=404)
    calls = []
    monkeypatch.setattr(cbbpy_utils.r, "get", lambda url, *a, **k: calls.append(url) or resp)
    monkeypatch.setattr(cbbpy_utils.time, "sleep", lambda *_: None)
    monkeypatch.setattr(cbbpy_utils, "ATTEMPTS", 1)

    info, box, pbp = cbbpy_utils._get_game(
        "401999999", "mens", True, True, True, source="html"
    )

    assert len(calls) == 1
    assert info.empty and box.empty and pbp.empty


def test_api_waf_challenge_logged_as_such(monkeypatch, caplog):
    # an empty 202 WAF challenge must be named in the log rather than surfacing
    # as an opaque JSON decode error, so it is distinguishable from a 404 (#74)
    resp = SimpleNamespace(content=b"", status_code=202)
    monkeypatch.setattr(cbbpy_utils.r, "get", lambda url, *a, **k: resp)
    monkeypatch.setattr(cbbpy_utils.time, "sleep", lambda *_: None)
    monkeypatch.setattr(cbbpy_utils, "ATTEMPTS", 1)

    with caplog.at_level(logging.ERROR, logger="CBBpy"):
        df = ms.get_game_info("401999999", source="api")

    # a transient WAF block is not a dead game: it must not raise
    with caplog.at_level(logging.ERROR, logger="CBBpy"):
        assert espn_api._fetch_summary("401999999", "mens") is None

    assert df.empty
    assert "WAF challenge" in caplog.text
    assert "JSONDecodeError" not in caplog.text


# --- retryable vs non-retryable error classification (#73) ------------------
# A deterministic parse error against a payload that was successfully obtained
# must not burn all ATTEMPTS retries with sleeps; only failures before the
# payload was obtained (network / WAF challenge / page-not-found) retry.


def _raise_key_error(*args, **kwargs):
    raise KeyError("forced parse error")


def test_html_parse_error_fails_fast(offline_espn, monkeypatch):
    # a valid page is served, but the downstream helper raises → a deterministic
    # parse error must return empty after a SINGLE fetch, with no retry sleeps
    calls = []
    sleeps = []
    fixture_get = cbbpy_utils.r.get
    monkeypatch.setattr(
        cbbpy_utils.r, "get", lambda url, *a, **k: calls.append(url) or fixture_get(url, *a, **k)
    )
    monkeypatch.setattr(cbbpy_utils.time, "sleep", sleeps.append)
    monkeypatch.setattr(cbbpy_utils, "ATTEMPTS", 5)
    monkeypatch.setattr(cbbpy_utils, "_get_game_boxscore_helper", _raise_key_error)

    df = ms.get_game_boxscore(recorded_games("mens")[0], source="html")

    assert df.empty
    assert len(calls) == 1
    assert sleeps == []


def test_html_network_error_retries(monkeypatch):
    # a network error happens before any payload is obtained → keep retrying the
    # full ATTEMPTS budget, then return empty
    calls = []

    def boom(url, *a, **k):
        calls.append(url)
        raise ConnectionError("simulated network failure")

    monkeypatch.setattr(cbbpy_utils.r, "get", boom)
    monkeypatch.setattr(cbbpy_utils.time, "sleep", lambda *_: None)
    monkeypatch.setattr(cbbpy_utils, "ATTEMPTS", 4)

    df = ms.get_game_info("401999999", source="html")

    assert df.empty
    assert len(calls) == 4


def test_api_missing_header_fails_fast(monkeypatch):
    # the summary JSON parsed cleanly but carries no "header": a deterministic bad
    # payload → single fetch, return None / empty, no retries
    calls = []

    def fake_get(url, *a, **k):
        calls.append(url)
        return FakeResponse(b'{"note": "no header here"}', status_code=200)

    monkeypatch.setattr(cbbpy_utils.r, "get", fake_get)
    monkeypatch.setattr(espn_api.time, "sleep", lambda *_: None)
    monkeypatch.setattr(cbbpy_utils, "ATTEMPTS", 5)

    assert espn_api._fetch_summary("401999999", "mens") is None
    assert len(calls) == 1

    calls.clear()
    assert ms.get_game_info("401999999", source="api").empty
    assert len(calls) == 1


def test_api_waf_challenge_retries(monkeypatch):
    # an empty 202 WAF challenge is obtained before any payload → keep retrying
    # the full ATTEMPTS budget, then return None
    calls = []

    def fake_get(url, *a, **k):
        calls.append(url)
        return FakeResponse(b"", status_code=202)

    monkeypatch.setattr(cbbpy_utils.r, "get", fake_get)
    monkeypatch.setattr(espn_api.time, "sleep", lambda *_: None)
    monkeypatch.setattr(cbbpy_utils, "ATTEMPTS", 4)

    assert espn_api._fetch_summary("401999999", "mens") is None
    assert len(calls) == 4


def test_api_game_ids_parse_error_fails_fast(monkeypatch):
    # the scoreboard JSON parsed cleanly but an event lacks "id" → deterministic
    # parse error, single fetch, empty list, no retries
    calls = []

    def fake_get(url, *a, **k):
        calls.append(url)
        return FakeResponse(b'{"events": [{"no_id": 1}]}', status_code=200)

    monkeypatch.setattr(cbbpy_utils.r, "get", fake_get)
    monkeypatch.setattr(espn_api.time, "sleep", lambda *_: None)
    monkeypatch.setattr(cbbpy_utils, "ATTEMPTS", 5)

    ids = ms.get_game_ids("2021-04-03", source="api")

    assert ids == []
    assert len(calls) == 1


def test_import_does_no_logging_io():
    # importing cbbpy must not create the log dir/file (#78); the handler
    # opens lazily on first emit. Run in a subprocess because cbbpy is
    # already imported (and may have logged) in the pytest process.
    script = textwrap.dedent(
        """
        import os, tempfile
        from cbbpy.utils import cbbpy_utils as cu

        # nothing opened at import time
        assert cu.file_handler.stream is None

        # first emitted record creates the dir and file
        tmp = tempfile.mkdtemp()
        cu.log_dir = os.path.join(tmp, "logs")
        cu.file_handler.baseFilename = os.path.join(cu.log_dir, "CBBpy.log")
        cu._log.error("test")
        assert os.path.isfile(cu.file_handler.baseFilename)
        """
    )
    subprocess.run([sys.executable, "-c", script], check=True)


def test_fuzzy_match_and_fallback_warn():
    # no fixtures needed: reads the bundled team-map CSVs only (#78)
    with pytest.warns(CBBpyWarning, match="No exact match for 'Yukon'"):
        assert _get_id_from_team("Yukon", 2025, "mens") == (41, "UConn")
    latest = int(_get_team_map("mens").season.max())
    with pytest.warns(CBBpyWarning, match="Falling back"):
        _get_id_from_team("UConn", latest + 1, "mens")


def test_missing_fixture_fails_loudly(offline_espn):
    # an unrecorded URL must register as a miss instead of silently retrying;
    # the scraper still returns its empty-DataFrame fallback
    df = ms.get_game_info("999999999")
    assert df.empty
    assert any("999999999" in url for url in offline_espn.misses)
    # clear so the fixture's teardown assertion (which guards real tests
    # against unrecorded URLs) doesn't fail this deliberate miss
    offline_espn.misses.clear()


# --- politeness throttle (#79) and request timeout (#70) --------------------


def test_throttle_sleeps_before_each_game(offline_espn, monkeypatch):
    # the offline fixture no-ops time.sleep; re-patch to record calls instead.
    # With ATTEMPTS=1 and no fetch failures, the throttle is the only sleeper.
    sleeps = []
    monkeypatch.setattr(cbbpy_utils.time, "sleep", sleeps.append)
    d = SCOREBOARD_DATES["mens"]
    ms.get_games_range(d, d, box=False, pbp=False, throttle=0.5)
    n_games = len(ms.get_game_ids(d))
    assert len(sleeps) == n_games
    assert all(0.25 <= s <= 0.75 for s in sleeps)


def test_throttle_zero_disables_delay(offline_espn, monkeypatch):
    sleeps = []
    monkeypatch.setattr(cbbpy_utils.time, "sleep", sleeps.append)
    d = SCOREBOARD_DATES["mens"]
    ms.get_games_range(d, d, box=False, pbp=False, throttle=0)
    assert sleeps == []


def test_every_request_passes_timeout(offline_espn, monkeypatch):
    # every GET across both transports must carry timeout=REQUEST_TIMEOUT (#70)
    timeouts = []
    fixture_get = cbbpy_utils.r.get

    def spy(url, *args, **kwargs):
        timeouts.append(kwargs.get("timeout"))
        return fixture_get(url, *args, **kwargs)

    monkeypatch.setattr(cbbpy_utils.r, "get", spy)
    d = SCOREBOARD_DATES["mens"]
    for source in SOURCES:
        ms.get_games_range(d, d, source=source, throttle=0)
    ms.get_player_info(PLAYERS["mens"][0])
    ms.get_team_schedule("Pacific", 2017)
    assert timeouts
    assert all(t == cbbpy_utils.REQUEST_TIMEOUT for t in timeouts)


def test_timeout_error_is_retried(offline_espn, monkeypatch):
    # a timed-out request must raise into the retry loop and succeed on the
    # next attempt, not fail permanently (#70)
    fixture_get = cbbpy_utils.r.get
    calls = []

    def flaky(url, *args, **kwargs):
        calls.append(url)
        if len(calls) == 1:
            raise TimeoutError("simulated hung connection")
        return fixture_get(url, *args, **kwargs)

    monkeypatch.setattr(cbbpy_utils.r, "get", flaky)
    monkeypatch.setattr(cbbpy_utils, "ATTEMPTS", 2)
    df = ms.get_game_info(recorded_games("mens")[0])
    assert not df.empty
    assert len(calls) == 2


def test_deprecated_columns_removed_on_schedule(offline_espn):
    # DEPRECATED_COLUMNS is the source of truth for what's on the way out. Before
    # the removal version this asserts the replacement exists; once __version__
    # reaches it, the test flips and fails until the old column is gone, so the
    # removal can't be forgotten.
    current = tuple(int(p) for p in cbbpy.__version__.split(".")[:2])
    game = recorded_games("mens")[0]
    emitted = set(ms.get_game_info(game).columns) | set(ms.get_game_pbp(game).columns)
    emitted |= set(ws.get_game_pbp(recorded_games("womens")[0]).columns)

    for col, (replacement, removal) in DEPRECATED_COLUMNS.items():
        removal_version = tuple(int(p) for p in removal.split(".")[:2])
        if current < removal_version:
            assert replacement in emitted, (
                f"{col} is deprecated in favor of {replacement}, which isn't emitted"
            )
        else:
            assert col not in emitted, (
                f"{col} was slated for removal in {removal} (current "
                f"{cbbpy.__version__}) — drop it and use {replacement}"
            )
