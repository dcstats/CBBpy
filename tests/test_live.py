"""Live tests against real ESPN — drift-robust invariants only.

These verify the scraper still works against ESPN as it exists today. They
deliberately assert only things that cannot legitimately change: schemas
(required columns as subsets, so new fields pass silently), immutable
historical facts (final scores and team IDs of long-finished games),
row counts within a tolerance, and game-id completeness. Display names,
player bios, timezones, dtypes, and row order are NOT compared — ESPN
changes those routinely and it doesn't mean the scraper is broken.

Run with: pytest -m live
"""

from pathlib import Path

import pandas as pd
import pytest

from cbbpy import mens_scraper as ms, womens_scraper as ws

pytestmark = pytest.mark.live

SCRAPERS = {"mens": ms, "womens": ws}
GENDERS = ["mens", "womens"]
DATA_PATH = Path(__file__).parent / "expected_data"


# --- schemas: required columns; issubset() so new scraper fields pass silently ---

INFO_REQUIRED_COLS = {
    "game_id", "game_status", "home_team", "home_id", "home_rank", "home_record",
    "home_score", "away_team", "away_id", "away_rank", "away_record", "away_score",
    "home_point_spread", "home_win", "num_ots", "is_conference", "is_neutral",
    "is_postseason", "tournament", "game_datetime", "game_day", "game_time", "game_loc", "arena",
    "arena_capacity", "attendance", "tv_network", "referee_1", "referee_2", "referee_3",
    # odds: present in the schema but volatile/absent historically, so only their
    # presence is asserted, never their values.
    "over_under", "home_moneyline", "away_moneyline",
}
BOXSCORE_REQUIRED_COLS = {
    "game_id", "team", "player", "player_id", "position", "starter", "min",
    "fgm", "fga", "2pm", "2pa", "3pm", "3pa", "ftm", "fta", "pts", "reb",
    "ast", "to", "stl", "blk", "oreb", "dreb", "pf",
}
PBP_REQUIRED_COLS = {
    "game_id", "home_team", "away_team", "play_desc", "home_score", "away_score",
    "secs_left_reg", "play_team", "play_type", "play_type_id", "shooting_play",
    "scoring_play", "is_three", "player_name", "is_assisted", "assist_player",
    "player_id", "assist_player_id",
    "shot_x", "shot_y",
    # win probability: presence-only (values are model-driven and absent for
    # older games).
    "home_win_prob",
}
PLAYER_REQUIRED_COLS = {
    "player_id", "first_name", "last_name", "jersey_number", "pos", "status",
    "team", "experience", "height", "weight", "birthplace", "date_of_birth",
}
SCHEDULE_REQUIRED_COLS = {
    "team", "team_id", "season", "game_id", "game_datetime", "game_day", "game_time", "opponent",
    "opponent_id", "season_type", "game_status", "tv_network", "game_result",
}

# Columns of schedule output that ESPN does not retroactively change for past
# seasons. Excluded as known drift: team/opponent display names (rebrands),
# game_time (timezone/format), tv_network (network renames).
SCHEDULE_STABLE_COLS = [
    "team_id", "season", "game_id", "game_day", "opponent_id",
    "season_type", "game_status", "game_result",
]


# --- immutable historical facts (sourced from long-finished games) ---
# away_id None = not checked: non-D1 teams get slug-style ids that ESPN
# regenerates from the school's current name (e.g. Union College KY became
# nd-union-commonwealth-ky-bulldogs after the school renamed)
# pbp_rows: baseline pbp row count, compared ±5%
# pbp_final: max running score in the pbp feed, when ESPN's archived feed is
# incomplete and never reaches the true final score

M_GAME_FACTS = {
    "400843495": dict(home_id="2636", away_id="98", home=71, away=83, num_ots=0,
                      home_win=False, is_postseason=False,
                      game_day="February 04, 2016", pbp_rows=341),
    "401082698": dict(home_id="52", away_id="5", home=81, away=63, num_ots=0,
                      home_win=True, is_postseason=False,
                      game_day="November 22, 2018", pbp_rows=354),
    "400517877": dict(home_id="12", away_id="227", home=87, away=59, num_ots=0,
                      home_win=True, is_postseason=False,
                      game_day="November 19, 2013", pbp_rows=331),
    "401581583": dict(home_id="299", away_id="309", home=82, away=92, num_ots=0,
                      home_win=False, is_postseason=False,
                      game_day="November 22, 2023", pbp_rows=308),
    "400989020": dict(home_id="2617", away_id="2443", home=78, away=68, num_ots=0,
                      home_win=True, is_postseason=False,
                      game_day="January 10, 2018", pbp_rows=321),
}
W_GAME_FACTS = {
    "400842484": dict(home_id="259", away_id="116", home=59, away=42, num_ots=0,
                      home_win=True, is_postseason=False,
                      game_day="December 19, 2015", pbp_rows=327),
    # ESPN's archived pbp feed for this game ends at 66-54, two points shy of
    # the real 68-54 final (the closing basket was never logged)
    "401276220": dict(home_id="2520", away_id="315", home=68, away=54, num_ots=0,
                      home_win=True, is_postseason=False,
                      game_day="February 14, 2021", pbp_rows=362,
                      pbp_final=(66, 54)),
    # OT game
    "401486000": dict(home_id="2230", away_id="2217", home=75, away=82, num_ots=1,
                      home_win=False, is_postseason=False,
                      game_day="November 30, 2022", pbp_rows=376),
    # pre-2015 halves era
    "303442739": dict(home_id="2739", away_id="2546", home=74, away=40, num_ots=0,
                      home_win=True, is_postseason=False,
                      game_day="December 10, 2010", pbp_rows=307),
    "401387091": dict(home_id="2198", away_id=None, home=120, away=53, num_ots=0,
                      home_win=True, is_postseason=False,
                      game_day="November 22, 2021", pbp_rows=392),
}
GAME_FACTS = {"mens": M_GAME_FACTS, "womens": W_GAME_FACTS}

PLAYERS = {
    "mens": ["4433093", "4431684", "5177650", "53043", "3924900"],
    "womens": ["14670", "15353", "4400272", "5174546", "17762"],
}

TEAM_SCHEDULES = {
    "mens": [
        (2014, "UC Davis"), (2019, "Minnesota"), (2008, "Saint Peter's"),
        (2014, "Air Force"), (2023, "Eastern Michigan"), (2010, "Alabama A&M"),
        (2013, "Western Carolina"), (2007, "Northern Iowa"), (2015, "Alcorn State"),
        (2017, "Pacific"),
    ],
    "womens": [
        (2019, "Detroit Mercy"), (2014, "TCU"), (2020, "Iowa State"),
        (2013, "Arizona"), (2016, "Sam Houston"), (2013, "Troy"),
        (2019, "Loyola Marymount"), (2021, "Illinois"), (2016, "Morehead State"),
        (2021, "Cal Poly"),
    ],
}
CONF_SCHEDULES = {
    "mens": [
        (2006, "Division I Independents"), (2021, "Patriot League"),
        (2020, "Sun Belt Conference"), (2017, "Pac-12 Conference"),
        (2016, "Big Sky Conference"),
    ],
    "womens": [
        (2024, "Northeast Conference"), (2013, "Big Ten Conference"),
        (2019, "Big Ten Conference"), (2022, "Southeastern Conference"),
        (2019, "West Coast Conference"),
    ],
}
CONF_SEASONS = {
    "mens": [("acc", 2017), ("a10", 2022), ("maac", 2019)],
    "womens": [("big east", 2018), ("mountain west", 2021), ("patriot", 2023)],
}

# game ids on fixed historical dates (immutable for long-past dates)
GAME_IDS_BY_DATE = {
    "mens": ("2021-04-03", {"401310866", "401310867"}),
    "womens": ("2023-04-02", {"401528028"}),
}

RANGE_START, RANGE_END = "2022-03-01", "2022-03-31"
ROW_COUNT_TOLERANCE = 0.05


def assert_required_cols(df, required):
    missing = required - set(df.columns)
    assert not missing, f"required columns missing from output: {sorted(missing)}"


def assert_row_count_close(n, baseline):
    lo, hi = baseline * (1 - ROW_COUNT_TOLERANCE), baseline * (1 + ROW_COUNT_TOLERANCE)
    assert lo <= n <= hi, f"row count {n} outside ±5% of baseline {baseline}"


def stable_schedule_projection(df):
    # only completed games: canceled/postponed games get their result and
    # status representation reshuffled by ESPN (e.g. NaN -> "N/A")
    return (
        df[df.game_status == "Final"][SCHEDULE_STABLE_COLS]
        .astype(str)
        .sort_values(["team_id", "game_id"])
        .reset_index(drop=True)
    )


SOURCES = ["html", "api"]


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("gender", GENDERS)
def test_live_game_info(gender, source):
    sc = SCRAPERS[gender]
    for gid, f in GAME_FACTS[gender].items():
        df = sc.get_game_info(gid, source=source)
        assert len(df) == 1, f"{gid}: expected exactly one info row"
        assert_required_cols(df, INFO_REQUIRED_COLS)
        row = df.iloc[0]
        assert str(row.game_id) == gid
        assert str(row.home_id) == f["home_id"]
        if f["away_id"] is not None:
            assert str(row.away_id) == f["away_id"]
        assert int(row.home_score) == f["home"] and int(row.away_score) == f["away"]
        assert int(row.num_ots) == f["num_ots"]
        assert bool(row.home_win) == f["home_win"]
        assert bool(row.is_postseason) == f["is_postseason"]
        assert row.game_day == f["game_day"]


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("gender", GENDERS)
def test_live_game_boxscore(gender, source):
    sc = SCRAPERS[gender]
    for gid, f in GAME_FACTS[gender].items():
        df = sc.get_game_boxscore(gid, source=source)
        assert_required_cols(df, BOXSCORE_REQUIRED_COLS)
        assert (df.game_id.astype(str) == gid).all()
        # cross-validate team totals against the known final scores
        totals = df[df.player == "TEAM"].pts.astype(int).tolist()
        assert sorted(totals) == sorted([f["home"], f["away"]]), (
            f"{gid}: boxscore team totals {totals} != known final "
            f"{f['home']}-{f['away']}"
        )
        # note: player rows are NOT summed against the totals — ESPN's data
        # for older games doesn't attribute every point to a player


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("gender", GENDERS)
def test_live_game_pbp(gender, source):
    sc = SCRAPERS[gender]
    for gid, f in GAME_FACTS[gender].items():
        df = sc.get_game_pbp(gid, source=source)
        assert_required_cols(df, PBP_REQUIRED_COLS)
        # period columns depend on era: halves for mens and pre-2015 womens
        assert ("half" in df.columns and "secs_left_half" in df.columns) or (
            "quarter" in df.columns and "secs_left_qt" in df.columns
        ), f"{gid}: no period columns found"
        assert (df.game_id.astype(str) == gid).all()
        # final running score of a finished game is immutable (pbp_final pins the
        # archived feed's truncated ending, which both sources share)
        h, a = f.get("pbp_final", (f["home"], f["away"]))
        assert int(df.home_score.max()) == h and int(df.away_score.max()) == a
        assert_row_count_close(len(df), f["pbp_rows"])
        # derived fields (is_three, player_name, assist_player) are intentionally
        # not value-checked: they come from fragile string parsing of ESPN text


@pytest.mark.parametrize("gender", GENDERS)
def test_live_player_info(gender):
    sc = SCRAPERS[gender]
    df = pd.concat([sc.get_player_info(p) for p in PLAYERS[gender]], ignore_index=True)
    assert_required_cols(df, PLAYER_REQUIRED_COLS)
    # names/bios are not compared: ESPN reformats them routinely
    assert set(df.player_id.astype(str)) == set(PLAYERS[gender])


@pytest.mark.parametrize("gender", GENDERS)
def test_live_team_schedule(gender):
    sc = SCRAPERS[gender]
    result = pd.concat(
        [sc.get_team_schedule(team, season) for season, team in TEAM_SCHEDULES[gender]],
        ignore_index=True,
    )
    assert_required_cols(result, SCHEDULE_REQUIRED_COLS)
    expected = pd.read_csv(DATA_PATH / f"{gender}_team_schedule.csv")
    pd.testing.assert_frame_equal(
        stable_schedule_projection(result),
        stable_schedule_projection(expected),
        check_dtype=False,
    )


@pytest.mark.parametrize("gender", GENDERS)
def test_live_conference_schedule(gender):
    sc = SCRAPERS[gender]
    result = pd.concat(
        [sc.get_conference_schedule(conf, season) for season, conf in CONF_SCHEDULES[gender]],
        ignore_index=True,
    )
    assert_required_cols(result, SCHEDULE_REQUIRED_COLS)
    expected = pd.read_csv(DATA_PATH / f"{gender}_conference_schedule.csv")
    pd.testing.assert_frame_equal(
        stable_schedule_projection(result),
        stable_schedule_projection(expected),
        check_dtype=False,
    )


@pytest.mark.parametrize("gender", GENDERS)
def test_live_game_ids(gender):
    sc = SCRAPERS[gender]
    date, expected_ids = GAME_IDS_BY_DATE[gender]
    assert set(map(str, sc.get_game_ids(date))) == expected_ids


@pytest.mark.parametrize("gender", GENDERS)
def test_live_games_range(gender):
    sc = SCRAPERS[gender]
    info, box, pbp = sc.get_games_range(RANGE_START, RANGE_END)
    assert_required_cols(info, INFO_REQUIRED_COLS)
    assert_required_cols(box, BOXSCORE_REQUIRED_COLS)
    assert_required_cols(pbp, PBP_REQUIRED_COLS)

    baseline = pd.read_csv(DATA_PATH / f"{gender}_game_info_range.csv")
    assert_row_count_close(len(info), len(baseline))
    # completeness: every game known at baseline time must still be returned;
    # superset (not equality) because ESPN occasionally adds games retroactively
    missing = set(baseline.game_id.astype(str)) - set(info.game_id.astype(str))
    assert not missing, f"games in baseline but not scraped: {sorted(missing)[:10]}"
    # every info game should also have boxscore and pbp data for >=95% of games
    assert_row_count_close(len(set(box.game_id.astype(str))), len(info))
    assert_row_count_close(len(set(pbp.game_id.astype(str))), len(info))


@pytest.mark.parametrize("gender", GENDERS)
def test_live_games_conference(gender):
    sc = SCRAPERS[gender]
    info = pd.DataFrame()
    box = pd.DataFrame()
    pbp = pd.DataFrame()
    for conf, season in CONF_SEASONS[gender]:
        i, b, p = sc.get_games_conference(conf, season)
        info = pd.concat([info, i], ignore_index=True)
        box = pd.concat([box, b], ignore_index=True)
        pbp = pd.concat([pbp, p], ignore_index=True)

    assert_required_cols(info, INFO_REQUIRED_COLS)
    assert_required_cols(box, BOXSCORE_REQUIRED_COLS)
    assert_required_cols(pbp, PBP_REQUIRED_COLS)

    baseline = pd.read_csv(DATA_PATH / f"{gender}_conference_info.csv")
    assert_row_count_close(len(info), len(baseline))
    missing = set(baseline.game_id.astype(str)) - set(info.game_id.astype(str))
    assert not missing, f"games in baseline but not scraped: {sorted(missing)[:10]}"
