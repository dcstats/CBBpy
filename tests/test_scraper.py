import pytest
import pandas as pd
from pathlib import Path
from utils.cbbpy_utils import InvalidDateRangeError
from cbbpy import mens_scraper as ms, womens_scraper as ws


M_GAMES = [
    400843495,
    401082698,
    400517877,
    400919646,
    400990907,
    400498476,
    401581583,
    400871140,
    401597830,
    400989020
]
W_GAMES = [
    400842484,
    401276220,
    401166919,
    400905162,
    401487896,
    401486000,
    400993766,
    303442739,
    401091038,
    401387091
]
M_SCHEDULE = [
    (2014, "UC Davis"),
    (2019, "Minnesota"),
    (2008, "Saint Peter's"),
    (2014, "Air Force"),
    (2023, "Eastern Michigan"),
    (2010, "Alabama A&M"),
    (2013, "Western Carolina"),
    (2007, "Northern Iowa"),
    (2015, "Alcorn State"),
    (2017, "Pacific")
]
W_SCHEDULE = [
    (2019, "Detroit Mercy"),
    (2014, "TCU"),
    (2020, "Iowa State"),
    (2013, "Arizona"),
    (2016, "Sam Houston"),
    (2013, "Troy"),
    (2019, "Loyola Marymount"),
    (2021, "Illinois"),
    (2016, "Morehead State"),
    (2021, "Cal Poly")
]
DATA_PATH = Path(__file__).parent / 'expected_data'


def converter(value):
    return '' if pd.isna(value) else str(value)


dtypes = {
    'game_id': str,
    'home_id': str,
    'away_id': str,
    'opponent_id': str,
    'starter': lambda x: x.lower() == 'true',
    'tournament': converter,
    'tv_network': converter,
    'play_team': converter,
    'shooter': converter,
    'assist_player': converter,
    'position': converter,
    'away_record': converter,
    'play_desc': converter,
    'play_type': converter,
    'game_loc': converter,
    'arena': converter,
    'referee_1': converter,
    'referee_2': converter,
    'referee_3': converter,
    'game_result': converter,
}


def load_expected_dataframe(file_path):
    return pd.read_csv(file_path, converters=dtypes)


def test_mens_info():
    for g in M_GAMES:
        mens_expected_df = load_expected_dataframe(DATA_PATH / f"mens_game_info_{g}.csv")
        mens_result_df = ms.get_game_info(str(g))
        pd.testing.assert_frame_equal(mens_result_df, mens_expected_df)


def test_womens_info():
    for g in W_GAMES:
        womens_expected_df = load_expected_dataframe(DATA_PATH / f"womens_game_info_{g}.csv")
        womens_result_df = ws.get_game_info(str(g))
        pd.testing.assert_frame_equal(womens_result_df, womens_expected_df)


def test_mens_boxscore():
    for g in M_GAMES:
        mens_expected_df = load_expected_dataframe(DATA_PATH / f"mens_game_boxscore_{g}.csv")
        mens_result_df = ms.get_game_boxscore(str(g))
        pd.testing.assert_frame_equal(mens_result_df, mens_expected_df)


def test_womens_boxscore():
    for g in W_GAMES:
        womens_expected_df = load_expected_dataframe(DATA_PATH / f"womens_game_boxscore_{g}.csv")
        womens_result_df = ws.get_game_boxscore(str(g))
        pd.testing.assert_frame_equal(womens_result_df, womens_expected_df)


def test_mens_pbp():
    for g in M_GAMES:
        mens_expected_df = load_expected_dataframe(DATA_PATH / f"mens_game_pbp_{g}.csv")
        mens_result_df = ms.get_game_pbp(str(g))
        pd.testing.assert_frame_equal(mens_result_df, mens_expected_df)


def test_womens_pbp():
    for g in W_GAMES:
        womens_expected_df = load_expected_dataframe(DATA_PATH / f"womens_game_pbp_{g}.csv")
        womens_result_df = ws.get_game_pbp(str(g))
        pd.testing.assert_frame_equal(womens_result_df, womens_expected_df)


# TODO
def test_mens_player():
    pass


# TODO
def test_womens_player():
    pass


def test_mens_schedule():
    mens_result_df = pd.DataFrame()
    mens_expected_df = load_expected_dataframe(DATA_PATH / f"mens_schedule.csv")

    for x in M_SCHEDULE:
        sn, nm = x
        d = ms.get_team_schedule(nm, sn)
        mens_result_df = pd.concat([mens_result_df, d])

    mens_result_df.reset_index(inplace=True, drop=True)

    pd.testing.assert_frame_equal(mens_result_df, mens_expected_df)


def test_womens_schedule():
    womens_result_df = pd.DataFrame()
    womens_expected_df = load_expected_dataframe(DATA_PATH / f"womens_schedule.csv")

    for x in W_SCHEDULE:
        sn, nm = x
        d = ws.get_team_schedule(nm, sn)
        womens_result_df = pd.concat([womens_result_df, d])

    womens_result_df.reset_index(inplace=True, drop=True)

    pd.testing.assert_frame_equal(womens_result_df, womens_expected_df)


def test_mens_range():
    start_date = "2022-03-01"
    end_date = "2022-03-31"
    expected_info_df = load_expected_dataframe(DATA_PATH / "mens_game_info_range.csv")
    expected_boxscore_df = load_expected_dataframe(DATA_PATH / "mens_game_boxscore_range.csv")
    expected_pbp_df = load_expected_dataframe(DATA_PATH / "mens_game_pbp_range.csv")
    result_info_df, result_boxscore_df, result_pbp_df = ms.get_games_range(start_date, end_date)
    pd.testing.assert_frame_equal(result_info_df, expected_info_df)
    pd.testing.assert_frame_equal(result_boxscore_df, expected_boxscore_df)
    pd.testing.assert_frame_equal(result_pbp_df, expected_pbp_df)


def test_womens_range():
    start_date = "2022-03-01"
    end_date = "2022-03-31"
    expected_info_df = load_expected_dataframe(DATA_PATH / "womens_game_info_range.csv")
    expected_boxscore_df = load_expected_dataframe(DATA_PATH / "womens_game_boxscore_range.csv")
    expected_pbp_df = load_expected_dataframe(DATA_PATH / "womens_game_pbp_range.csv")
    result_info_df, result_boxscore_df, result_pbp_df = ws.get_games_range(start_date, end_date)
    pd.testing.assert_frame_equal(result_info_df, expected_info_df)
    pd.testing.assert_frame_equal(result_boxscore_df, expected_boxscore_df)
    pd.testing.assert_frame_equal(result_pbp_df, expected_pbp_df)


def test_mens_season():
    future_season = 2030
    with pytest.raises(InvalidDateRangeError, match="The start date must not be in the future."):
        ms.get_games_season(future_season)


def test_womens_season():
    future_season = 2030
    with pytest.raises(InvalidDateRangeError, match="The start date must not be in the future."):
        ws.get_games_season(future_season)