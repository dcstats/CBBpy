import pytest
import pandas as pd
from src.cbbpy.cbbpy_utils import InvalidDateRangeError
from src.cbbpy import mens_scraper as ms, womens_scraper as ws


def converter(value):
    return '' if pd.isna(value) else str(value)


dtypes = {
    'game_id': str,
    'home_id': str,
    'away_id': str,
    'tournament': converter,
    'tv_network': converter,
    'play_team': converter,
    'shooter': converter,
    'assist_player': converter,
}


def load_expected_dataframe(file_path):
    return pd.read_csv(file_path, converters=dtypes)


def test_mens_info():
    game_id = "401372396"
    mens_expected_df = load_expected_dataframe("expected_data/mens_game_info.csv")
    mens_result_df = ms.get_game_info(game_id)
    pd.testing.assert_frame_equal(mens_result_df, mens_expected_df)


# def test_womens_info():
#     game_id = "123456"
#     womens_expected_df = load_expected_dataframe("expected_data/womens_game_info.csv")
#     womens_result_df = ws.get_game_info(game_id)
#     pd.testing.assert_frame_equal(womens_result_df, womens_expected_df)


def test_mens_boxscore():
    game_id = "401372396"
    mens_expected_df = load_expected_dataframe("expected_data/mens_game_boxscore.csv")
    mens_result_df = ms.get_game_boxscore(game_id)
    pd.testing.assert_frame_equal(mens_result_df, mens_expected_df)


# def test_womens_boxscore():
#     game_id = "123456"
#     womens_expected_df = load_expected_dataframe("expected_data/womens_game_boxscore.csv")
#     womens_result_df = ws.get_game_boxscore(game_id)
#     pd.testing.assert_frame_equal(womens_result_df, womens_expected_df)


def test_mens_pbp():
    game_id = "401372396"
    mens_expected_df = load_expected_dataframe("expected_data/mens_game_pbp.csv")
    mens_result_df = ms.get_game_pbp(game_id)
    pd.testing.assert_frame_equal(mens_result_df, mens_expected_df)


# def test_womens_pbp():
#     game_id = "123456"
#     womens_expected_df = load_expected_dataframe("expected_data/mens_game_pbp.csv")
#     womens_result_df = ws.get_game_pbp(game_id)
#     pd.testing.assert_frame_equal(womens_result_df, womens_expected_df)


# def test_mens_range():
#     start_date = "2022-01-01"
#     end_date = "2022-01-31"
#     expected_info_df = load_expected_dataframe("expected_data/mens_game_info.csv")
#     expected_boxscore_df = load_expected_dataframe("expected_data/mens_game_boxscore.csv")
#     expected_pbp_df = load_expected_dataframe("expected_data/mens_game_pbp.csv")
#     result_info_df, result_boxscore_df, result_pbp_df = ms.get_games_range(start_date, end_date)
#     pd.testing.assert_frame_equal(result_info_df, expected_info_df)
#     pd.testing.assert_frame_equal(result_boxscore_df, expected_boxscore_df)
#     pd.testing.assert_frame_equal(result_pbp_df, expected_pbp_df)


# def test_womens_range():
#     start_date = "2022-01-01"
#     end_date = "2022-01-31"
#     expected_info_df = load_expected_dataframe("expected_data/mens_game_info.csv")
#     expected_boxscore_df = load_expected_dataframe("expected_data/mens_game_boxscore.csv")
#     expected_pbp_df = load_expected_dataframe("expected_data/mens_game_pbp.csv")
#     result_info_df, result_boxscore_df, result_pbp_df = ws.get_games_range(start_date, end_date)
#     pd.testing.assert_frame_equal(result_info_df, expected_info_df)
#     pd.testing.assert_frame_equal(result_boxscore_df, expected_boxscore_df)
#     pd.testing.assert_frame_equal(result_pbp_df, expected_pbp_df)


def test_mens_season():
    future_season = 2030
    with pytest.raises(InvalidDateRangeError, match="The start date must not be in the future."):
        ms.get_games_season(future_season)


def test_womens_season():
    future_season = 2030
    with pytest.raises(InvalidDateRangeError, match="The start date must not be in the future."):
        ws.get_games_season(future_season)