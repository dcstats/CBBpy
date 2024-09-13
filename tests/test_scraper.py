import pandas as pd
import pytest
from datetime import datetime
from src.cbbpy.mens_scraper import get_game_info, get_game_boxscore, get_game_pbp, get_games_season
from src.cbbpy.cbbpy_utils import _get_games_range

def load_expected_dataframe(file_path):
    return pd.read_csv(file_path)

def test_get_game_info():
    game_id = "123456"
    expected_df = load_expected_dataframe("expected_data/mens_game_info.csv")
    result_df = get_game_info(game_id)
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_get_game_boxscore():
    game_id = "123456"
    expected_df = load_expected_dataframe("expected_data/mens_game_boxscore.csv")
    result_df = get_game_boxscore(game_id)
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_get_game_pbp():
    game_id = "123456"
    expected_df = load_expected_dataframe("expected_data/mens_game_pbp.csv")
    result_df = get_game_pbp(game_id)
    pd.testing.assert_frame_equal(result_df, expected_df)


def test_get_games_range():
    start_date = "2022-01-01"
    end_date = "2022-01-31"
    expected_info_df = load_expected_dataframe("expected_data/mens_game_info.csv")
    expected_boxscore_df = load_expected_dataframe("expected_data/mens_game_boxscore.csv")
    expected_pbp_df = load_expected_dataframe("expected_data/mens_game_pbp.csv")
    result_info_df, result_boxscore_df, result_pbp_df = _get_games_range(start_date, end_date, "mens", True, True, True)
    pd.testing.assert_frame_equal(result_info_df, expected_info_df)
    pd.testing.assert_frame_equal(result_boxscore_df, expected_boxscore_df)
    pd.testing.assert_frame_equal(result_pbp_df, expected_pbp_df)


def test_get_games_season_future():
    future_season = datetime.today().year + 1
    with pytest.raises(ValueError, match="Season has not ended yet"):
        get_games_season(future_season)
