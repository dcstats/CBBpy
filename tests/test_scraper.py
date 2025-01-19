import pytest
import pandas as pd
from pathlib import Path
from cbbpy.utils.cbbpy_utils import InvalidDateRangeError
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
M_TEAM_SCHEDULE = [
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
W_TEAM_SCHEDULE = [
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
M_CONF_SCHEDULE = [
    (2006, "Division I Independents"),
    (2021, "Patriot League"),
    (2020, "Sun Belt Conference"),
    (2017, "Pac-12 Conference"),
    (2016, "Big Sky Conference"),
]
W_CONF_SCHEDULE = [
    (2024, "Northeast Conference"),
    (2013, "Big Ten Conference"),
    (2019, "Big Ten Conference"),
    (2022, "Southeastern Conference"),
    (2019, "West Coast Conference"),
]
M_CONF_SEASONS = [
    ('acc', 2017),
    ('a10', 2022),
    ('maac', 2019),
]
W_CONF_SEASONS = [
    ('big east', 2018),
    ('mountain west', 2021),
    ('patriot', 2023),
]
DATA_PATH = Path(__file__).parent / 'expected_data'


def converter(value):
    return '' if pd.isna(value) else str(value)


dtypes = {
    'game_id': str,
    'home_id': str,
    'away_id': str,
    'opponent_id': str,
    'player_id': str,
    'jersey_number': str,
    'pos': str,
    'attendance': float,
    'capacity': float,
    'starter': lambda x: x.lower() == 'true',
    'tournament': converter,
    'tv_network': converter,
    'play_team': converter,
    'shooter': converter,
    'assist_player': converter,
    'position': converter,
    'home_record': converter,
    'away_record': converter,
    'play_desc': converter,
    'play_type': converter,
    'game_loc': converter,
    'arena': converter,
    'referee_1': converter,
    'referee_2': converter,
    'referee_3': converter,
    'game_result': converter,
    'home_point_spread': converter,
    'height': converter,
    'weight': converter,
    'birthplace': converter,
    'date_of_birth': converter,
}


def load_expected_dataframe(file_path):
    return pd.read_csv(file_path, converters=dtypes)


@pytest.mark.parametrize("func, ex_path, data", [
    (ms.get_game_info, "mens_game_info", M_GAMES),
    (ms.get_game_boxscore, "mens_game_boxscore", M_GAMES),
    (ms.get_game_pbp, "mens_game_pbp", M_GAMES),
    (ws.get_game_info, "womens_game_info", W_GAMES),
    (ws.get_game_boxscore, "womens_game_boxscore", W_GAMES),
    (ws.get_game_pbp, "womens_game_pbp", W_GAMES),
])
def test_game_data(func, ex_path, data):
    expected_df = load_expected_dataframe(DATA_PATH / f"{ex_path}.csv")
    result_df = pd.DataFrame()

    for g in data:
        d = func(str(g))
        result_df = pd.concat([result_df, d], ignore_index=True)
    
    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("func, game_type", [
    (ms.get_player_info, "mens"),
    (ws.get_player_info, "womens"),
])
def test_player(func, game_type):
    expected_df = load_expected_dataframe(DATA_PATH / f"{game_type}_players.csv")
    pl_ls = expected_df.player_id.tolist()

    result_df = pd.DataFrame()

    for pl in pl_ls:
        d = func(pl)
        result_df = pd.concat([result_df, d], ignore_index=True)

    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("func, ex_path, data", [
    (ms.get_team_schedule, "mens_team_schedule", M_TEAM_SCHEDULE),
    (ms.get_conference_schedule, "mens_conference_schedule", M_CONF_SCHEDULE),
    (ws.get_team_schedule, "womens_team_schedule", W_TEAM_SCHEDULE),
    (ws.get_conference_schedule, "womens_conference_schedule", W_CONF_SCHEDULE),
])
def test_schedule(func, ex_path, data):
    result_df = pd.DataFrame()
    expected_df = load_expected_dataframe(DATA_PATH / f"{ex_path}.csv")

    for x in data:
        sn, nm = x
        d = func(nm, sn)
        result_df = pd.concat([result_df, d], ignore_index=True)

    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.parametrize("func, game_type", [
    (ms.get_games_range, "mens"),
    (ws.get_games_range, "womens"),
])
def test_range(func, game_type):
    start_date = "2022-03-01"
    end_date = "2022-03-31"
    expected_info_df = load_expected_dataframe(DATA_PATH / f"{game_type}_game_info_range.csv")
    expected_boxscore_df = load_expected_dataframe(DATA_PATH / f"{game_type}_game_boxscore_range.csv")
    expected_pbp_df = load_expected_dataframe(DATA_PATH / f"{game_type}_game_pbp_range.csv")
    result_info_df, result_boxscore_df, result_pbp_df = func(start_date, end_date)
    pd.testing.assert_frame_equal(result_info_df, expected_info_df)
    pd.testing.assert_frame_equal(result_boxscore_df, expected_boxscore_df)
    pd.testing.assert_frame_equal(result_pbp_df, expected_pbp_df)


@pytest.mark.parametrize("func", [
    (ms.get_games_season),
    (ws.get_games_season),
])
def test_season(func):
    future_season = 3000
    with pytest.raises(InvalidDateRangeError, match="The start date must not be in the future."):
        func(future_season)


@pytest.mark.parametrize("func, game_type, data", [
    (ms.get_games_conference, "mens", M_CONF_SEASONS),
    (ws.get_games_conference, "womens", W_CONF_SEASONS),
])
def test_conference_games(func, game_type, data):
    expected_info_df = load_expected_dataframe(DATA_PATH / f"{game_type}_conference_info.csv")
    expected_boxscore_df = load_expected_dataframe(DATA_PATH / f"{game_type}_conference_boxscore.csv")
    expected_pbp_df = load_expected_dataframe(DATA_PATH / f"{game_type}_conference_pbp.csv")
    result_info = pd.DataFrame()
    result_box = pd.DataFrame()
    result_pbp = pd.DataFrame()
    
    for conf, season in data:
        info, box, pbp = func(conf, season)
        result_info = pd.concat([result_info, info], ignore_index=True)
        result_box = pd.concat([result_box, box], ignore_index=True)
        result_pbp = pd.concat([result_pbp, pbp], ignore_index=True)
        
    pd.testing.assert_frame_equal(result_info, expected_info_df)
    pd.testing.assert_frame_equal(result_box, expected_boxscore_df)
    pd.testing.assert_frame_equal(result_pbp, expected_pbp_df)