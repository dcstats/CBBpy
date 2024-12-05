"""
A tool to scrape data for NCAA D1 Women's college basketball games.

Author: Daniel Cowan
"""

from datetime import datetime
import pandas as pd
from typing import Union, Tuple
from utils.cbbpy_utils import (
    _get_game,
    _get_games_range,
    _get_games_season,
    _get_game_ids,
    _get_game_boxscore,
    _get_game_pbp,
    _get_game_info,
    _get_player_info,
    _get_teams_from_conference,
    _get_team_schedule,
    _get_conference_schedule,
    _get_games_team,
    _get_games_conference,
    _get_current_season,
)


def get_game(
    game_id: Union[str, int],
    info: bool = True,
    box: bool = True,
    pbp: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """A function that scrapes all game info (metadata, boxscore, play-by-play).

    Parameters:
        game_id (str | int): The game's ESPN game ID
        info (bool, optional): Whether the game metadata is to be scraped. Defaults to True.
        box (bool, optional): Whether the game boxscore is to be scraped. Defaults to True.
        pbp (bool, optional): Whether the game play-by-play is to be scraped. Defaults to True.

    Returns:
        a tuple containing

        - pd.DataFrame: The game's metadata.\n
        - pd.DataFrame: The game's boxscore (both teams combined).\n
        - pd.DataFrame: The game's play-by-play.
    """
    return _get_game(game_id, "womens", info, box, pbp)


def get_games_range(
    start_date: Union[str, datetime],
    end_date: Union[str, datetime],
    info: bool = True,
    box: bool = True,
    pbp: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """A function that scrapes a game information between a given range of dates.

    Parameters:
        start_date (str | datetime): The first day of games to scrape.
        end_date (str | datetime): The last day of games to scrape (inclusive).
        info (bool, optional): Whether the game metadata is to be scraped. Defaults to True.
        box (bool, optional): Whether the game boxscore is to be scraped. Defaults to True.
        pbp (bool, optional): Whether the game play-by-play is to be scraped. Defaults to True.

    Returns:
        a tuple containing

        - pd.DataFrame: The game's metadata.\n
        - pd.DataFrame: The game's boxscore (both teams combined).\n
        - pd.DataFrame: The game's play-by-play.
    """
    return _get_games_range(start_date, end_date, "womens", info, box, pbp)


def get_games_season(
    season: Union[str, int] = None,
    info: bool = True,
    box: bool = True,
    pbp: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Scrapes desired game information (metadata, boxscore, play-by-play) for every game of a given season.

    Parameters:
        season (str | int): The season to be scraped. 
            NOTE: season takes the form of the four-digit representation of the later year of the season. 
            So, as an example, the 2021-22 season is referred to by the integer 2022.
        info (bool, optional): Whether the game metadata is to be scraped. Defaults to True.
        box (bool, optional): Whether the game boxscore is to be scraped. Defaults to True.
        pbp (bool, optional): Whether the game play-by-play is to be scraped. Defaults to True.

    Returns:
        a tuple containing

        - pd.DataFrame: The game's metadata.\n
        - pd.DataFrame: The game's boxscore (both teams combined).\n
        - pd.DataFrame: The game's play-by-play.
    """
    if season is None:
        season = _get_current_season()
    return _get_games_season(season, "womens", info, box, pbp)


def get_games_team(
    team: str, 
    season: Union[str, int] = None, 
    info: bool = True, 
    box: bool = True, 
    pbp: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Scrapes desired game information (metadata, boxscore, play-by-play) for every game of a given team and season.

    Parameters:
        team (str): The team whose games will be scraped.
        season (str | int, optional): The season to be scraped. 
            NOTE: season takes the form of the four-digit representation of the later year of the season. 
            So, as an example, the 2021-22 season is referred to by the integer 2022.
        info (bool, optional): Whether the game metadata is to be scraped. Defaults to True.
        box (bool, optional): Whether the game boxscore is to be scraped. Defaults to True.
        pbp (bool, optional): Whether the game play-by-play is to be scraped. Defaults to True.

    Returns:
        a tuple containing

        - pd.DataFrame: The team's games metadata.\n
        - pd.DataFrame: The team's season boxscores (both teams combined).\n
        - pd.DataFrame: The team's season play-by-plays.
    """
    if season is None:
        season = _get_current_season()
    return _get_games_team(team, season, "womens", info, box, pbp)


def get_games_conference(
    conference: str,
    season: Union[str, int] = None,
    info: bool = True,
    box: bool = True,
    pbp: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Scrapes desired game information (metadata, boxscore, play-by-play) for every game for every team for a given conference and season.

    Parameters:
        conference (str): The conference whose teams will have their games scraped.
        season (str | int, optional): The season to be scraped. 
            NOTE: season takes the form of the four-digit representation of the later year of the season. 
            So, as an example, the 2021-22 season is referred to by the integer 2022.
        info (bool, optional): Whether the game metadata is to be scraped. Defaults to True.
        box (bool, optional): Whether the game boxscore is to be scraped. Defaults to True.
        pbp (bool, optional): Whether the game play-by-play is to be scraped. Defaults to True.

    Returns:
        a tuple containing

        - pd.DataFrame: The conference's teams' games metadata.\n
        - pd.DataFrame: The conference's teams' season boxscores (both teams combined).\n
        - pd.DataFrame: The conference's teams' season play-by-plays.
    """
    if season is None:
        season = _get_current_season()
    return _get_games_conference(conference, season, "womens", info, box, pbp)


def get_game_ids(date: Union[str, datetime]) -> list:
    """Scrapes all game IDs for a given date.

    Parameters:
        date (str | datetime): The date of the games to be scraped.

    Returns:
        list: The ESPN game IDs for each game played on the given date.
    """
    return _get_game_ids(date, "womens")


def get_game_boxscore(game_id: Union[str, int]) -> pd.DataFrame:
    """Scrapes each team's boxscore for a given game.

    Parameters:
        game_id (str | int): The game's ESPN game ID.

    Returns:
        pd.DataFrame: The boxscores of both teams, combined into one table.
    """
    return _get_game_boxscore(game_id, "womens")


def get_game_pbp(game_id: Union[str, int]) -> pd.DataFrame:
    """Scrapes a game's play-by-play data.

    Parameters:
        game_id (str | int): The game's ESPN game ID.

    Returns:
        pd.DataFrame: The game's play-by-play information, with a row for each play.
    """
    return _get_game_pbp(game_id, "womens")


def get_game_info(game_id: Union[str, int]) -> pd.DataFrame:
    """Scrapes game metadata from the ESPN game page.

    Args:
        game_id (str | int): The game's ESPN game ID.

    Returns:
        pd.DataFrame: The game's metadata scraped from the game page.
    """
    return _get_game_info(game_id, "womens")


def get_player_info(player_id: Union[str, int]) -> pd.DataFrame:
    """Scrapes player details from her bio page for a given player ID.

    Args:
        player_id (int | str): The player's ESPN player ID.

    Returns:
        pd.DataFrame: The given player's details.
    """
    return _get_player_info(player_id, "womens")


def get_teams_from_conference(conference: str, season: Union[str, int] = None) -> list:
    """Fetches the list of teams from the given conference during a given season.

    Args:
        conference (str): The conference to be fetched.
        season (str | int): The relevant season. Defaults to current season.
            NOTE: season takes the form of the four-digit representation of the later year of the season. 
            So, as an example, the 2021-22 season is referred to by the integer 2022.

    Returns:
        list: The teams in the given conference.
    """
    if season is None:
        season = _get_current_season()
    return _get_teams_from_conference(conference, season, "womens")


def get_team_schedule(team: str, season: Union[str, int] = None) -> pd.DataFrame:
    """Scrapes a given team's schedule for a specified season.

    Args:
        team (str): The name of the team to be scraped.
        season (str | int, optional): The season to be scraped. Defaults to current season.
            NOTE: season takes the form of the four-digit representation of the later year of the season. 
            So, as an example, the 2021-22 season is referred to by the integer 2022.

    Returns:
        pd.DataFrame: The given team's schedule for the year.
    """
    if season is None:
        season = _get_current_season()
    return _get_team_schedule(team, season, "womens")


def get_conference_schedule(conference: str, season: Union[str, int] = None) -> pd.DataFrame:
    """Returns the given season's schedules for all teams in the given conference.

    Args:
        conference (str): The conference to return schedules for.
        season (int, optional): The season to return conferences for. Defaults to current season.
            NOTE: season takes the form of the four-digit representation of the later year of the season. 
            So, as an example, the 2021-22 season is referred to by the integer 2022.

    Returns:
        pd.DataFrame: The conference schedules.
    """
    if season is None:
        season = _get_current_season()
    return _get_conference_schedule(conference, season, "womens")