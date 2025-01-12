from bs4 import BeautifulSoup as bs
import requests as r
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from dateutil import parser
from pytz import timezone as tz
from tqdm import trange
from joblib import Parallel, delayed
import re
import time
import traceback
import json
import os
import logging
import warnings
from rapidfuzz import process, distance, utils
from pathlib import Path
from platformdirs import user_log_dir
from importlib.metadata import version
from functools import wraps


warnings.filterwarnings('ignore', category=UserWarning)


ATTEMPTS = 15
DATE_PARSES = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m-%d-%Y",
    "%m/%d/%Y",
]
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 "
    + "(KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    + "(KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 "
    + "(KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 "
    + "(KHTML, like Gecko) Chrome/21.0.1180.83 Safari/537.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    + "(KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    + "(KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36",
    "Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 "
    + "(KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    + "(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 "
    + "(KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36",
]
REFERERS = [
    "https://google.com/",
    "https://youtube.com/",
    "https://facebook.com/",
    "https://twitter.com/",
    "https://nytimes.com/",
    "https://washingtonpost.com/",
    "https://linkedin.com/",
    "https://nhl.com/",
    "https://mlb.com/",
    "https://nfl.com/",
]
MENS_SCOREBOARD_URL = "https://www.espn.com/mens-college-basketball/scoreboard/_/date/{}/seasontype/2/group/50"
MENS_GAME_URL = "https://www.espn.com/mens-college-basketball/game/_/gameId/{}"
MENS_BOXSCORE_URL = "https://www.espn.com/mens-college-basketball/boxscore/_/gameId/{}"
MENS_PBP_URL = "https://www.espn.com/mens-college-basketball/playbyplay/_/gameId/{}"
MENS_PLAYER_URL = "https://www.espn.com/mens-college-basketball/player/_/id/{}"
MENS_SCHEDULE_URL = "https://www.espn.com/mens-college-basketball/team/schedule/_/id/{}/season/{}"
WOMENS_SCOREBOARD_URL = "https://www.espn.com/womens-college-basketball/scoreboard/_/date/{}/seasontype/2/group/50"
WOMENS_GAME_URL = "https://www.espn.com/womens-college-basketball/game/_/gameId/{}"
WOMENS_BOXSCORE_URL = (
    "https://www.espn.com/womens-college-basketball/boxscore/_/gameId/{}"
)
WOMENS_PBP_URL = "https://www.espn.com/womens-college-basketball/playbyplay/_/gameId/{}"
WOMENS_PLAYER_URL = "https://www.espn.com/womens-college-basketball/player/_/id/{}"
WOMENS_SCHEDULE_URL = "https://www.espn.com/womens-college-basketball/team/schedule/_/id/{}/season/{}"
NON_SHOT_TYPES = [
    "TV Timeout",
    "Jump Ball",
    "Turnover",
    "Timeout",
    "Rebound",
    "Block",
    "Steal",
    "Foul",
    "End",
]
SHOT_TYPES = [
    "Three Point Jumper",
    "Two Point Tip Shot",
    "Free Throw",
    "Jumper",
    "Layup",
    "Dunk",
]
WINDOW_STRING = "window['__espnfitt__']="
JSON_REGEX = r"window\[\'__espnfitt__\'\]={(.*)};"
STATUS_OK = 200
WOMEN_HALF_RULE_CHANGE_DATE = parser.parse("2015-05-01")
GOOD_GAME_STATUSES = ['In Progress', 'Final']


# logging setup
log_dir = user_log_dir(appname="CBBpy", appauthor="Daniel Cowan", version=version("cbbpy"))
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "CBBpy.log")

file_handler = logging.FileHandler(log_file)
formatter = logging.Formatter('%(asctime)s | %(name)s | %(levelname)s: %(message)s')
file_handler.setFormatter(formatter)

_log = logging.getLogger("CBBpy")
_log.setLevel(logging.WARNING)
_log.addHandler(file_handler)


_call_depth = [0]

def print_log_file_location(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        _call_depth[0] += 1  # Increment call depth
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            _call_depth[0] -= 1  # Decrement call depth
            if _call_depth[0] == 0:
                print(f"Log file is located at {log_file}")
    return wrapper


# pnf_ will keep track of games w/ page not found errors
# if game has error, don't run the other scrape functions to save time
pnf_ = []


class CouldNotParseError(Exception):
    pass


class InvalidDateRangeError(Exception):
    pass


def _get_game(game_id, game_type, info, box, pbp):
    game_id = str(game_id)
    game_info_df = boxscore_df = pbp_df = pd.DataFrame([])

    if game_id in pnf_:
        _log.error(f'{game_id} - Game Info: Page not found error')
    elif info:
        game_info_df = _get_game_info(game_id, game_type)

    if game_id in pnf_:
        _log.error(f'{game_id} - Boxscore: Page not found error')
    elif box:
        boxscore_df = _get_game_boxscore(game_id, game_type)

    if game_id in pnf_:
        _log.error(f'{game_id} - PBP: Page not found error')
    elif pbp:
        pbp_df = _get_game_pbp(game_id, game_type)

    return (game_info_df, boxscore_df, pbp_df)


@print_log_file_location
def _get_games_range(start_date, end_date, game_type, info, box, pbp):
    if isinstance(start_date, str):
        start_date = _parse_date(start_date)
    if isinstance(end_date, str):
        end_date = _parse_date(end_date)
    date_range = pd.date_range(start_date, end_date)
    len_scrape = len(date_range)
    all_data = []
    cpus = os.cpu_count() - 1

    if len_scrape < 1:
        raise InvalidDateRangeError("The start date must be sooner than the end date.")

    if start_date > datetime.today():
        raise InvalidDateRangeError("The start date must not be in the future.")

    if end_date > datetime.today():
        raise InvalidDateRangeError("The end date must not be in the future.")

    bar_format = (
        "{l_bar}{bar}| {n_fmt} of {total_fmt} days scraped in {elapsed_s:.1f} sec"
    )

    with trange(len_scrape, bar_format=bar_format) as t:
        for i in t:
            date = date_range[i]
            game_ids = _get_game_ids(date, game_type)
            t.set_description(f"Scraping {len(game_ids)} games on {date.strftime('%D')}")

            if len(game_ids) > 0:
                result = Parallel(n_jobs=cpus)(
                    delayed(_get_game)(gid, game_type, info, box, pbp)
                    for gid in game_ids
                )
                all_data.append(result)

            else:
                t.set_description(f"No games on {date.strftime('%D')}", refresh=False)

    if not len(all_data) > 0:
        return ()

    # sort returned dataframes to ensure consistency between runs
    game_info_df = pd.concat([game[0] for day in all_data for game in day])
    if info:
        game_info_df = game_info_df.sort_values(
            by=['game_day', 'game_time', 'game_id'], 
            key=lambda col: pd.to_datetime(col.str.replace(r' P[SD]T', '', 
                                                        regex=True)) if col.name != 'game_id' else col
        ).reset_index(drop=True)

    game_boxscore_df = pd.concat([game[1] for day in all_data for game in day])
    if box:
        game_boxscore_df = game_boxscore_df.sort_values(
            by=['game_id', 'team'], 
            ascending=False, 
            kind='mergesort'
        ).reset_index(drop=True)

    game_pbp_df = pd.concat([game[2] for day in all_data for game in day])
    if pbp:
        game_pbp_df = game_pbp_df.sort_values(
            by=['game_id'],
            ascending=False,
            kind='mergesort'
        ).reset_index(drop=True)

    return (game_info_df, game_boxscore_df, game_pbp_df)


@print_log_file_location
def _get_games_season(season, game_type, info, box, pbp):
    season_start_date = f"{season-1}-11-01"
    season_end_date = f"{season}-05-01"

    # if season has not started yet, throw error
    if datetime.strptime(season_start_date, "%Y-%m-%d") > datetime.today():
        raise InvalidDateRangeError("The start date must not be in the future.")

    # if season has not ended yet, set end scrape date to today
    if datetime.strptime(season_end_date, "%Y-%m-%d") > datetime.today():
        season_end_date = datetime.today().strftime("%Y-%m-%d")

    info = _get_games_range(
        season_start_date, season_end_date, game_type, info, box, pbp
    )

    return info


@print_log_file_location
def _get_games_team(team, season, game_type, info, box, pbp):
    cpus = os.cpu_count() - 1
    schedule_df = _get_team_schedule(team, season, game_type)
    game_ids = list(schedule_df[schedule_df.game_status.isin(GOOD_GAME_STATUSES)].game_id)

    print(f'Scraping {len(game_ids)} games for {schedule_df.team.iloc[0]}')

    result = Parallel(n_jobs=cpus)(
        delayed(_get_game)(gid, game_type, info, box, pbp)
        for gid in game_ids
    )

    # sort returned dataframes to ensure consistency between runs
    game_info_df = pd.concat([x[0] for x in result])
    if info:
        game_info_df = game_info_df.sort_values(
            by=['game_day', 'game_time', 'game_id'], 
            key=lambda col: pd.to_datetime(col.str.replace(r' P[SD]T', '', 
                                                        regex=True)) if col.name != 'game_id' else col
        ).reset_index(drop=True)

    game_boxscore_df = pd.concat([x[1] for x in result])
    if box:
        game_boxscore_df = game_boxscore_df.sort_values(
            by=['game_id', 'team'], 
            ascending=False, 
            kind='mergesort'
        ).reset_index(drop=True)

    game_pbp_df = pd.concat([x[2] for x in result])
    if pbp:
        game_pbp_df = game_pbp_df.sort_values(
            by=['game_id'],
            ascending=False,
            kind='mergesort'
        ).reset_index(drop=True)

    # print(f"Log file is located at {log_file}")

    return (game_info_df, game_boxscore_df, game_pbp_df)


@print_log_file_location
def _get_games_conference(conference, season, game_type, info, box, pbp):
    teams = _get_teams_from_conference(conference, season, game_type)
    result = [_get_games_team(x, season, game_type, info, box, pbp) for x in teams]

    # sort returned dataframes to ensure consistency between runs
    game_info_df = pd.concat([x[0] for x in result])
    if info:
        game_info_df = game_info_df.sort_values(
            by=['game_day', 'game_time', 'game_id'], 
            key=lambda col: pd.to_datetime(col.str.replace(r' P[SD]T', '', 
                                                        regex=True)) if col.name != 'game_id' else col
        ).reset_index(drop=True)

    game_boxscore_df = pd.concat([x[1] for x in result])
    if box:
        game_boxscore_df = game_boxscore_df.sort_values(
            by=['game_id', 'team'], 
            ascending=False, 
            kind='mergesort'
        ).reset_index(drop=True)

    game_pbp_df = pd.concat([x[2] for x in result])
    if pbp:
        game_pbp_df = game_pbp_df.sort_values(
            by=['game_id'],
            ascending=False,
            kind='mergesort'
        ).reset_index(drop=True)

    return (game_info_df, game_boxscore_df, game_pbp_df)


def _get_game_ids(date, game_type):
    soup = None

    if game_type == "mens":
        pre_url = MENS_SCOREBOARD_URL
    else:
        pre_url = WOMENS_SCOREBOARD_URL

    if isinstance(date, str):
        date = _parse_date(date)

    for i in range(ATTEMPTS):
        try:
            header = {
                "User-Agent": str(np.random.choice(USER_AGENTS)),
                "Referer": str(np.random.choice(REFERERS)),
            }
            d = date.strftime("%Y%m%d")
            url = pre_url.format(d)
            page = r.get(url, headers=header)
            soup = bs(page.content, "lxml")
            scoreboard = _get_scoreboard_from_soup(soup)
            ids = [x["id"] for x in scoreboard]

        except Exception as ex:
            if i + 1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                if soup is not None:
                    if "Page not found." in soup.text:
                        _log.error(
                            f'{date.strftime("%D")} - IDs: Page not found error'
                        )
                    elif "Page error" in soup.text:
                        _log.error(
                            f'{date.strftime("%D")} - IDs: Page error'
                        )
                    elif scoreboard is None:
                        _log.error(
                            f'{date.strftime("%D")} - IDs: JSON not found on page.'
                        )
                    else:
                        _log.error(
                            f'{date.strftime("%D")} - IDs: {ex}\n{traceback.format_exc()}'
                        )
                else:
                    _log.error(
                        f'{date.strftime("%D")} - IDs: GET error\n{ex}\n{traceback.format_exc()}'
                    )
                return pd.DataFrame([])
            else:
                # try again with a random sleep
                time.sleep(np.random.uniform(low=1, high=3))
                continue
        else:
            # no exception thrown
            break

    return ids


def _get_game_boxscore(game_id, game_type):
    soup = None
    game_id = str(game_id)

    if game_type == "mens":
        pre_url = MENS_BOXSCORE_URL
    else:
        pre_url = WOMENS_BOXSCORE_URL

    for i in range(ATTEMPTS):
        try:
            header = {
                "User-Agent": str(np.random.choice(USER_AGENTS)),
                "Referer": str(np.random.choice(REFERERS)),
            }
            url = pre_url.format(game_id)
            page = r.get(url, headers=header)
            soup = bs(page.content, "lxml")
            gamepackage = _get_gamepackage_from_soup(soup)

            # check if game was postponed, cancelled, etc
            gm_status = gamepackage["gmStrp"]["status"]["desc"]
            gsbool = gm_status in GOOD_GAME_STATUSES
            if not gsbool:
                _log.warning(f'{game_id} - {gm_status}')
                return pd.DataFrame([])

            boxscore = gamepackage["bxscr"]

            df = _get_game_boxscore_helper(boxscore, game_id)

        except Exception as ex:
            if soup is not None:
                if "No Box Score Available" in soup.text:
                    _log.warning(f'{game_id} - No boxscore available')
                    return pd.DataFrame([])

            if i + 1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                if soup is not None:
                    if "Page not found." in soup.text:
                        _log.error(
                            f'{game_id} - Boxscore: Page not found error'
                        )
                        pnf_.append(game_id)
                    elif "Page error" in soup.text:
                        _log.error(
                            f'{game_id} - Boxscore: Page error'
                        )
                    elif gamepackage is None:
                        _log.error(
                            f'{game_id} - Boxscore: Game JSON not found on page.'
                        )
                    else:
                        _log.error(
                            f'{game_id} - Boxscore: {ex}\n{traceback.format_exc()}'
                        )
                else:
                    _log.error(
                        f'{game_id} - Boxscore: GET error\n{ex}\n{traceback.format_exc()}'
                    )
                return pd.DataFrame([])
            else:
                # try again with a random sleep
                time.sleep(np.random.uniform(low=1, high=3))
                continue
        else:
            # no exception thrown
            break

    return df.reset_index(drop=True)


def _get_game_pbp(game_id, game_type):
    soup = None
    game_id = str(game_id)

    if game_type == "mens":
        pre_url = MENS_PBP_URL
    else:
        pre_url = WOMENS_PBP_URL

    for i in range(ATTEMPTS):
        try:
            header = {
                "User-Agent": str(np.random.choice(USER_AGENTS)),
                "Referer": str(np.random.choice(REFERERS)),
            }
            url = pre_url.format(game_id)
            page = r.get(url, headers=header)
            soup = bs(page.content, "lxml")
            gamepackage = _get_gamepackage_from_soup(soup)

            # check if game was postponed
            gm_status = gamepackage["gmStrp"]["status"]["desc"]
            gsbool = gm_status in GOOD_GAME_STATUSES
            if not gsbool:
                _log.warning(f'{game_id} - {gm_status}')
                return pd.DataFrame([])

            df = _get_game_pbp_helper(gamepackage, game_id, game_type)

        except Exception as ex:
            if i + 1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                if soup is not None:
                    if "Page not found." in soup.text:
                        _log.error(
                            f'{game_id} - PBP: Page not found error'
                        )
                        pnf_.append(game_id)
                    elif "Page error" in soup.text:
                        _log.error(f'{game_id} - PBP: Page error')
                    elif gamepackage is None:
                        _log.error(
                            f'{game_id} - PBP: Game JSON not found on page.'
                        )
                    else:
                        _log.error(
                            f'{game_id} - PBP: {ex}\n{traceback.format_exc()}'
                        )
                else:
                    _log.error(
                        f'{game_id} - PBP: GET error\n{ex}\n{traceback.format_exc()}'
                    )
                return pd.DataFrame([])
            else:
                # try again with a random sleep
                time.sleep(np.random.uniform(low=1, high=3))
                continue
        else:
            # no exception thrown
            break

    return df.reset_index(drop=True)


def _get_game_info(game_id, game_type):
    soup = None
    game_id = str(game_id)

    if game_type == "mens":
        pre_url = MENS_GAME_URL
    else:
        pre_url = WOMENS_GAME_URL

    for i in range(ATTEMPTS):
        try:
            header = {
                "User-Agent": str(np.random.choice(USER_AGENTS)),
                "Referer": str(np.random.choice(REFERERS)),
            }
            url = pre_url.format(game_id)
            page = r.get(url, headers=header)
            soup = bs(page.content, "lxml")
            gamepackage = _get_gamepackage_from_soup(soup)

            # check if game was postponed
            gm_status = gamepackage["gmStrp"]["status"]["desc"]
            gsbool = gm_status in GOOD_GAME_STATUSES
            if not gsbool:
                _log.warning(f'{game_id} - {gm_status}')

            df = _get_game_info_helper(gamepackage, game_id, game_type)

        except Exception as ex:
            if i + 1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                if soup is not None:
                    if "Page not found." in soup.text:
                        _log.error(
                            f'{game_id} - Game Info: Page not found error'
                        )
                        pnf_.append(game_id)
                    elif "Page error" in soup.text:
                        _log.error(
                            f'{game_id} - Game Info: Page error'
                        )
                    elif gamepackage is None:
                        _log.error(
                            f'{game_id} - Game Info: Game JSON not found on page.'
                        )
                    else:
                        _log.error(
                            f'{game_id} - Game Info: {ex}\n{traceback.format_exc()}'
                        )
                else:
                    _log.error(
                        f'{game_id} - Game Info: GET error\n{ex}\n{traceback.format_exc()}'
                    )
                return pd.DataFrame([])
            else:
                # try again with a random sleep
                time.sleep(np.random.uniform(low=1, high=3))
                continue
        else:
            # no exception thrown
            break

    return df


def _get_player_info(player_id, game_type):
    soup = None
    df = None

    if game_type == "mens":
        pre_url = MENS_PLAYER_URL
    else:
        pre_url = WOMENS_PLAYER_URL

    for i in range(ATTEMPTS):
        try:
            header = {
                "User-Agent": str(np.random.choice(USER_AGENTS)),
                "Referer": str(np.random.choice(REFERERS)),
            }
            url = pre_url.format(player_id)
            page = r.get(url, headers=header)
            soup = bs(page.content, "lxml")
            raw_player = _get_player_from_soup(soup)

            df = _get_player_details_helper(player_id, raw_player, game_type)

        except Exception as ex:
            if "Page not found." in soup.text:
                _log.error(
                    f'{player_id} - Player: Page not found error'
                )
                break

            if i + 1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                if soup is not None:
                    if "Page error" in soup.text:
                        _log.error(
                            f'{player_id} - Player: Page error'
                        )
                    elif raw_player is None:
                        _log.error(
                            f'{player_id} - Player: Player JSON not found on page.'
                        )
                    else:
                        _log.error(
                            f'{player_id} - Player: {ex}\n{traceback.format_exc()}'
                        )
                else:
                    _log.error(
                        f'{player_id} - Player: GET error\n{ex}\n{traceback.format_exc()}'
                    )
                return pd.DataFrame([])
            else:
                # try again with a random sleep
                time.sleep(np.random.uniform(low=1, high=3))
                continue
        else:
            # no exception thrown
            break

    return df


def _get_team_schedule(team, season, game_type):
    soup = None

    team_id, team_name = _get_id_from_team(team, season, game_type)

    if game_type == "mens":
        pre_url = MENS_SCHEDULE_URL
    else:
        pre_url = WOMENS_SCHEDULE_URL

    for i in range(ATTEMPTS):
        try:
            header = {
                "User-Agent": str(np.random.choice(USER_AGENTS)),
                "Referer": str(np.random.choice(REFERERS)),
            }
            url = pre_url.format(team_id, season)
            page = r.get(url, headers=header)
            soup = bs(page.content, "lxml")
            jsn = _get_json_from_soup(soup)
            df = _get_schedule_helper(jsn, team_name, team_id, season)

        except Exception as ex:
            if i + 1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                if soup is not None:
                    if "Page not found." in soup.text:
                        _log.error(
                            f'{team} - Schedule: Page not found error'
                        )
                    elif "Page error" in soup.text:
                        _log.error(
                            f'{team} - Schedule: Page error'
                        )
                    else:
                        _log.error(
                            f'{team} - Schedule: {ex}\n{traceback.format_exc()}'
                        )
                else:
                    _log.error(
                        f'{team} - Schedule: GET error\n{ex}\n{traceback.format_exc()}'
                    )
                return pd.DataFrame([])
            else:
                # try again with a random sleep
                time.sleep(np.random.uniform(low=1, high=3))
                continue
        else:
            # no exception thrown
            break

    return df


@print_log_file_location
def _get_conference_schedule(conference, season, game_type):
    teams = _get_teams_from_conference(conference, season, game_type)

    df = pd.DataFrame()

    for team in teams:
        sch = _get_team_schedule(team, season, game_type)
        df = pd.concat([df, sch])

    return df.reset_index(drop=True)


def _parse_date(date):
    parsed = False

    for parse in DATE_PARSES:
        try:
            date = datetime.strptime(date, parse)
        except:
            continue
        else:
            parsed = True
            break

    if not parsed:
        raise CouldNotParseError(
            f"The given date ({date}) could not be parsed. Try any of these formats:\n"
            + "Y-m-d\nY/m/d\nm-d-Y\nm/d/Y"
        )

    return date


def _get_game_boxscore_helper(boxscore, game_id):
    tm1, tm2 = boxscore[0], boxscore[1]
    tm1_name, tm2_name = tm1["tm"]["dspNm"], tm2["tm"]["dspNm"]
    tm1_stats, tm2_stats = tm1["stats"], tm2["stats"]

    labels = tm1_stats[0]["lbls"]

    tm1_starters, tm1_bench, tm1_totals = (
        tm1_stats[0]["athlts"],
        tm1_stats[1]["athlts"],
        tm1_stats[2]["ttls"],
    )
    tm2_starters, tm2_bench, tm2_totals = (
        tm2_stats[0]["athlts"],
        tm2_stats[1]["athlts"],
        tm2_stats[2]["ttls"],
    )

    # starters' stats
    if len(tm1_starters) > 0:
        tm1_st_dict = {
            labels[i].lower(): [
                tm1_starters[j]["stats"][i] for j in range(len(tm1_starters))
            ]
            for i in range(len(labels))
        }

        tm1_st_pos = [
            (
                tm1_starters[i]["athlt"]["pos"]
                if "pos" in tm1_starters[i]["athlt"].keys()
                else ""
            )
            for i in range(len(tm1_starters))
        ]
        tm1_st_id = [
            (
                tm1_starters[i]["athlt"]["uid"].split(":")[-1]
                if "uid" in tm1_starters[i]["athlt"].keys()
                else ""
            )
            for i in range(len(tm1_starters))
        ]
        tm1_st_nm = [
            (
                tm1_starters[i]["athlt"]["shrtNm"]
                if "shrtNm" in tm1_starters[i]["athlt"].keys()
                else ""
            )
            for i in range(len(tm1_starters))
        ]

        tm1_st_df = pd.DataFrame(tm1_st_dict)
        tm1_st_df.insert(0, "starter", True)
        tm1_st_df.insert(0, "position", tm1_st_pos)
        tm1_st_df.insert(0, "player_id", tm1_st_id)
        tm1_st_df.insert(0, "player", tm1_st_nm)
        tm1_st_df.insert(0, "team", tm1_name)
        tm1_st_df.insert(0, "game_id", game_id)

    else:
        cols = ["starter", "position", "player_id", "player", "team", "game_id"] + [
            x.lower() for x in labels
        ]
        tm1_st_df = pd.DataFrame(columns=cols)

    # bench players' stats
    if len(tm1_bench) > 0:
        tm1_bn_dict = {
            labels[i].lower(): [tm1_bench[j]["stats"][i] for j in range(len(tm1_bench))]
            for i in range(len(labels))
        }

        tm1_bn_pos = [
            (
                tm1_bench[i]["athlt"]["pos"]
                if "pos" in tm1_bench[i]["athlt"].keys()
                else ""
            )
            for i in range(len(tm1_bench))
        ]
        tm1_bn_id = [
            (
                tm1_bench[i]["athlt"]["uid"].split(":")[-1]
                if "uid" in tm1_bench[i]["athlt"].keys()
                else ""
            )
            for i in range(len(tm1_bench))
        ]
        tm1_bn_nm = [
            (
                tm1_bench[i]["athlt"]["shrtNm"]
                if "shrtNm" in tm1_bench[i]["athlt"].keys()
                else ""
            )
            for i in range(len(tm1_bench))
        ]

        tm1_bn_df = pd.DataFrame(tm1_bn_dict)
        tm1_bn_df.insert(0, "starter", False)
        tm1_bn_df.insert(0, "position", tm1_bn_pos)
        tm1_bn_df.insert(0, "player_id", tm1_bn_id)
        tm1_bn_df.insert(0, "player", tm1_bn_nm)
        tm1_bn_df.insert(0, "team", tm1_name)
        tm1_bn_df.insert(0, "game_id", game_id)

    else:
        cols = ["starter", "position", "player_id", "player", "team", "game_id"] + [
            x.lower() for x in labels
        ]
        tm1_bn_df = pd.DataFrame(columns=cols)

    # team totals
    if len(tm1_totals) > 0:
        tm1_tot_dict = {labels[i].lower(): [tm1_totals[i]] for i in range(len(labels))}

        tm1_tot_df = pd.DataFrame(tm1_tot_dict)
        tm1_tot_df.insert(0, "starter", False)
        tm1_tot_df.insert(0, "position", "TOTAL")
        tm1_tot_df.insert(0, "player_id", "TOTAL")
        tm1_tot_df.insert(0, "player", "TEAM")
        tm1_tot_df.insert(0, "team", tm1_name)
        tm1_tot_df.insert(0, "game_id", game_id)

    else:
        cols = ["starter", "position", "player_id", "player", "team", "game_id"] + [
            x.lower() for x in labels
        ]
        tm1_tot_df = pd.DataFrame(columns=cols)

    tm1_df = pd.concat([tm1_st_df, tm1_bn_df, tm1_tot_df])

    # starters' stats
    if len(tm2_starters) > 0:
        tm2_st_dict = {
            labels[i].lower(): [
                tm2_starters[j]["stats"][i] for j in range(len(tm2_starters))
            ]
            for i in range(len(labels))
        }

        tm2_st_pos = [
            (
                tm2_starters[i]["athlt"]["pos"]
                if "pos" in tm2_starters[i]["athlt"].keys()
                else ""
            )
            for i in range(len(tm2_starters))
        ]
        tm2_st_id = [
            (
                tm2_starters[i]["athlt"]["uid"].split(":")[-1]
                if "uid" in tm2_starters[i]["athlt"].keys()
                else ""
            )
            for i in range(len(tm2_starters))
        ]
        tm2_st_nm = [
            (
                tm2_starters[i]["athlt"]["shrtNm"]
                if "shrtNm" in tm2_starters[i]["athlt"].keys()
                else ""
            )
            for i in range(len(tm2_starters))
        ]

        tm2_st_df = pd.DataFrame(tm2_st_dict)
        tm2_st_df.insert(0, "starter", True)
        tm2_st_df.insert(0, "position", tm2_st_pos)
        tm2_st_df.insert(0, "player_id", tm2_st_id)
        tm2_st_df.insert(0, "player", tm2_st_nm)
        tm2_st_df.insert(0, "team", tm2_name)
        tm2_st_df.insert(0, "game_id", game_id)

    else:
        cols = ["starter", "position", "player_id", "player", "team", "game_id"] + [
            x.lower() for x in labels
        ]
        tm2_st_df = pd.DataFrame(columns=cols)

    # bench players' stats
    if len(tm2_bench) > 0:
        tm2_bn_dict = {
            labels[i].lower(): [tm2_bench[j]["stats"][i] for j in range(len(tm2_bench))]
            for i in range(len(labels))
        }

        tm2_bn_pos = [
            (
                tm2_bench[i]["athlt"]["pos"]
                if "pos" in tm2_bench[i]["athlt"].keys()
                else ""
            )
            for i in range(len(tm2_bench))
        ]
        tm2_bn_id = [
            (
                tm2_bench[i]["athlt"]["uid"].split(":")[-1]
                if "uid" in tm2_bench[i]["athlt"].keys()
                else ""
            )
            for i in range(len(tm2_bench))
        ]
        tm2_bn_nm = [
            (
                tm2_bench[i]["athlt"]["shrtNm"]
                if "shrtNm" in tm2_bench[i]["athlt"].keys()
                else ""
            )
            for i in range(len(tm2_bench))
        ]

        tm2_bn_df = pd.DataFrame(tm2_bn_dict)
        tm2_bn_df.insert(0, "starter", False)
        tm2_bn_df.insert(0, "position", tm2_bn_pos)
        tm2_bn_df.insert(0, "player_id", tm2_bn_id)
        tm2_bn_df.insert(0, "player", tm2_bn_nm)
        tm2_bn_df.insert(0, "team", tm2_name)
        tm2_bn_df.insert(0, "game_id", game_id)

    else:
        cols = ["starter", "position", "player_id", "player", "team", "game_id"] + [
            x.lower() for x in labels
        ]
        tm2_bn_df = pd.DataFrame(columns=cols)

    # team totals
    if len(tm2_totals) > 0:
        tm2_tot_dict = {labels[i].lower(): [tm2_totals[i]] for i in range(len(labels))}

        tm2_tot_df = pd.DataFrame(tm2_tot_dict)
        tm2_tot_df.insert(0, "starter", False)
        tm2_tot_df.insert(0, "position", "TOTAL")
        tm2_tot_df.insert(0, "player_id", "TOTAL")
        tm2_tot_df.insert(0, "player", "TEAM")
        tm2_tot_df.insert(0, "team", tm2_name)
        tm2_tot_df.insert(0, "game_id", game_id)

    else:
        cols = ["starter", "position", "player_id", "player", "team", "game_id"] + [
            x.lower() for x in labels
        ]
        tm2_tot_df = pd.DataFrame(columns=cols)

    tm2_df = pd.concat([tm2_st_df, tm2_bn_df, tm2_tot_df])

    df = pd.concat([tm1_df, tm2_df])

    if len(df) <= 0:
        _log.warning(f'{game_id} - No boxscore available')
        return pd.DataFrame([])

    # SPLIT UP THE FG FIELDS
    fgm = pd.to_numeric([x.split("-")[0] for x in df["fg"]], errors="coerce")
    fga = pd.to_numeric([x.split("-")[1] for x in df["fg"]], errors="coerce")
    thpm = pd.to_numeric([x.split("-")[0] for x in df["3pt"]], errors="coerce")
    thpa = pd.to_numeric([x.split("-")[1] for x in df["3pt"]], errors="coerce")
    ftm = pd.to_numeric([x.split("-")[0] for x in df["ft"]], errors="coerce")
    fta = pd.to_numeric([x.split("-")[1] for x in df["ft"]], errors="coerce")

    # GET RID OF UNWANTED COLUMNS
    df = df.drop(columns=["fg", "3pt", "ft"])

    # INSERT COLUMNS WHERE NECESSARY
    df.insert(7, "fgm", fgm)
    df.insert(8, "fga", fga)
    df.insert(9, "2pm", fgm - thpm)
    df.insert(10, "2pa", fga - thpa)
    df.insert(11, "3pm", thpm)
    df.insert(12, "3pa", thpa)
    df.insert(13, "ftm", ftm)
    df.insert(14, "fta", fta)

    # column type handling
    df["min"] = pd.to_numeric(df["min"], errors="coerce")
    df["oreb"] = pd.to_numeric(df["oreb"], errors="coerce")
    df["dreb"] = pd.to_numeric(df["dreb"], errors="coerce")
    df["reb"] = pd.to_numeric(df["reb"], errors="coerce")
    df["ast"] = pd.to_numeric(df["ast"], errors="coerce")
    df["stl"] = pd.to_numeric(df["stl"], errors="coerce")
    df["blk"] = pd.to_numeric(df["blk"], errors="coerce")
    df["to"] = pd.to_numeric(df["to"], errors="coerce")
    df["pf"] = pd.to_numeric(df["pf"], errors="coerce")
    df["pts"] = pd.to_numeric(df["pts"], errors="coerce")
    df['starter'] = df['starter'].astype(bool)

    return df


def _get_game_pbp_helper(gamepackage, game_id, game_type):
    pbp = gamepackage["pbp"]
    home_team = pbp["tms"]["home"]["displayName"]
    away_team = pbp["tms"]["away"]["displayName"]
    game_date = parser.parse(gamepackage["gmInfo"]["dtTm"])

    all_plays = [play for period in pbp["playGrps"] for play in period]

    # check if PBP exists
    if len(all_plays) <= 0:
        _log.warning(f'{game_id} - No PBP available')
        return pd.DataFrame([])

    descs = [x["text"] if "text" in x.keys() else "" for x in all_plays]
    teams = [
        (
            ""
            if not "homeAway" in x.keys()
            else home_team if x["homeAway"] == "home" else away_team
        )
        for x in all_plays
    ]
    hscores = [
        int(x["homeScore"]) if "homeScore" in x.keys() else np.nan for x in all_plays
    ]
    ascores = [
        int(x["awayScore"]) if "awayScore" in x.keys() else np.nan for x in all_plays
    ]
    periods = [
        int(x["period"]["number"]) if "period" in x.keys() else np.nan
        for x in all_plays
    ]

    time_splits = [
        x["clock"]["displayValue"].split(":") if "clock" in x.keys() else ""
        for x in all_plays
    ]
    minutes = [int(x[0]) for x in time_splits]
    seconds = [int(x[1]) for x in time_splits]
    min_to_sec = [x * 60 for x in minutes]
    pd_secs_left = [x + y for x, y in zip(min_to_sec, seconds)]

    # men, and women before the 15-16 season, use halves
    if (
        game_type == "mens"
        or game_date.replace(tzinfo=None) < WOMEN_HALF_RULE_CHANGE_DATE
    ):
        reg_secs_left = [
            1200 + x if half_num == 1 else x
            for x, half_num in zip(pd_secs_left, periods)
        ]
        pd_type = "half"
        pd_type_sec = "secs_left_half"
    # women (after 14-15) use quarters
    else:
        reg_secs_left = [
            (
                1800 + x
                if qt_num == 1
                else 1200 + x if qt_num == 2 else 600 + x if qt_num == 3 else x
            )
            for x, qt_num in zip(pd_secs_left, periods)
        ]
        pd_type = "quarter"
        pd_type_sec = "secs_left_qt"

    sc_play = [True if "scoringPlay" in x.keys() else False for x in all_plays]
    is_assisted = [
        True if ("text" in x.keys() and "assisted" in x["text"].lower()) else False
        for x in all_plays
    ]

    # ASSIGN PLAY TYPES
    p_types = []

    for x in all_plays:
        if not "text" in x.keys():
            p_types.append("")
            continue

        play = x["text"]

        if not type(play) == str:
            play = ""

        added = False
        for pt in NON_SHOT_TYPES:
            if pt in play:
                p_types.append(pt.lower())
                added = True
                break
        if not added:
            for st in SHOT_TYPES:
                if st in play:
                    p_types.append(st.lower())
                    added = True
                    break

        if not added:
            p_types.append("")

    # FIND SHOOTERS
    shooting_play = [
        True if x in (y.lower() for y in SHOT_TYPES) else False for x in p_types
    ]

    scorers = [x[0].split(" made ")[0] if x[1] else "" for x in zip(descs, sc_play)]

    non_scorers = [
        (
            x[0].split(" missed ")[0]
            if x[1] in (y.lower() for y in SHOT_TYPES) and not x[2]
            else ""
        )
        for x in zip(descs, p_types, sc_play)
    ]

    shooters = [x[0] if not x[0] == "" else x[1] for x in zip(scorers, non_scorers)]

    assisted_pls = [
        x[0].split("Assisted by ")[-1].replace(".", "") if x[1] else ""
        for x in zip(descs, is_assisted)
    ]

    is_three = ["three point" in x.lower() for x in descs]

    data = {
        "game_id": game_id,
        "home_team": home_team,
        "away_team": away_team,
        "play_desc": descs,
        "home_score": hscores,
        "away_score": ascores,
        pd_type: periods,
        pd_type_sec: pd_secs_left,
        "secs_left_reg": reg_secs_left,
        "play_team": teams,
        "play_type": p_types,
        "shooting_play": shooting_play,
        "scoring_play": sc_play,
        "is_three": is_three,
        "shooter": shooters,
        "is_assisted": is_assisted,
        "assist_player": assisted_pls,
    }

    df = pd.DataFrame(data)

    # add shot data if it exists
    is_shotchart = "shtChrt" in gamepackage

    if is_shotchart:
        chart = gamepackage["shtChrt"]["plays"]

        shotteams = [x["homeAway"] for x in chart]
        shotdescs = [x["text"] for x in chart]
        xs = [50 - int(x["coordinate"]["x"]) for x in chart]
        ys = [int(x["coordinate"]["y"]) for x in chart]

        shot_data = {"team": shotteams, "play_desc": shotdescs, "x": xs, "y": ys}

        shot_df = pd.DataFrame(shot_data)

        # shot matching
        shot_info = {
            "shot_x": [],
            "shot_y": [],
        }
        shot_count = 0

        for play, isshot in zip(df.play_desc, df.shooting_play):
            if shot_count >= len(shot_df):
                shot_info["shot_x"].append(np.nan)
                shot_info["shot_y"].append(np.nan)
                continue

            if not isshot:
                shot_info["shot_x"].append(np.nan)
                shot_info["shot_y"].append(np.nan)
                continue

            if "free throw" in play.lower():
                shot_info["shot_x"].append(np.nan)
                shot_info["shot_y"].append(np.nan)
                shot_count += 1
                continue

            shot_play = shot_df.play_desc.iloc[shot_count]

            if play == shot_play:
                shot_info["shot_x"].append(shot_df.x.iloc[shot_count])
                shot_info["shot_y"].append(shot_df.y.iloc[shot_count])
                shot_count += 1
            else:
                shot_info["shot_x"].append(np.nan)
                shot_info["shot_y"].append(np.nan)

        # make sure that length of shot data matches number of shots in PBP data
        if (not (len(shot_info["shot_x"]) == len(df))) or (
            not (len(shot_info["shot_y"]) == len(df))
        ):
            _log.warning(
                f'{game_id} - Shot data length does not match PBP data'
            )
            df["shot_x"] = np.nan
            df["shot_y"] = np.nan
            return df.sort_values(by=[pd_type, pd_type_sec], ascending=[True, False])

        df["shot_x"] = shot_info["shot_x"]
        df["shot_y"] = shot_info["shot_y"]

    else:
        df["shot_x"] = np.nan
        df["shot_y"] = np.nan

    return df.sort_values(by=[pd_type, pd_type_sec], ascending=[True, False])


def _get_game_info_helper(gamepackage, game_id, game_type):
    info = gamepackage["gmInfo"]
    more_info = gamepackage["gmStrp"]

    attendance = float(info.get("attnd", np.nan))
    capacity = float(info.get("cpcty", np.nan))
    network = info.get("cvrg", "")

    gm_date = parser.parse(info["dtTm"])
    game_date = gm_date.replace(tzinfo=timezone.utc).astimezone(tz=tz("US/Pacific"))
    game_day = game_date.strftime("%B %d, %Y")
    game_time = game_date.strftime("%I:%M %p %Z")
    gm_status = more_info["status"]["desc"]

    arena = info.get("loc", "")
    loc = (
        info["locAddr"]["city"] + ", " + info["locAddr"]["state"]
        if "locAddr" in info.keys()
        else ""
    )

    tot_refs = info.get("refs", {})
    ref_1 = tot_refs[0]["dspNm"] if len(tot_refs) > 0 else ""
    ref_2 = tot_refs[1]["dspNm"] if len(tot_refs) > 1 else ""
    ref_3 = tot_refs[2]["dspNm"] if len(tot_refs) > 2 else ""

    teams = more_info["tms"]
    ht_info, at_info = teams[0], teams[1]

    home_team, away_team = ht_info["displayName"], at_info["displayName"]

    home_id = ht_info["id"]
    away_id = at_info["id"]

    if len(ht_info["links"]) == 0:
        ht = home_team.lower().replace(" ", "-")
        home_id = "nd-" + re.sub(r"[^0-9a-zA-Z-]", "", ht)
    elif len(ht_info["records"]) == 0:
        ht = home_team.lower().replace(" ", "-")
        home_id = "nd-" + re.sub(r"[^0-9a-zA-Z-]", "", ht)

    if len(at_info["links"]) == 0:
        at = away_team.lower().replace(" ", "-")
        away_id = "nd-" + re.sub(r"[^0-9a-zA-Z-]", "", at)
    elif len(at_info["records"]) == 0:
        at = away_team.lower().replace(" ", "-")
        away_id = "nd-" + re.sub(r"[^0-9a-zA-Z-]", "", at)

    home_rank = ht_info.get("rank", np.nan)
    away_rank = at_info.get("rank", np.nan)

    home_record = (
        ht_info["records"][0]["displayValue"] if len(ht_info["records"]) > 0 else ""
    )
    away_record = (
        at_info["records"][0]["displayValue"] if len(at_info["records"]) > 0 else ""
    )

    home_score, away_score = int(ht_info.get("score", 0)), int(at_info.get("score", 0))

    home_win = True if home_score > away_score and gm_status == 'Final' else False

    is_postseason = True if more_info["seasonType"] == 3 else False
    is_conference = more_info["isConferenceGame"]

    if "neutralSite" in more_info:
        is_neutral = True
    else:
        is_neutral = False

    tournament = more_info.get("nte", "")

    if ("linescores" in ht_info) and ("linescores" in at_info):
        # men, and women before the 15-16 season, use halves
        if (
            game_type == "mens"
            or game_date.replace(tzinfo=None) < WOMEN_HALF_RULE_CHANGE_DATE
        ):
            h_ot, a_ot = len(ht_info["linescores"]) - 2, len(at_info["linescores"]) - 2
        # women (after 14-15) use quarters
        else:
            h_ot, a_ot = len(ht_info["linescores"]) - 4, len(at_info["linescores"]) - 4

        assert h_ot == a_ot
        num_ots = h_ot
    else:
        _log.warning(f'{game_id} - No score info available')
        num_ots = -1

    try:
        home_spread = gamepackage['gameOdds']['odds'][-1]['pointSpread']['primary']
    except:
        home_spread = ''

    game_info_list = [
        game_id,
        gm_status,
        home_team,
        home_id,
        home_rank,
        home_record,
        home_score,
        away_team,
        away_id,
        away_rank,
        away_record,
        away_score,
        home_spread,
        home_win,
        num_ots,
        is_conference,
        is_neutral,
        is_postseason,
        tournament,
        game_day,
        game_time,
        loc,
        arena,
        capacity,
        attendance,
        network,
        ref_1,
        ref_2,
        ref_3,
    ]

    game_info_cols = [
        "game_id",
        "game_status",
        "home_team",
        "home_id",
        "home_rank",
        "home_record",
        "home_score",
        "away_team",
        "away_id",
        "away_rank",
        "away_record",
        "away_score",
        "home_point_spread",
        "home_win",
        "num_ots",
        "is_conference",
        "is_neutral",
        "is_postseason",
        "tournament",
        "game_day",
        "game_time",
        "game_loc",
        "arena",
        "arena_capacity",
        "attendance",
        "tv_network",
        "referee_1",
        "referee_2",
        "referee_3",
    ]

    return pd.DataFrame([game_info_list], columns=game_info_cols)


def _get_player_details_helper(player_id, info, game_type):
    details = info['plyrHdr']['ath']
    more_details = info['prtlCmnApiRsp']['athlete']
    
    if len(more_details['collegeTeam']) > 0:
        prof = True
    else:
        prof = False

    if game_type == 'mens':
        prof_league = 'NBA'
    else:
        prof_league = 'WNBA'

    dob = more_details.get('displayDOB', '')
    team = more_details['college'].get('displayName', '') if prof else more_details['team'].get('displayName', '')

    return pd.DataFrame.from_records([{
        'player_id': str(player_id),
        'first_name': details.get('fNm'),
        'last_name': details.get('lNm'),
        'jersey_number': 'N/A' if prof else details.get('dspNum', '').replace('#', ''),
        'pos': details.get('position', {}).get('displayName', ''),
        'status': more_details['status']['name'],
        'team': team,
        'experience': prof_league if prof else more_details.get('displayExperience'),
        'height': more_details.get('displayHeight', ''),
        'weight': more_details.get('displayWeight', ''),
        'birthplace': more_details.get('displayBirthPlace', ''),
        'date_of_birth': str(_parse_date(dob).date()) if not dob == '' else ''
    }])


def _get_schedule_helper(jsn, team, id_, season):
    # reg season, playoffs, etc are separated
    season_types = jsn["page"]["content"]['scheduleData']['teamSchedule']

    tot_events = []

    # combine data from diff season types
    for x in season_types[::-1]:
        y = x['events']['pre'] + x['events']['post']
        tot_events.extend(y)

    tot_events = [x for x in tot_events if 'date' in x]

    data = []

    # get info from each game
    for ev in tot_events:
        mat = re.search(r'gameId/(\d+)/', ev['time']['link'])
        game_id = mat.group(1) if mat is not None else ''

        date = parser.parse(ev['date']['date']).astimezone(tz('America/Los_Angeles'))
        day = date.strftime('%B %d, %Y')
        time = date.strftime('%I:%M %p %Z')

        opp = ev['opponent']['displayName']
        opp_id = ev['opponent']['id']

        network = ev['network'][0]['name'] if len(ev['network']) > 0 else ''
        season_type = ev['seasonType']['name']
        status = ev['status']['description']

        res = ev['result']

        if status == 'Final':
            result = res['winLossSymbol'] + ' ' + res['currentTeamScore'] + '-' + res['opponentTeamScore']
        else:
            result = 'N/A'

        row = (team, id_, season, game_id, day, time, opp, opp_id, season_type, status, network, result)
        data.append(row)

    cols = [
        'team',
        'team_id',
        'season',
        'game_id',
        'game_day',
        'game_time',
        'opponent',
        'opponent_id',
        'season_type',
        'game_status',
        'tv_network',
        'game_result'
    ]

    df = pd.DataFrame(data, columns=cols)
    df = df.sort_values(
        by=['team', 'game_day'],
        key=lambda x: x if x.name == 'team' else pd.to_datetime(x)
    )

    return df.reset_index(drop=True)


def _get_team_map(game_type):
    data_path = Path(__file__).parent / f'{game_type}_team_map.csv'
    return pd.read_csv(data_path)


def _get_id_from_team(team, season, game_type):
    # fetch list of teams and team IDs for given season
    season = int(season)
    team_map_df = _get_team_map(game_type)
    id_map = team_map_df[team_map_df.season == season][['id', 'location']]
    id_map = id_map.set_index('location')['id'].to_dict()
    lowercase_map = {x.lower(): x for x in id_map.keys()}

    # if the given team is not in the list of teams, search for nearest match
    if not team.lower() in lowercase_map:
        choices = list(id_map.keys())

        best_match, score, _ = process.extractOne(
            team,
            choices,
            scorer=distance.JaroWinkler.normalized_similarity,
            processor=utils.default_process
        )

        print(f"No exact match for '{team}'. Fetching closest team match: '{best_match}'.")
        
        id_ = id_map[best_match]
    else:
        best_match = lowercase_map[team.lower()]
        id_ = id_map[best_match]

    return id_, best_match


def _get_season_conferences(season, game_type):
    season = int(season)
    team_map_df = _get_team_map(game_type)
    confs_df = team_map_df[team_map_df.season == season][['conference', 'conference_abb']].drop_duplicates()
    return confs_df.reset_index(drop=True)


def _get_teams_from_conference(conference, season, game_type):
    # fetch list of teams and team IDs for given season
    season = int(season)
    team_map_df, confs_df = _get_team_map(game_type), _get_season_conferences(season, game_type)
    abb_map = confs_df.set_index('conference_abb').conference.to_dict()
    choices = confs_df.conference.tolist() + confs_df.conference_abb.tolist()
    lowercase_map = {x.lower(): x for x in choices}

    # if the given conference is not in the list of conferences, search for nearest match
    if not conference.lower() in lowercase_map:
        best_match, score, _ = process.extractOne(
            conference,
            choices,
            scorer=distance.JaroWinkler.normalized_similarity,
            processor=utils.default_process
        )

        # if matched abbreviation, swap for conference name
        if best_match in abb_map:
            best_match = abb_map[best_match]

        print(f"No exact match for '{conference}'. Fetching closest conference match: '{best_match}'.")
    else:
        best_match = lowercase_map[conference.lower()]

        # if matched abbreviation, swap for conference name
        if best_match in abb_map:
            best_match = abb_map[best_match]

    # filter teams df to relevant conference
    rel_team_df = team_map_df[(team_map_df.season == season) & (team_map_df.conference == best_match)]

    return rel_team_df.location.tolist()


def _get_json_from_soup(soup):
    script_string = _find_json_in_content(soup)

    if script_string == "":
        return None

    pattern = re.compile(JSON_REGEX)
    found = re.search(pattern, script_string).group(1)
    js = "{" + found + "}"
    jsn = json.loads(js)

    return jsn


def _get_gamepackage_from_soup(soup):
    script_string = _find_json_in_content(soup)

    if script_string == "":
        return None

    pattern = re.compile(JSON_REGEX)
    found = re.search(pattern, script_string).group(1)
    js = "{" + found + "}"
    jsn = json.loads(js)
    gamepackage = jsn["page"]["content"]["gamepackage"]

    return gamepackage


def _get_player_from_soup(soup):
    script_string = _find_json_in_content(soup)

    if script_string == "":
        return None

    pattern = re.compile(JSON_REGEX)
    found = re.search(pattern, script_string).group(1)
    js = "{" + found + "}"
    jsn = json.loads(js)
    player = jsn["page"]["content"]["player"]

    return player


def _get_scoreboard_from_soup(soup):
    script_string = _find_json_in_content(soup)

    if script_string == "":
        return None

    pattern = re.compile(JSON_REGEX)
    found = re.search(pattern, script_string).group(1)
    js = "{" + found + "}"
    jsn = json.loads(js)
    scoreboard = jsn["page"]["content"]["scoreboard"]["evts"]

    return scoreboard


def _find_json_in_content(soup):
    script_string = ""
    for x in soup.find_all("script"):
        if WINDOW_STRING in x.text:
            script_string = x.text
            break
    return script_string


def _get_current_season():
    if datetime.today().month >= 10:
        return datetime.today().year + 1
    return datetime.today().year