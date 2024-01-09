from bs4 import BeautifulSoup as bs
import requests as r
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from dateutil.parser import parse
from pytz import timezone as tz
from tqdm import trange
from joblib import Parallel, delayed
import re
import time
import traceback
import json
import os
import logging


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
WOMENS_SCOREBOARD_URL = "https://www.espn.com/womens-college-basketball/scoreboard/_/date/{}/seasontype/2/group/50"
WOMENS_GAME_URL = "https://www.espn.com/womens-college-basketball/game/_/gameId/{}"
WOMENS_BOXSCORE_URL = (
    "https://www.espn.com/womens-college-basketball/boxscore/_/gameId/{}"
)
WOMENS_PBP_URL = "https://www.espn.com/womens-college-basketball/playbyplay/_/gameId/{}"
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


logging.basicConfig(filename="cbbpy.log")
_log = logging.getLogger(__name__)


# pnf_ will keep track of games w/ page not found errors
# if game has error, don't run the other scrape functions to save time
pnf_ = []


class CouldNotParseError(Exception):
    pass


class InvalidDateRangeError(Exception):
    pass


def _get_game(game_id, game_type, info, box, pbp):
    """A function that scrapes all game info (metadata, boxscore, play-by-play).

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - (game_info_df, boxscore_df, pbp_df), a tuple consisting of:
            -- game_info_df: a DataFrame of the game's metadata
            -- boxscore_df: a DataFrame of the game's boxscore (both teams combined)
            -- pbp_df: a DataFrame of the game's play-by-play
    """
    game_info_df = boxscore_df = pbp_df = pd.DataFrame([])

    if game_id in pnf_:
        _log.error(f'"{time.ctime()}": {game_id} - Game Info: Page not found error')
    elif info:
        game_info_df = _get_game_info(game_id, game_type)

    if game_id in pnf_:
        _log.error(f'"{time.ctime()}": {game_id} - Boxscore: Page not found error')
    elif box:
        boxscore_df = _get_game_boxscore(game_id, game_type)

    if game_id in pnf_:
        _log.error(f'"{time.ctime()}": {game_id} - PBP: Page not found error')
    elif pbp:
        pbp_df = _get_game_pbp(game_id, game_type)

    return (game_info_df, boxscore_df, pbp_df)


def _get_games_range(start_date, end_date, game_type, info, box, pbp):
    """A function that scrapes a game information between a given range of dates.

    Parameters:
        - start_date: a string representing the first day of games to scrape
        - end_date: a string representing the last day of games to scrape (inclusive)
        - info: a boolean denoting whether game metadata is to be scraped
        - box: a boolean denoting whether game boxscore is to be scraped
        - pbp: a boolean denoting whether game play-by-play is to be scraped

    Returns
        - (game_info_df, boxscore_df, pbp_df), a tuple consisting of:
            -- game_info_df: a DataFrame of the game's metadata
            -- boxscore_df: a DataFrame of the game's boxscore (both teams combined)
            -- pbp_df: a DataFrame of the game's play-by-play
    """
    sd = _parse_date(start_date)
    ed = _parse_date(end_date)
    date_range = pd.date_range(sd, ed)
    len_scrape = len(date_range)
    all_data = []
    cpus = os.cpu_count() - 1

    if len_scrape < 1:
        raise InvalidDateRangeError("The start date must be sooner than the end date.")

    if sd > datetime.today():
        raise InvalidDateRangeError("The start date must not be in the future.")

    if ed > datetime.today():
        raise InvalidDateRangeError("The end date must not be in the future.")

    bar_format = (
        "{l_bar}{bar}| {n_fmt} of {total_fmt} days scraped in {elapsed_s:.1f} sec"
    )

    with trange(len_scrape, bar_format=bar_format) as t:
        for i in t:
            date = date_range[i]
            game_ids = _get_game_ids(date, game_type)
            t.set_description(
                f"Scraping {len(game_ids)} games on {date.strftime('%D')}",
                refresh=False,
            )

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

    game_info_df = pd.concat([game[0] for day in all_data for game in day]).reset_index(
        drop=True
    )
    game_boxscore_df = pd.concat(
        [game[1] for day in all_data for game in day]
    ).reset_index(drop=True)
    game_pbp_df = pd.concat([game[2] for day in all_data for game in day]).reset_index(
        drop=True
    )

    return (game_info_df, game_boxscore_df, game_pbp_df)


def _get_games_season(season, game_type, info, box, pbp):
    """A function that scrapes all game info (metadata, boxscore, play-by-play) for every game of
    a given season.

    Parameters:
        - season: an integer representing the season to be scraped. NOTE: season is takes the form
        of the four-digit representation of the later year of the season. So, as an example, the
        2021-22 season is referred to by the integer 2022.

    Returns
        - (game_info_df, boxscore_df, pbp_df), a tuple consisting of:
            -- game_info_df: a DataFrame of the game's metadata
            -- boxscore_df: a DataFrame of the game's boxscore (both teams combined)
            -- pbp_df: a DataFrame of the game's play-by-play
    """
    season_start_date = f"{season-1}-11-01"
    season_end_date = f"{season}-05-01"

    # if season has not ended yet, set end scrape date to today
    if datetime.strptime(season_end_date, "%Y-%m-%d") > datetime.today():
        season_end_date = datetime.today().strftime("%Y-%m-%d")

    info = _get_games_range(
        season_start_date, season_end_date, game_type, info, box, pbp
    )

    return info


def _get_game_ids(date, game_type):
    """A function that scrapes all game IDs on a date.

    Parameters:
        - date: a string/datetime object representing the date to be scraped

    Returns
        - a list of ESPN all game IDs for games played on the date given
    """
    soup = None

    if game_type == "mens":
        pre_url = MENS_SCOREBOARD_URL
    else:
        pre_url = WOMENS_SCOREBOARD_URL

    if type(date) == str:
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
                            f'"{time.ctime()}": {date.strftime("%D")} - IDs: Page not found error'
                        )
                    elif "Page error" in soup.text:
                        _log.error(
                            f'"{time.ctime()}": {date.strftime("%D")} - IDs: Page error'
                        )
                    elif scoreboard is None:
                        _log.error(
                            f'"{time.ctime()}": {date.strftime("%D")} - IDs: JSON not found on page.'
                        )
                    else:
                        _log.error(
                            f'"{time.ctime()}": {date.strftime("%D")} - IDs: {ex}\n{traceback.format_exc()}'
                        )
                else:
                    _log.error(
                        f'"{time.ctime()}": {date.strftime("%D")} - IDs: GET error\n{ex}\n{traceback.format_exc()}'
                    )
                return pd.DataFrame([])
            else:
                # try again
                time.sleep(2)
                continue
        else:
            # no exception thrown
            break

    return ids


def _get_game_boxscore(game_id, game_type):
    """A function that scrapes a game's boxscore.

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - the game boxscore as a DataFrame
    """
    soup = None

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

            # check if game was postponed
            gm_status = gamepackage["gmStrp"]["status"]["desc"]
            gsbool = gm_status == "Final"  # or (gm_status == 'In Progress')
            if not gsbool:
                _log.warning(f'"{time.ctime()}": {game_id} - {gm_status}')
                return pd.DataFrame([])

            boxscore = gamepackage["bxscr"]

            df = _get_game_boxscore_helper(boxscore, game_id)

        except Exception as ex:
            if soup is not None:
                if "No Box Score Available" in soup.text:
                    _log.warning(f'"{time.ctime()}": {game_id} - No boxscore available')
                    return pd.DataFrame([])

            if i + 1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                if soup is not None:
                    if "Page not found." in soup.text:
                        _log.error(
                            f'"{time.ctime()}": {game_id} - Boxscore: Page not found error'
                        )
                        pnf_.append(game_id)
                    elif "Page error" in soup.text:
                        _log.error(
                            f'"{time.ctime()}": {game_id} - Boxscore: Page error'
                        )
                    elif gamepackage is None:
                        _log.error(
                            f'"{time.ctime()}": {game_id} - Boxscore: Game JSON not found on page.'
                        )
                    else:
                        _log.error(
                            f'"{time.ctime()}": {game_id} - Boxscore: {ex}\n{traceback.format_exc()}'
                        )
                else:
                    _log.error(
                        f'"{time.ctime()}": {game_id} - Boxscore: GET error\n{ex}\n{traceback.format_exc()}'
                    )
                return pd.DataFrame([])
            else:
                # try again
                time.sleep(2)
                continue
        else:
            # no exception thrown
            break

    return df


def _get_game_pbp(game_id, game_type):
    """A function that scrapes a game's play-by-play information.

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - the game's play-by-play information represented as a DataFrame
    """
    soup = None

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
            gsbool = gm_status == "Final"  # or (gm_status == 'In Progress')
            if not gsbool:
                _log.warning(f'"{time.ctime()}": {game_id} - {gm_status}')
                return pd.DataFrame([])

            df = _get_game_pbp_helper(gamepackage, game_id, game_type)

        except Exception as ex:
            if i + 1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                if soup is not None:
                    if "Page not found." in soup.text:
                        _log.error(
                            f'"{time.ctime()}": {game_id} - PBP: Page not found error'
                        )
                        pnf_.append(game_id)
                    elif "Page error" in soup.text:
                        _log.error(f'"{time.ctime()}": {game_id} - PBP: Page error')
                    elif gamepackage is None:
                        _log.error(
                            f'"{time.ctime()}": {game_id} - PBP: Game JSON not found on page.'
                        )
                    else:
                        _log.error(
                            f'"{time.ctime()}": {game_id} - PBP: {ex}\n{traceback.format_exc()}'
                        )
                else:
                    _log.error(
                        f'"{time.ctime()}": {game_id} - PBP: GET error\n{ex}\n{traceback.format_exc()}'
                    )
                return pd.DataFrame([])
            else:
                # try again
                time.sleep(2)
                continue
        else:
            # no exception thrown
            break

    return df


def _get_game_info(game_id, game_type):
    """A function that scrapes game metadata.

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - a DataFrame with one row and a column for each piece of metadata
    """
    soup = None

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
            gsbool = gm_status == "Final"  # or (gm_status == 'In Progress')
            if not gsbool:
                _log.warning(f'"{time.ctime()}": {game_id} - {gm_status}')
                return pd.DataFrame([])

            # get general game info
            info = gamepackage["gmInfo"]

            # get team info
            more_info = gamepackage["gmStrp"]

            df = _get_game_info_helper(info, more_info, game_id, game_type)

        except Exception as ex:
            if i + 1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                if soup is not None:
                    if "Page not found." in soup.text:
                        _log.error(
                            f'"{time.ctime()}": {game_id} - Game Info: Page not found error'
                        )
                        pnf_.append(game_id)
                    elif "Page error" in soup.text:
                        _log.error(
                            f'"{time.ctime()}": {game_id} - Game Info: Page error'
                        )
                    elif gamepackage is None:
                        _log.error(
                            f'"{time.ctime()}": {game_id} - Game Info: Game JSON not found on page.'
                        )
                    else:
                        _log.error(
                            f'"{time.ctime()}": {game_id} - Game Info: {ex}\n{traceback.format_exc()}'
                        )
                else:
                    _log.error(
                        f'"{time.ctime()}": {game_id} - Game Info: GET error\n{ex}\n{traceback.format_exc()}'
                    )
                return pd.DataFrame([])
            else:
                # try again
                time.sleep(2)
                continue
        else:
            # no exception thrown
            break

    return df


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
            "The given date could not be parsed. Try any of these formats:\n"
            + "Y-m-d\nY/m/d\nm-d-Y\nm/d/Y"
        )

    return date


def _get_game_boxscore_helper(boxscore, game_id):
    """A helper function that cleans a game's boxscore.

    Parameters:
        - boxscore: a JSON object containing the boxscore
        - game_id: a string representing the game's ESPN game ID

    Returns
        - the game boxscore as a DataFrame
    """
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
            tm1_starters[i]["athlt"]["pos"]
            if "pos" in tm1_starters[i]["athlt"].keys()
            else ""
            for i in range(len(tm1_starters))
        ]
        tm1_st_id = [
            tm1_starters[i]["athlt"]["uid"].split(":")[-1]
            if "uid" in tm1_starters[i]["athlt"].keys()
            else ""
            for i in range(len(tm1_starters))
        ]
        tm1_st_nm = [
            tm1_starters[i]["athlt"]["shrtNm"]
            if "shrtNm" in tm1_starters[i]["athlt"].keys()
            else ""
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
            tm1_bench[i]["athlt"]["pos"]
            if "pos" in tm1_bench[i]["athlt"].keys()
            else ""
            for i in range(len(tm1_bench))
        ]
        tm1_bn_id = [
            tm1_bench[i]["athlt"]["uid"].split(":")[-1]
            if "uid" in tm1_bench[i]["athlt"].keys()
            else ""
            for i in range(len(tm1_bench))
        ]
        tm1_bn_nm = [
            tm1_bench[i]["athlt"]["shrtNm"]
            if "shrtNm" in tm1_bench[i]["athlt"].keys()
            else ""
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
            tm2_starters[i]["athlt"]["pos"]
            if "pos" in tm2_starters[i]["athlt"].keys()
            else ""
            for i in range(len(tm2_starters))
        ]
        tm2_st_id = [
            tm2_starters[i]["athlt"]["uid"].split(":")[-1]
            if "uid" in tm2_starters[i]["athlt"].keys()
            else ""
            for i in range(len(tm2_starters))
        ]
        tm2_st_nm = [
            tm2_starters[i]["athlt"]["shrtNm"]
            if "shrtNm" in tm2_starters[i]["athlt"].keys()
            else ""
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
            tm2_bench[i]["athlt"]["pos"]
            if "pos" in tm2_bench[i]["athlt"].keys()
            else ""
            for i in range(len(tm2_bench))
        ]
        tm2_bn_id = [
            tm2_bench[i]["athlt"]["uid"].split(":")[-1]
            if "uid" in tm2_bench[i]["athlt"].keys()
            else ""
            for i in range(len(tm2_bench))
        ]
        tm2_bn_nm = [
            tm2_bench[i]["athlt"]["shrtNm"]
            if "shrtNm" in tm2_bench[i]["athlt"].keys()
            else ""
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
        _log.warning(f'"{time.ctime()}": {game_id} - No boxscore available')
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

    return df


def _get_game_pbp_helper(gamepackage, game_id, game_type):
    """A helper function that cleans a game's PBP.

    Parameters:
        - pbp: a JSON object containing the play-by-play
        - game_id: a string representing the game's ESPN game ID
        - game_type: a string representing whether men's or women's basketball is being scraped

    Returns
        - the game PBP as a DataFrame
    """
    pbp = gamepackage["pbp"]
    home_team = pbp["tms"]["home"]["displayName"]
    away_team = pbp["tms"]["away"]["displayName"]

    all_plays = [play for period in pbp["playGrps"] for play in period]

    # check if PBP exists
    if len(all_plays) <= 0:
        _log.warning(f'"{time.ctime()}": {game_id} - No PBP available')
        return pd.DataFrame([])

    descs = [x["text"] if "text" in x.keys() else "" for x in all_plays]
    teams = [
        ""
        if not "homeAway" in x.keys()
        else home_team
        if x["homeAway"] == "home"
        else away_team
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

    if game_type == "mens":
        reg_secs_left = [
            1200 + x if half_num == 1 else x
            for x, half_num in zip(pd_secs_left, periods)
        ]
    else:
        reg_secs_left = [
            1800 + x
            if qt_num == 1
            else 1200 + x
            if qt_num == 2
            else 600 + x
            if qt_num == 3
            else x
            for x, qt_num in zip(pd_secs_left, periods)
        ]

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
        x[0].split(" missed ")[0]
        if x[1] in (y.lower() for y in SHOT_TYPES) and not x[2]
        else ""
        for x in zip(descs, p_types, sc_play)
    ]

    shooters = [x[0] if not x[0] == "" else x[1] for x in zip(scorers, non_scorers)]

    assisted_pls = [
        x[0].split("Assisted by ")[-1].replace(".", "") if x[1] else ""
        for x in zip(descs, is_assisted)
    ]

    is_three = ["three point" in x.lower() for x in descs]

    if game_type == "mens":
        pd_type = "half"
        pd_type_sec = "secs_left_half"
    else:
        pd_type = "quarter"
        pd_type_sec = "secs_left_qt"

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
                f'"{time.ctime()}": {game_id} - Shot data length does not match PBP data'
            )
            df["shot_x"] = np.nan
            df["shot_y"] = np.nan
            return df

        df["shot_x"] = shot_info["shot_x"]
        df["shot_y"] = shot_info["shot_y"]

    else:
        df["shot_x"] = np.nan
        df["shot_y"] = np.nan
        return df

    return df


def _get_game_info_helper(info, more_info, game_id, game_type):
    """A helper function that cleans a game's metadata.

    Parameters:
        - info: a JSON object containing game metadata
        - more_info: a JSON object containing game metadata
        - game_id: a string representing the game's ESPN game ID
        - game_type: a string representing whether men's or women's basketball is being scraped

    Returns
        - the game metadata as a DataFrame
    """
    attendance = int(info["attnd"]) if "attnd" in info.keys() else np.nan
    capacity = int(info["cpcty"]) if "cpcty" in info.keys() else np.nan
    network = info["cvrg"] if "cvrg" in info.keys() else ""

    gm_date = parse(info["dtTm"])
    game_date = gm_date.replace(tzinfo=timezone.utc).astimezone(tz=tz("US/Pacific"))
    game_day = game_date.strftime("%B %d, %Y")
    game_time = game_date.strftime("%I:%M %p %Z")

    arena = info["loc"] if "loc" in info.keys() else ""
    loc = (
        info["locAddr"]["city"] + ", " + info["locAddr"]["state"]
        if "locAddr" in info.keys()
        else ""
    )

    tot_refs = info["refs"] if "refs" in info.keys() else {}
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
        home_id = "nd-" + re.sub("[^0-9a-zA-Z\-]", "", ht)
    elif len(ht_info["records"]) == 0:
        ht = home_team.lower().replace(" ", "-")
        home_id = "nd-" + re.sub("[^0-9a-zA-Z\-]", "", ht)

    if len(at_info["links"]) == 0:
        at = away_team.lower().replace(" ", "-")
        away_id = "nd-" + re.sub("[^0-9a-zA-Z\-]", "", at)
    elif len(at_info["records"]) == 0:
        at = away_team.lower().replace(" ", "-")
        away_id = "nd-" + re.sub("[^0-9a-zA-Z\-]", "", at)

    home_rank = ht_info["rank"] if "rank" in ht_info.keys() else np.nan
    away_rank = at_info["rank"] if "rank" in at_info.keys() else np.nan

    home_record = (
        ht_info["records"][0]["displayValue"] if len(ht_info["records"]) > 0 else ""
    )
    away_record = (
        at_info["records"][0]["displayValue"] if len(at_info["records"]) > 0 else ""
    )

    home_score, away_score = int(ht_info["score"]), int(at_info["score"])

    home_win = True if home_score > away_score else False

    is_postseason = True if more_info["seasonType"] == 3 else False
    is_conference = more_info["isConferenceGame"]

    if len(ht_info["records"]) > 1 and ht_info["records"][1]["type"] == "home":
        is_neutral = False

    elif len(at_info["records"]) > 1 and at_info["records"][1]["type"] == "away":
        is_neutral = False

    else:
        is_neutral = True

    tournament = more_info["nte"] if "nte" in more_info.keys() else ""

    if ("linescores" in ht_info) and ("linescores" in at_info):
        if game_type == "mens":
            h_ot, a_ot = len(ht_info["linescores"]) - 2, len(at_info["linescores"]) - 2
        else:
            h_ot, a_ot = len(ht_info["linescores"]) - 4, len(at_info["linescores"]) - 4

        assert h_ot == a_ot
        num_ots = h_ot
    else:
        _log.warning(f'"{time.ctime()}": {game_id} - No score info available')
        num_ots = -1

    game_info_list = [
        game_id,
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
