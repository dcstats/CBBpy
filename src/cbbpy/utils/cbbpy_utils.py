from bs4 import BeautifulSoup as bs
from curl_cffi import requests as r
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
from functools import lru_cache, wraps


class CBBpyWarning(Warning):
    # category for cbbpy's user-facing warnings (fuzzy-match/fallback notices)
    # so consumers can silence them without touching other warnings
    pass


ATTEMPTS = 15
# seconds per request; a hung connection raises into the retry loop (#70)
REQUEST_TIMEOUT = 30
DATE_PARSES = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m-%d-%Y",
    "%m/%d/%Y",
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
# logos are school-level assets keyed by ESPN team ID, shared across genders
TEAM_LOGO_URL = "https://a.espncdn.com/i/teamlogos/ncaa/500/{}.png"
TEAM_LOGO_DARK_URL = "https://a.espncdn.com/i/teamlogos/ncaa/500-dark/{}.png"
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
# AWS WAF challenges Chromium TLS fingerprints as of July 2026; Safari passes
IMPERSONATE = "safari"
WINDOW_STRING = "window['__espnfitt__']="
JSON_REGEX = r"window\[\'__espnfitt__\'\]={(.*)};"
STATUS_OK = 200
WOMEN_HALF_RULE_CHANGE_DATE = parser.parse("2015-05-01")
GOOD_GAME_STATUSES = ['In Progress', 'Final']


# logging setup
log_dir = user_log_dir(appname="CBBpy", appauthor="Daniel Cowan", version=version("cbbpy"))
log_file = os.path.join(log_dir, "CBBpy.log")


class _LazyFileHandler(logging.FileHandler):
    # delay=True + creating the dir in _open means importing cbbpy does no
    # filesystem I/O; the log dir/file appear only once something is logged
    def _open(self):
        os.makedirs(log_dir, exist_ok=True)
        return super()._open()


file_handler = _LazyFileHandler(log_file, delay=True)
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
        start = time.time()
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            _call_depth[0] -= 1  # Decrement call depth
            if _call_depth[0] == 0:
                # mtime check (not file_handler.stream) because joblib workers
                # log errors in their own processes, not the parent's handler
                try:
                    errors_logged = os.path.getmtime(log_file) >= start
                except OSError:
                    errors_logged = False
                if errors_logged:
                    print(f"Errors were logged; see {log_file}")
    return wrapper


# pnf_ will keep track of games w/ page not found errors
# if game has error, don't run the other scrape functions to save time
pnf_ = []


class CouldNotParseError(Exception):
    pass


class InvalidDateRangeError(Exception):
    pass


def _validate_source(source):
    if source not in ("api", "html"):
        raise ValueError(f"source must be 'api' or 'html', got {source!r}")


def _get_game(game_id, game_type, info, box, pbp, source="api", throttle=0):
    _validate_source(source)
    # baseline politeness delay before each game's requests (#79); jittered
    # ±50% around the mean so parallel workers don't fire in lockstep
    if throttle:
        time.sleep(np.random.uniform(0.5 * throttle, 1.5 * throttle))
    game_id = str(game_id)
    game_info_df = boxscore_df = pbp_df = pd.DataFrame([])

    if source == "api":
        from cbbpy.utils import espn_api

        # one summary request serves info + boxscore + pbp for the game
        summary = (
            espn_api._fetch_summary(game_id, game_type)
            if (info or box or pbp) and game_id not in pnf_
            else None
        )

        # summary is None for a real 404 (game in pnf_) or any other persistent
        # failure, already logged by _fetch_summary; distinguish the two here
        if info:
            if game_id in pnf_:
                _log.error(f'{game_id} - Game Info: Page not found error')
            elif summary is None:
                _log.error(f'{game_id} - Game Info: summary unavailable')
            else:
                game_info_df = espn_api._get_game_info_api(game_id, game_type, summary)

        if box:
            if game_id in pnf_:
                _log.error(f'{game_id} - Boxscore: Page not found error')
            elif summary is None:
                _log.error(f'{game_id} - Boxscore: summary unavailable')
            else:
                boxscore_df = espn_api._get_game_boxscore_api(game_id, game_type, summary)

        if pbp:
            if game_id in pnf_:
                _log.error(f'{game_id} - PBP: Page not found error')
            elif summary is None:
                _log.error(f'{game_id} - PBP: summary unavailable')
            else:
                pbp_df = espn_api._get_game_pbp_api(game_id, game_type, summary)

        return (game_info_df, boxscore_df, pbp_df)

    if game_id in pnf_:
        _log.error(f'{game_id} - Game Info: Page not found error')
    elif info:
        game_info_df = _get_game_info(game_id, game_type, source)

    if game_id in pnf_:
        _log.error(f'{game_id} - Boxscore: Page not found error')
    elif box:
        boxscore_df = _get_game_boxscore(game_id, game_type, source)

    if game_id in pnf_:
        _log.error(f'{game_id} - PBP: Page not found error')
    elif pbp:
        pbp_df = _get_game_pbp(game_id, game_type, source)

    return (game_info_df, boxscore_df, pbp_df)


@print_log_file_location
def _get_games_range(
    start_date, end_date, game_type, info, box, pbp, source="api",
    throttle=0.5, n_jobs=None,
):
    _validate_source(source)
    if isinstance(start_date, str):
        start_date = _parse_date(start_date)
    if isinstance(end_date, str):
        end_date = _parse_date(end_date)
    date_range = pd.date_range(start_date, end_date)
    len_scrape = len(date_range)
    all_data = []
    cpus = n_jobs if n_jobs is not None else max((os.cpu_count() or 2) - 1, 1)

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
            game_ids = _get_game_ids(date, game_type, source)
            t.set_description(f"Scraping {len(game_ids)} games on {date.strftime('%D')}")

            if len(game_ids) > 0:
                result = Parallel(n_jobs=cpus)(
                    delayed(_get_game)(gid, game_type, info, box, pbp, source, throttle)
                    for gid in game_ids
                )
                all_data.append(result)

            else:
                t.set_description(f"No games on {date.strftime('%D')}", refresh=False)

    if not len(all_data) > 0:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    # sort returned dataframes to ensure consistency between runs
    game_info_df = pd.concat([game[0] for day in all_data for game in day])
    if info:
        # fixed-width ISO-8601 UTC strings sort correctly lexicographically (#80)
        game_info_df = game_info_df.sort_values(
            by=['game_datetime', 'game_id']
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
def _get_games_season(
    season, game_type, info, box, pbp, source="api", throttle=0.5, n_jobs=None
):
    _validate_source(source)
    season_start_date = f"{season-1}-11-01"
    season_end_date = f"{season}-05-01"

    # if season has not started yet, throw error
    if datetime.strptime(season_start_date, "%Y-%m-%d") > datetime.today():
        raise InvalidDateRangeError("The start date must not be in the future.")

    # if season has not ended yet, set end scrape date to today
    if datetime.strptime(season_end_date, "%Y-%m-%d") > datetime.today():
        season_end_date = datetime.today().strftime("%Y-%m-%d")

    info = _get_games_range(
        season_start_date, season_end_date, game_type, info, box, pbp, source,
        throttle, n_jobs,
    )

    return info


@print_log_file_location
def _get_games_team(
    team, season, game_type, info, box, pbp, source="api", throttle=0.5, n_jobs=None
):
    _validate_source(source)
    cpus = n_jobs if n_jobs is not None else max((os.cpu_count() or 2) - 1, 1)
    schedule_df = _get_team_schedule(team, season, game_type)

    if schedule_df.empty:
        _log.error(f'{team} - Schedule unavailable, cannot scrape games')
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    game_ids = list(schedule_df[schedule_df.game_status.isin(GOOD_GAME_STATUSES)].game_id)

    _log.info(f'Scraping {len(game_ids)} games for {schedule_df.team.iloc[0]}')

    if not game_ids:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    result = Parallel(n_jobs=cpus)(
        delayed(_get_game)(gid, game_type, info, box, pbp, source, throttle)
        for gid in game_ids
    )

    # sort returned dataframes to ensure consistency between runs
    game_info_df = pd.concat([x[0] for x in result])
    if info:
        # fixed-width ISO-8601 UTC strings sort correctly lexicographically (#80)
        game_info_df = game_info_df.sort_values(
            by=['game_datetime', 'game_id']
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
def _get_games_conference(
    conference, season, game_type, info, box, pbp, source="api",
    throttle=0.5, n_jobs=None,
):
    _validate_source(source)
    teams = _get_teams_from_conference(conference, season, game_type)
    result = [
        _get_games_team(x, season, game_type, info, box, pbp, source, throttle, n_jobs)
        for x in teams
    ]

    # intra-conference games appear on both teams' schedules; keep only the
    # first scraped copy of each game (#84)
    def _drop_repeat_games(frames):
        seen = set()
        deduped = []
        for f in frames:
            if 'game_id' in f.columns:
                keep = ~f.game_id.isin(seen)
                seen.update(f.game_id)
                f = f[keep]
            deduped.append(f)
        return deduped

    # sort returned dataframes to ensure consistency between runs
    game_info_df = pd.concat(_drop_repeat_games([x[0] for x in result]))
    if info:
        # fixed-width ISO-8601 UTC strings sort correctly lexicographically (#80)
        game_info_df = game_info_df.sort_values(
            by=['game_datetime', 'game_id']
        ).reset_index(drop=True)

    game_boxscore_df = pd.concat(_drop_repeat_games([x[1] for x in result]))
    if box:
        game_boxscore_df = game_boxscore_df.sort_values(
            by=['game_id', 'team'], 
            ascending=False, 
            kind='mergesort'
        ).reset_index(drop=True)

    game_pbp_df = pd.concat(_drop_repeat_games([x[2] for x in result]))
    if pbp:
        game_pbp_df = game_pbp_df.sort_values(
            by=['game_id'],
            ascending=False,
            kind='mergesort'
        ).reset_index(drop=True)

    return (game_info_df, game_boxscore_df, game_pbp_df)


def _get_game_ids(date, game_type, source="api"):
    _validate_source(source)
    if source == "api":
        from cbbpy.utils import espn_api

        return espn_api._get_game_ids_api(date, game_type)

    soup = None
    scoreboard = None
    ids = []

    if game_type == "mens":
        pre_url = MENS_SCOREBOARD_URL
    else:
        pre_url = WOMENS_SCOREBOARD_URL

    if isinstance(date, str):
        date = _parse_date(date)

    for i in range(ATTEMPTS):
        try:
            header = {
                "Referer": str(np.random.choice(REFERERS)),
            }
            d = date.strftime("%Y%m%d")
            url = pre_url.format(d)
            page = r.get(url, headers=header, impersonate=IMPERSONATE, timeout=REQUEST_TIMEOUT)
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
                return []
            else:
                # try again with a random sleep
                time.sleep(np.random.uniform(low=1, high=3))
                continue
        else:
            # no exception thrown
            break

    return ids


def _get_game_boxscore(game_id, game_type, source="api"):
    _validate_source(source)
    if source == "api":
        from cbbpy.utils import espn_api

        return espn_api._get_game_boxscore_api(game_id, game_type)

    soup = None
    gamepackage = None
    game_id = str(game_id)

    if game_type == "mens":
        pre_url = MENS_BOXSCORE_URL
    else:
        pre_url = WOMENS_BOXSCORE_URL

    for i in range(ATTEMPTS):
        try:
            header = {
                "Referer": str(np.random.choice(REFERERS)),
            }
            url = pre_url.format(game_id)
            page = r.get(url, headers=header, impersonate=IMPERSONATE, timeout=REQUEST_TIMEOUT)
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


def _get_win_prob_html(game_id, game_type):
    """Fetch the game page's win-probability block (absent from the PBP page).

    Returns {play_id: home_win_prob in [0,1]}, or {} on failure / when ESPN has
    no win-prob data. ESPN stores the AWAY win pct on a 0-100 scale, so flip and
    rescale to a home probability.
    """
    pre_url = MENS_GAME_URL if game_type == "mens" else WOMENS_GAME_URL
    try:
        header = {"Referer": str(np.random.choice(REFERERS))}
        page = r.get(pre_url.format(game_id), headers=header, impersonate=IMPERSONATE, timeout=REQUEST_TIMEOUT)
        gamepackage = _get_gamepackage_from_soup(bs(page.content, "lxml"))
        pts = (gamepackage.get("wnPrb") or {}).get("pts") or {}
    except Exception as ex:
        _log.warning(f'{game_id} - PBP: win probability fetch failed, leaving home_win_prob empty\n{ex}')
        return {}

    return {
        str(k): (100 - v["a"]) / 100 for k, v in pts.items() if "a" in v
    }


def _get_game_pbp(game_id, game_type, source="api"):
    _validate_source(source)
    if source == "api":
        from cbbpy.utils import espn_api

        return espn_api._get_game_pbp_api(game_id, game_type)

    soup = None
    gamepackage = None
    game_id = str(game_id)

    if game_type == "mens":
        pre_url = MENS_PBP_URL
    else:
        pre_url = WOMENS_PBP_URL

    for i in range(ATTEMPTS):
        try:
            header = {
                "Referer": str(np.random.choice(REFERERS)),
            }
            url = pre_url.format(game_id)
            page = r.get(url, headers=header, impersonate=IMPERSONATE, timeout=REQUEST_TIMEOUT)
            soup = bs(page.content, "lxml")
            gamepackage = _get_gamepackage_from_soup(soup)

            # check if game was postponed
            gm_status = gamepackage["gmStrp"]["status"]["desc"]
            gsbool = gm_status in GOOD_GAME_STATUSES
            if not gsbool:
                _log.warning(f'{game_id} - {gm_status}')
                return pd.DataFrame([])

            # win probability lives on the game page, not the PBP page, so fetch
            # it separately (best-effort; leaves home_win_prob NaN on failure)
            gamepackage["win_prob"] = _get_win_prob_html(game_id, game_type)

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


def _get_game_info(game_id, game_type, source="api"):
    _validate_source(source)
    if source == "api":
        from cbbpy.utils import espn_api

        return espn_api._get_game_info_api(game_id, game_type)

    soup = None
    gamepackage = None
    game_id = str(game_id)

    if game_type == "mens":
        pre_url = MENS_GAME_URL
    else:
        pre_url = WOMENS_GAME_URL

    for i in range(ATTEMPTS):
        try:
            header = {
                "Referer": str(np.random.choice(REFERERS)),
            }
            url = pre_url.format(game_id)
            page = r.get(url, headers=header, impersonate=IMPERSONATE, timeout=REQUEST_TIMEOUT)
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
    raw_player = None
    df = pd.DataFrame([])

    if game_type == "mens":
        pre_url = MENS_PLAYER_URL
    else:
        pre_url = WOMENS_PLAYER_URL

    for i in range(ATTEMPTS):
        try:
            header = {
                "Referer": str(np.random.choice(REFERERS)),
            }
            url = pre_url.format(player_id)
            page = r.get(url, headers=header, impersonate=IMPERSONATE, timeout=REQUEST_TIMEOUT)
            soup = bs(page.content, "lxml")
            raw_player = _get_player_from_soup(soup)

            df = _get_player_details_helper(player_id, raw_player, game_type)

        except Exception as ex:
            if soup is not None and "Page not found." in soup.text:
                _log.error(
                    f'{player_id} - Player: Page not found error'
                )
                return pd.DataFrame([])

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
                "Referer": str(np.random.choice(REFERERS)),
            }
            url = pre_url.format(team_id, season)
            page = r.get(url, headers=header, impersonate=IMPERSONATE, timeout=REQUEST_TIMEOUT)
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
        except ValueError:
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


def _build_player_rows(players, team_name, game_id, labels, is_starter):
    cols = ["starter", "position", "player_id", "player", "team", "game_id"] + [
        x.lower() for x in labels
    ]
    if len(players) == 0:
        return pd.DataFrame(columns=cols)

    stat_dict = {
        labels[i].lower(): [players[j]["stats"][i] for j in range(len(players))]
        for i in range(len(labels))
    }
    positions = [x["athlt"].get("pos", "") for x in players]
    # uid works for both transports: HTML embeds "s:40~l:41~a:<id>", the API
    # adapter passes the bare athlete id (no colons)
    ids = [
        x["athlt"]["uid"].split(":")[-1] if "uid" in x["athlt"] else ""
        for x in players
    ]
    names = [x["athlt"].get("shrtNm", "") for x in players]

    df = pd.DataFrame(stat_dict)
    df.insert(0, "starter", is_starter)
    df.insert(0, "position", positions)
    df.insert(0, "player_id", ids)
    df.insert(0, "player", names)
    df.insert(0, "team", team_name)
    df.insert(0, "game_id", game_id)
    return df


def _build_totals_row(totals, team_name, game_id, labels):
    cols = ["starter", "position", "player_id", "player", "team", "game_id"] + [
        x.lower() for x in labels
    ]
    if len(totals) == 0:
        return pd.DataFrame(columns=cols)

    tot_dict = {labels[i].lower(): [totals[i]] for i in range(len(labels))}
    df = pd.DataFrame(tot_dict)
    df.insert(0, "starter", False)
    df.insert(0, "position", "TOTAL")
    df.insert(0, "player_id", "TOTAL")
    df.insert(0, "player", "TEAM")
    df.insert(0, "team", team_name)
    df.insert(0, "game_id", game_id)
    return df


def _build_team_df(stats, team_name, game_id, labels):
    return pd.concat([
        _build_player_rows(stats[0]["athlts"], team_name, game_id, labels, True),
        _build_player_rows(stats[1]["athlts"], team_name, game_id, labels, False),
        _build_totals_row(stats[2]["ttls"], team_name, game_id, labels),
    ])


def _get_game_boxscore_helper(boxscore, game_id):
    tm1, tm2 = boxscore[0], boxscore[1]
    tm1_name, tm2_name = tm1["tm"]["dspNm"], tm2["tm"]["dspNm"]
    labels = tm1["stats"][0]["lbls"]

    tm1_df = _build_team_df(tm1["stats"], tm1_name, game_id, labels)
    tm2_df = _build_team_df(tm2["stats"], tm2_name, game_id, labels)

    df = pd.concat([tm1_df, tm2_df])

    if len(df) <= 0:
        _log.warning(f'{game_id} - No boxscore available')
        return pd.DataFrame([])

    # SPLIT UP THE FG FIELDS (made-attempted strings; NaN when malformed)
    def _split_stat(series, idx):
        return pd.to_numeric(
            [x.split("-")[idx] if isinstance(x, str) and "-" in x else np.nan for x in series],
            errors="coerce",
        )

    fgm = _split_stat(df["fg"], 0)
    fga = _split_stat(df["fg"], 1)
    thpm = _split_stat(df["3pt"], 0)
    thpa = _split_stat(df["3pt"], 1)
    ftm = _split_stat(df["ft"], 0)
    fta = _split_stat(df["ft"], 1)

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
    home_team = pbp["tms"]["home"]["nm"]
    away_team = pbp["tms"]["away"]["nm"]
    game_date = parser.parse(gamepackage["gmInfo"]["dtTm"])

    all_plays = pbp["plays"]

    # check if PBP exists
    if len(all_plays) <= 0:
        _log.warning(f'{game_id} - No PBP available')
        return pd.DataFrame([])

    play_ids = [str(x.get('id', '')) for x in all_plays]
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
        int(x["hmScr"]) if "hmScr" in x.keys() else np.nan for x in all_plays
    ]
    ascores = [
        int(x["awScr"]) if "awScr" in x.keys() else np.nan for x in all_plays
    ]
    periods = [
        int(x["period"]["number"]) if "period" in x.keys() else np.nan
        for x in all_plays
    ]

    # a missing or malformed clock (no colon, non-numeric) degrades to 0:00
    # for that play instead of crashing the whole game's parse
    def _clock_parts(play):
        parts = (play.get("clock") or {}).get("displayValue", "").split(":")
        if len(parts) != 2:
            return 0, 0
        try:
            return int(float(parts[0])), int(float(parts[1]))
        except ValueError:
            return 0, 0

    clock_parts = [_clock_parts(x) for x in all_plays]
    minutes = [m for m, _ in clock_parts]
    seconds = [s for _, s in clock_parts]
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
            # case-insensitive: ESPN lowercased play texts in recent seasons
            # ("misses 12-foot jumper" vs the older "missed Jumper.")
            if pt.lower() in play.lower():
                p_types.append(pt.lower())
                added = True
                break
        if not added:
            for st in SHOT_TYPES:
                if st.lower() in play.lower():
                    p_types.append(st.lower())
                    added = True
                    break

        if not added:
            p_types.append("")

    # FIND SHOOTERS
    # prefer ESPN's structured shootingPlay flag when the play carries it; fall
    # back to the text-derived type (older embeds omit the flag)
    shooting_play = [
        (
            bool(x.get("shootingPlay"))
            if "shootingPlay" in x
            else p in (y.lower() for y in SHOT_TYPES) or sc_play[i]
        )
        for i, (x, p) in enumerate(zip(all_plays, p_types))
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

    # STRUCTURED FIELDS FROM ESPN JSON (may be absent, esp. in older games)
    player_ids = [str((x.get("athlete") or {}).get("id", "")) for x in all_plays]
    assist_player_ids = [
        next(
            (
                str(p.get("id", ""))
                for p in (x.get("participants") or [])
                if p.get("description") == "AST"
            ),
            "",
        )
        for x in all_plays
    ]
    type_txts = [(x.get("type") or {}).get("txt", "") for x in all_plays]
    play_type_ids = [str((x.get("type") or {}).get("id", "")) for x in all_plays]

    # play_type carries the structured JSON type verbatim, falling back to the
    # lossy text-parsed value only when the JSON type is absent (rare to never)
    play_types = [txt if txt else fb for txt, fb in zip(type_txts, p_types)]

    # primary actor's full name from the JSON (any attributed play); when the
    # embed omits it, fall back to the text-parsed shooter on shooting plays only
    json_player_names = [((x.get("athlete") or {}).get("name", "") or "") for x in all_plays]
    player_names = [
        jn if jn else (shooters[i] if shooting_play[i] else "")
        for i, jn in enumerate(json_player_names)
    ]

    # assister's full name from the JSON, falling back to the text parse
    json_assist_names = [
        next(
            (
                p.get("name", "") or ""
                for p in (x.get("participants") or [])
                if p.get("description") == "AST"
            ),
            "",
        )
        for x in all_plays
    ]
    assist_players = [
        jn if jn else assisted_pls[i] for i, jn in enumerate(json_assist_names)
    ]

    # play-level shot coordinates (sole source when no shot chart, else fallback)
    play_shot_xs = []
    play_shot_ys = []
    for x, is_shot in zip(all_plays, shooting_play):
        coord = x.get("coordinate") or {}
        if not is_shot or "x" not in coord or "y" not in coord:
            play_shot_xs.append(np.nan)
            play_shot_ys.append(np.nan)
            continue
        cx = int(coord["x"])
        cy = int(coord["y"])
        if cx < 0 or cy < 0:
            play_shot_xs.append(np.nan)
            play_shot_ys.append(np.nan)
        else:
            play_shot_xs.append(50 - cx)
            play_shot_ys.append(cy)

    # home win probability [0,1] per play, keyed by play id; both sources feed a
    # pre-normalized {play_id: home_prob} map (API from summary["winprobability"],
    # HTML from the game page's wnPrb block), NaN where a play has no win-prob point
    wp_map = gamepackage.get("win_prob") or {}
    home_win_prob = [wp_map.get(pid, np.nan) for pid in play_ids]

    data = {
        "id": play_ids,
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
        "play_type": play_types,
        "play_type_id": play_type_ids,
        "shooting_play": shooting_play,
        "scoring_play": sc_play,
        "is_three": is_three,
        "player_name": player_names,
        "is_assisted": is_assisted,
        "assist_player": assist_players,
        "player_id": player_ids,
        "assist_player_id": assist_player_ids,
        "shot_x": play_shot_xs,
        "shot_y": play_shot_ys,
        "home_win_prob": home_win_prob,
    }

    df = pd.DataFrame(data)

    # add shot data if it exists
    is_shotchart = "shtChrt" in gamepackage

    if is_shotchart:
        chart = gamepackage["shtChrt"]["plays"]

        ids = [str(x.get('id', '')) for x in chart]
        # shotteams = [x.get('homeAway', '') for x in chart]
        # shotdescs = [x.get('text', '') for x in chart]
        xs = [50-int((x.get('coordinate') or {}).get('x', -100)) for x in chart]
        ys = [int((x.get('coordinate') or {}).get('y', -100)) for x in chart]

        shot_data = {
            "id": ids,
            # "team": shotteams,
            # "play_desc": shotdescs,
            "x": xs,
            "y": ys
        }

        shot_df = pd.DataFrame(shot_data)

        # match shot data to pbp data
        df_merged = df.merge(shot_df, left_on='id', right_on='id', how='left', suffixes=('', '_shot'))

        if len(shot_df[~shot_df['id'].isin(df_merged['id'])]) > 0:
            _log.warning(f'{game_id} - Some shot data could not be matched to PBP data')

        # chart coordinates take precedence; keep play-level coords as fallback
        df['shot_x'] = df_merged['x'].where(df_merged['x'].notna(), df['shot_x'])
        df['shot_y'] = df_merged['y'].where(df_merged['y'].notna(), df['shot_y'])

    return df.sort_values(by=[pd_type, pd_type_sec], ascending=[True, False])


def _compute_num_ots(home_ls, away_ls, game_id, game_type, game_date):
    """Derive the number of OTs from both teams' linescores.

    Returns -1 when either linescore is missing. If the two disagree (ESPN
    occasionally publishes a truncated linescore for one team), warn and use
    the larger of the two rather than raising.
    """
    if not home_ls or not away_ls:
        _log.warning(f"{game_id} - No score info available")
        return -1

    # men, and women before the 15-16 season, use halves
    if game_type == "mens" or game_date.replace(tzinfo=None) < WOMEN_HALF_RULE_CHANGE_DATE:
        regulation = 2
    # women (after 14-15) use quarters
    else:
        regulation = 4

    h_ot, a_ot = len(home_ls) - regulation, len(away_ls) - regulation

    if h_ot != a_ot:
        _log.warning(
            f"{game_id} - Inconsistent linescore lengths "
            f"(home: {len(home_ls)}, away: {len(away_ls)}); using {max(h_ot, a_ot)} OTs"
        )

    return max(h_ot, a_ot)


def _get_game_info_helper(gamepackage, game_id, game_type):
    info = gamepackage["gmInfo"]
    more_info = gamepackage["gmStrp"]

    attendance = float(info.get("attnd", np.nan))
    capacity = float(info.get("cpcty", np.nan))
    network = info.get("cvrg", "")

    gm_date = parser.parse(info["dtTm"])
    game_datetime = gm_date.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    game_date = gm_date.replace(tzinfo=timezone.utc).astimezone(tz=tz("US/Pacific"))
    # game_day/game_time (Pacific) are deprecated in favor of game_datetime; removal in 3.0
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
    ht_info = next(team for team in teams if team['isHome'])
    at_info = next(team for team in teams if not team['isHome'])

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

    # TODO: fix for in-progress games which are first of conference play
    vs_conf = [x for y in more_info['tms'] for x in y['records'] if x['type'] == 'vsconf']
    # is a conference game if both teams have a vsconf record and neither is 0-0
    is_conference = True if len(vs_conf) == 2 and any(x['summary'] != '0-0' for x in vs_conf) else False

    if "neutralSite" in more_info:
        is_neutral = True
    else:
        is_neutral = False

    tournament = more_info.get("nte", "")

    # use number of entries in scoreline to determine number of OTs
    num_ots = _compute_num_ots(
        ht_info.get("linescores"), at_info.get("linescores"), game_id, game_type, game_date
    )

    try:
        home_spread = gamepackage['gameOdds']['odds'][-1]['pointSpread']['primary']
    except (KeyError, IndexError, TypeError):
        home_spread = ''

    # over/under and moneylines: the archived HTML embed carries no gameOdds, so
    # these stay NaN for recorded games (the API path fills them from pickcenter;
    # all three are on the parity exclusion list)
    over_under = np.nan
    home_ml = np.nan
    away_ml = np.nan

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
        game_datetime,
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
        over_under,
        home_ml,
        away_ml,
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
        "game_datetime",
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
        "over_under",
        "home_moneyline",
        "away_moneyline",
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
        mat = re.search(r'gameId/(\d+)/', ev.get('time', {}).get('link', ''))
        game_id = mat.group(1) if mat is not None else ''

        gm_dt = parser.parse(ev['date']['date'])
        game_datetime = gm_dt.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        date = gm_dt.astimezone(tz('America/Los_Angeles'))
        # game_day/game_time (Pacific) are deprecated in favor of game_datetime; removal in 3.0
        day = date.strftime('%B %d, %Y')
        time = date.strftime('%I:%M %p %Z')

        opp_info = ev.get('opponent', {})
        opp = opp_info.get('displayName', '')
        opp_id = opp_info.get('id', '')

        network_list = ev.get('network', [])
        network = network_list[0]['name'] if len(network_list) > 0 else ''
        season_type = ev.get('seasonType', {}).get('name', '')
        status = ev.get('status', {}).get('description', '')

        res = ev.get('result', {})

        if status == 'Final' and res:
            result = res.get('winLossSymbol', '') + ' ' + res.get('currentTeamScore', '') + '-' + res.get('opponentTeamScore', '')
        else:
            result = 'N/A'

        row = (team, id_, season, game_id, game_datetime, day, time, opp, opp_id, season_type, status, network, result)
        data.append(row)

    cols = [
        'team',
        'team_id',
        'season',
        'game_id',
        'game_datetime',
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
    # fixed-width ISO-8601 UTC strings sort correctly lexicographically
    df = df.sort_values(by=['team', 'game_datetime'])

    return df.reset_index(drop=True)


@lru_cache(maxsize=2)
def _get_team_map(game_type):
    data_path = Path(__file__).parent / f'{game_type}_team_map.csv'
    return pd.read_csv(data_path)


def _resolve_map_season(team_map_df, season):
    # the team map is static; when the requested season isn't in it yet
    # (e.g. a new season before the CSVs are updated), fall back to the
    # latest available season since most teams/conferences don't change YOY
    if (team_map_df.season == season).any():
        return season
    fallback = int(team_map_df.season.max())
    warnings.warn(
        f"No team map data for the {season} season. Falling back to {fallback}.",
        CBBpyWarning,
        stacklevel=2,
    )
    return fallback


def _get_id_from_team(team, season, game_type):
    # fetch list of teams and team IDs for given season
    season = int(season)
    team_map_df = _get_team_map(game_type)
    season = _resolve_map_season(team_map_df, season)
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

        warnings.warn(
            f"No exact match for '{team}'. Fetching closest team match: '{best_match}'.",
            CBBpyWarning,
            stacklevel=2,
        )
        
        id_ = id_map[best_match]
    else:
        best_match = lowercase_map[team.lower()]
        id_ = id_map[best_match]

    return id_, best_match


def _get_team_logos(teams, season, dest, game_type, dark=False, overwrite=False):
    season = int(season)
    team_map_df = _get_team_map(game_type)
    season = _resolve_map_season(team_map_df, season)

    if teams is None:
        season_map = team_map_df[team_map_df.season == season]
        pairs = list(season_map[["location", "id"]].itertuples(index=False, name=None))
    else:
        if isinstance(teams, str):
            teams = [teams]
        pairs = [_get_id_from_team(t, season, game_type)[::-1] for t in teams]

    url = TEAM_LOGO_DARK_URL if dark else TEAM_LOGO_URL
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)

    rows = []
    for name, id_ in pairs:
        path = dest / f"{id_}.png"
        if path.exists() and not overwrite:
            rows.append((name, id_, str(path)))
            continue
        try:
            header = {"Referer": str(np.random.choice(REFERERS))}
            resp = r.get(url.format(id_), headers=header, impersonate=IMPERSONATE, timeout=REQUEST_TIMEOUT)
            if resp.status_code == STATUS_OK and resp.content:
                path.write_bytes(resp.content)
                rows.append((name, id_, str(path)))
            else:
                _log.warning(f'"{name}" logo: request returned status {resp.status_code}')
                rows.append((name, id_, None))
        except Exception as ex:
            _log.error(f'"{name}" logo: {ex}')
            rows.append((name, id_, None))

    return pd.DataFrame(rows, columns=["team", "id", "logo_path"])


def _get_season_conferences(season, game_type):
    season = int(season)
    team_map_df = _get_team_map(game_type)
    season = _resolve_map_season(team_map_df, season)
    confs_df = team_map_df[team_map_df.season == season][['conference', 'conference_abb']].drop_duplicates()
    return confs_df.reset_index(drop=True)


def _get_teams_from_conference(conference, season, game_type):
    # fetch list of teams and team IDs for given season
    season = int(season)
    team_map_df = _get_team_map(game_type)
    season = _resolve_map_season(team_map_df, season)
    confs_df = _get_season_conferences(season, game_type)
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

        warnings.warn(
            f"No exact match for '{conference}'. Fetching closest conference match: '{best_match}'.",
            CBBpyWarning,
            stacklevel=2,
        )
    else:
        best_match = lowercase_map[conference.lower()]

        # if matched abbreviation, swap for conference name
        if best_match in abb_map:
            best_match = abb_map[best_match]

    # filter teams df to relevant conference
    rel_team_df = team_map_df[(team_map_df.season == season) & (team_map_df.conference == best_match)]

    return rel_team_df.location.tolist()


def _parse_espn_json(soup):
    script_string = _find_json_in_content(soup)

    if script_string == "":
        return None

    pattern = re.compile(JSON_REGEX)
    match = re.search(pattern, script_string)
    if match is None:
        return None

    js = "{" + match.group(1) + "}"
    return json.loads(js)


def _get_json_from_soup(soup):
    return _parse_espn_json(soup)


def _get_gamepackage_from_soup(soup):
    jsn = _parse_espn_json(soup)
    if jsn is None:
        return None
    return jsn["page"]["content"]["gamepackage"]


def _get_player_from_soup(soup):
    jsn = _parse_espn_json(soup)
    if jsn is None:
        return None
    return jsn["page"]["content"]["player"]


def _get_scoreboard_from_soup(soup):
    jsn = _parse_espn_json(soup)
    if jsn is None:
        return None
    return jsn["page"]["content"]["scoreboard"]["evts"]


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