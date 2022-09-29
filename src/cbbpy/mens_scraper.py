"""
A tool to scrape data for NCAA D1 Men's college basketball games.

Author: Daniel Cowan
"""


from bs4 import BeautifulSoup as bs
import requests as r
import pandas as pd
import numpy as np
import html
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
from pytz import timezone as tz
from tqdm import trange
import re
import time
import logging
import traceback
from typing import Union


logging.basicConfig(filename='cbbpy.log')
_log = logging.getLogger(__name__)

ATTEMPTS = 10
DATE_PARSES = [
    '%Y-%m-%d',
    '%Y/%m/%d',
    '%m-%d-%Y',
    '%m/%d/%Y',
]
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 ' +
    '(KHTML, like Gecko) Chrome/21.0.1180.83 Safari/537.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 ' +
    '(KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36',
]
REFERERS = [
    'https://google.com/',
    'https://youtube.com/',
    'https://facebook.com/',
    'https://twitter.com/',
    'https://nytimes.com/',
    'https://washingtonpost.com/',
    'https://linkedin.com/',
    'https://nhl.com/',
    'https://mlb.com/',
    'https://nfl.com/'
]
SCOREBOARD_URL = (
    "https://www.espn.com/mens-college-basketball/scoreboard/_/date/{}/seasontype/2/group/50"
)
GAME_URL = "https://www.espn.com/mens-college-basketball/game/_/gameId/{}"
BOXSCORE_URL = "https://www.espn.com/mens-college-basketball/boxscore/_/gameId/{}"
PBP_URL = "https://www.espn.com/mens-college-basketball/playbyplay/_/gameId/{}"
NON_SHOT_TYPES = [
    "Turnover",
    "Steal",
    "Rebound",
    "Foul",
    "Timeout",
    "TV Timeout",
    "Block",
    "Jump Ball",
    "End",
]
SHOT_TYPES = [
    "Jumper",
    "Three Point Jumper",
    "Two Point Tip Shot",
    "Free Throw",
    "Layup",
    "Dunk",
]
BAD_GAMES = [
    "Forfeit",
    "Postponed",
    "Canceled",
    "Uncontested",
    "TBD",
    "Suspended"
]
TOURN_WORDS = [
    'tournament',
    'championship',
    'playoff',
    '1st round',
    '2nd round',
    'quarterfinal',
    'semifinal',
    'final'
]

TOURN_SPEC = [
    'cit ',
    'cbi ',
    'nit - ',
    "men's basketball championship",
    'the basketball classic',
    'vegas 16',
]


class CouldNotParseError(Exception):
    pass


def get_game(game_id: str) -> tuple:
    """A function that scrapes all game info (metadata, boxscore, play-by-play).

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - (game_info_df, boxscore_df, pbp_df), a tuple consisting of:
            -- game_info_df: a DataFrame of the game's metadata
            -- boxscore_df: a DataFrame of the game's boxscore (both teams combined)
            -- pbp_df: a DataFrame of the game's play-by-play
    """

    game_info_df = get_game_info(game_id)

    boxscore_df = get_game_boxscore(game_id)

    pbp_df = get_game_pbp(game_id)

    return (game_info_df, boxscore_df, pbp_df)


def get_game_boxscore(game_id: str) -> pd.DataFrame:
    """A function that scrapes a game's boxscore.

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - the game boxscore as a DataFrame
    """

    for i in range(ATTEMPTS):
        try:
            header = {
                'User-Agent': np.random.choice(USER_AGENTS),
                'Referer': np.random.choice(REFERERS),
            }
            url = BOXSCORE_URL.format(game_id)
            page = r.get(url, headers=header)
            soup = bs(page.content, "lxml")

            # check if game was postponed
            status_div = soup.find("div", {"class": "game-status"})
            for bad_game in BAD_GAMES:
                if bad_game in status_div.get_text():
                    _log.warning(f'"{time.ctime()}": {game_id} - {bad_game}')
                    return pd.DataFrame([])

            div = soup.find("div", {"id": "gamepackage-boxscore-module"})
            tables = div.find_all("table")
            away_table = tables[0]
            home_table = tables[1]

            # GET TEAM NAMES
            home_info = soup.find("div", {"class": "team home"})
            home_long = home_info.find(
                "span", {"class": "long-name"}).get_text()
            home_short = home_info.find(
                "span", {"class": "short-name"}).get_text()
            home_team_name = home_long + " " + home_short
            away_info = soup.find("div", {"class": "team away"})
            away_long = away_info.find(
                "span", {"class": "long-name"}).get_text()
            away_short = away_info.find(
                "span", {"class": "short-name"}).get_text()
            away_team_name = away_long + " " + away_short

            df_home = _clean_boxscore_table(
                home_table, home_team_name, game_id)
            df_away = _clean_boxscore_table(
                away_table, away_team_name, game_id)

        except Exception as ex:
            if i+1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                _log.error(
                    f'"{time.ctime()}" attempt {i+1}: {game_id} - {ex}\n{traceback.format_exc()}')
                return pd.DataFrame([])
            else:
                # try again
                time.sleep(1.5)
                continue
        else:
            # no exception thrown
            break

    return pd.concat([df_home, df_away]).reset_index(drop=True)


def get_game_pbp(game_id: str) -> pd.DataFrame:
    """A function that scrapes a game's play-by-play information.

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - the game's play-by-play information represented as a DataFrame
    """

    for i in range(ATTEMPTS):
        try:
            header = {
                'User-Agent': np.random.choice(USER_AGENTS),
                'Referer': np.random.choice(REFERERS),
            }
            url = PBP_URL.format(game_id)
            page = r.get(url, headers=header)
            soup = bs(page.content, "lxml")

            # check if game was postponed
            status_div = soup.find("div", {"class": "game-status"})
            for bad_game in BAD_GAMES:
                if bad_game in status_div.get_text():
                    _log.warning(f'"{time.ctime()}": {game_id} - {bad_game}')
                    return pd.DataFrame([])

            # GET PBP METADATA
            team_map = _get_pbp_map(soup)

            # GET PBP DATA
            div = soup.find("div", {"id": "gamepackage-play-by-play"})
            tables = div.find_all("table")

            if '0th Half' in div.get_text():
                tables = tables[1:]

            num_halves = len(tables)
            pbp_halves = []

            if num_halves == 0:
                return pd.DataFrame([])

            for i, table in enumerate(tables):
                cleaned_pbp_half = _clean_pbp_table(
                    table, (team_map, num_halves, i + 1, game_id)
                )
                pbp_halves.append(cleaned_pbp_half)

        except Exception as ex:
            if i+1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                _log.error(
                    f'"{time.ctime()}" attempt {i+1}: {game_id} - {ex}\n{traceback.format_exc()}')
                return pd.DataFrame([])
            else:
                # try again
                time.sleep(1.5)
                continue
        else:
            # no exception thrown
            break

    return pd.concat(pbp_halves).reset_index(drop=True)


def get_game_info(game_id: str) -> pd.DataFrame:
    """A function that scrapes game metadata.

    Parameters:
        - game_id: a string representing the game's ESPN game ID

    Returns
        - a DataFrame with one row and a column for each piece of metadata
    """

    for i in range(ATTEMPTS):
        try:
            header = {
                'User-Agent': np.random.choice(USER_AGENTS),
                'Referer': np.random.choice(REFERERS),
            }
            url = GAME_URL.format(game_id)
            page = r.get(url, headers=header)
            soup = bs(page.content, "lxml")

            # check if game was postponed
            status_div = soup.find("div", {"class": "game-status"})
            for bad_game in BAD_GAMES:
                if bad_game in status_div.get_text():
                    _log.warning(f'"{time.ctime()}": {game_id} - {bad_game}')
                    return pd.DataFrame([])

            # GET DIVS
            linesc_div = soup.find("div", {"id": "gamepackage-linescore-wrap"})
            home_div = soup.find("div", {"class": "team home"})
            away_div = soup.find("div", {"class": "team away"})

            # GET HOME INFO
            home_team = (
                home_div.find("span", {"class": "long-name"}).get_text()
                + " "
                + home_div.find("span", {"class": "short-name"}).get_text()
            )
            home_score = home_div.find(
                "div", {"class": "score-container"}).get_text()
            try:
                home_id_pre = home_div.find(
                    "a", {"class": "team-name"})["href"]
                home_id = [x for x in home_id_pre.split("/") if x.isdigit()][0]
                try:
                    home_rank = home_div.find(
                        "span", {"class": "rank"}).get_text()
                except:
                    home_rank = np.nan
                home_record = home_div.find(
                    "div", {"class": "record"}).get_text()
            except:
                # this case is for teams who are not D1
                ht = home_team.lower().replace(" ", "-")
                home_id = "nd-" + re.sub("[^0-9a-zA-Z\-]", "", ht)
                home_rank = np.nan
                home_record = np.nan

            # GET AWAY INFO
            away_team = (
                away_div.find("span", {"class": "long-name"}).get_text()
                + " "
                + away_div.find("span", {"class": "short-name"}).get_text()
            )
            away_score = away_div.find(
                "div", {"class": "score-container"}).get_text()
            try:
                away_id_pre = away_div.find(
                    "a", {"class": "team-name"})["href"]
                away_id = [x for x in away_id_pre.split("/") if x.isdigit()][0]
                try:
                    away_rank = away_div.find(
                        "span", {"class": "rank"}).get_text()
                except:
                    away_rank = np.nan
                away_record = away_div.find(
                    "div", {"class": "record"}).get_text()
            except:
                # this case is for teams who are not D1
                at = away_team.lower().replace(" ", "-")
                away_id = "nd-" + re.sub("[^0-9a-zA-Z\-]", "", at)
                away_rank = np.nan
                away_record = np.nan

            # GET GAME INFO
            game_info_div = soup.find(
                "div", {"data-module": "gameInformation"})
            game_date_pre = parse(
                game_info_div.find(
                    "span", {"data-behavior": "date_time"})["data-date"]
            )
            game_date = game_date_pre.replace(tzinfo=timezone.utc).astimezone(
                tz=tz("US/Pacific")
            )
            game_day = game_date.strftime("%B %d, %Y")
            game_time = game_date.strftime("%I:%M %p %Z")

            try:
                game_meta = (
                    soup.find(
                        "div", {"class": "game-details header"}).get_text().strip()
                )
            except:
                game_meta = np.nan

            home_win = True if int(home_score) > int(away_score) else False
            num_ots = len(linesc_div.find("table").find(
                "thead").find_all("th")) - 4

            try:
                game_network = (
                    game_info_div.find("div", {"class": "game-network"})
                    .get_text()
                    .strip()
                    .replace("Coverage: ", "")
                )
            except:
                game_network = np.nan

            try:
                game_arena_pre = game_info_div.find(
                    'div', {'class': 'caption-wrapper'})

                if not game_arena_pre:
                    div_loc = game_info_div.find(
                        'div', {'class': 'location-details'})
                    game_arena = div_loc.find(
                        'span', {'class': 'game-location'})

                    if game_arena:
                        game_arena = game_arena.get_text().strip()
                        game_loc = div_loc.find(
                            'div', {'class': 'game-location'}).get_text().strip()

                    else:
                        game_arena = game_info_div.find(
                            'div', {'class': 'game-location'}).get_text().strip()
                        game_loc = None
                else:
                    game_arena = game_arena_pre.get_text().strip()
                    game_loc = game_info_div.find(
                        'div', {'class': 'game-location'}).get_text().strip()
            except:
                game_arena = None
                game_loc = None

            game_cap_pre = game_info_div.find_all(
                "div", {"class": "game-info-note capacity"})

            if len(game_cap_pre) > 1:
                game_att = game_cap_pre[0].get_text(
                ).strip().replace("Attendance: ", "")
                game_cap = game_cap_pre[1].get_text(
                ).strip().replace("Capacity: ", "")
            elif len(game_cap_pre) == 1:
                info = game_cap_pre[0].get_text().strip()

                if "Capacity" in info:
                    game_att = np.nan
                    game_cap = info.replace("Capacity: ", "")
                else:
                    game_att = info.replace("Attendance: ", "")
                    game_cap = np.nan
            else:
                game_att = np.nan
                game_cap = np.nan

            try:
                game_refs = (
                    game_info_div.find(
                        "span", {"class": "game-info-note__content"})
                    .get_text()
                    .split(", ")
                )
                game_r1 = game_refs[0] if len(game_refs) > 0 else np.nan
                game_r2 = game_refs[1] if len(game_refs) > 1 else np.nan
                game_r3 = game_refs[2] if len(game_refs) > 2 else np.nan
            except:
                game_r1 = np.nan
                game_r2 = np.nan
                game_r3 = np.nan

            conf_home = 'conf' in home_div.get_text().lower()
            conf_away = 'conf' in away_div.get_text().lower()
            home_home = 'home' in home_div.get_text().lower()
            away_away = 'away' in away_div.get_text().lower()

            if conf_home and conf_away:
                is_conf = True
            else:
                is_conf = False

            if home_home or away_away:
                is_neutral = False
            elif is_conf and not type(game_meta) == str:
                is_neutral = False
            else:
                is_neutral = True

            game_meta = str(game_meta)

            is_postseason = (any(x in game_meta.lower() for x in TOURN_WORDS) or
                             any(x in game_meta.lower() for x in TOURN_SPEC))

            # AGGREGATE DATA INTO DATAFRAME AND RETURN
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
                is_conf,
                is_neutral,
                is_postseason,
                game_meta,
                game_day,
                game_time,
                game_loc,
                game_arena,
                game_cap,
                game_att,
                game_network,
                game_r1,
                game_r2,
                game_r3,
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

        except Exception as ex:
            if i+1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                _log.error(
                    f'"{time.ctime()}" attempt {i+1}: {game_id} - {ex}\n{traceback.format_exc()}')
                return pd.DataFrame([])
            else:
                # try again
                time.sleep(1.5)
                continue
        else:
            # no exception thrown
            break

    return pd.DataFrame([game_info_list], columns=game_info_cols)


def get_games_season(season: int) -> tuple:
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
    season_start_date = datetime(season - 1, 11, 1)
    season_end_date = datetime(season, 5, 1)
    len_season = (season_end_date - season_start_date).days
    date = season_start_date
    all_data = []

    with trange(len_season) as t:
        for i in t:
            game_ids = get_game_ids(date)

            if len(game_ids) > 0:
                games_info_day = []
                for j, gid in enumerate(game_ids):
                    t.set_description(
                        f"Scraping {gid} ({j+1}/{len(game_ids)}) on {date.strftime('%D')}"
                    )
                    games_info_day.append(get_game(gid))
                all_data.append(games_info_day)

            else:
                t.set_description(f"No games on {date.strftime('%D')}")

            date += timedelta(days=1)

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


def get_game_ids(date: Union[str, datetime]) -> list:
    """A function that scrapes all game IDs on a date.

    Parameters:
        - date: a string/datetime object representing the date to be scraped

    Returns
        - a list of ESPN all game IDs for games played on the date given
    """
    if type(date) == str:
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
            raise CouldNotParseError('The given date could not be parsed. Try any of these formats:\n' +
                                     'Y-m-d\nY/m/d\nm-d-Y\nm/d/Y')

    for i in range(ATTEMPTS):
        try:
            header = {
                'User-Agent': np.random.choice(USER_AGENTS),
                'Referer': np.random.choice(REFERERS),
            }
            d = date.strftime("%Y%m%d")
            url = SCOREBOARD_URL.format(d)
            page = r.get(url, headers=header)
            soup = bs(page.content, "lxml")
            sec = soup.find("section", {"class": "Card gameModules"})
            games = sec.find_all(
                "section", {
                    "class": "Scoreboard bg-clr-white flex flex-auto justify-between"}
            )
            ids = [game["id"] for game in games]

        except Exception as ex:
            if i+1 == ATTEMPTS:
                # max number of attempts reached, so return blank df
                _log.error(
                    f'"{time.ctime()}" attempt {i+1}: {date.strftime("%D")} - {ex}\n{traceback.format_exc()}')
                return []
            else:
                # try again
                time.sleep(1.5)
                continue
        else:
            # no exception thrown
            break

    return ids


def _clean_boxscore_table(table, team, game_id):
    """A helper function to clean the DataFrame returned by get_game_boxscore"""

    if len(table.find_all("thead")) <= 1:
        return pd.DataFrame([])

    # GET RID OF UNWANTED ROWS
    all_rows = table.find_all("tr")
    bad_rows_a = table.find_all("thead")[1].find_all("tr")
    bad_rows_b = table.find_all("tr", {"class": "highlight"})
    bad_rows = bad_rows_a + bad_rows_b
    good_rows = [
        row
        for row in all_rows
        if row not in bad_rows and not "Did not play" in str(row)
    ]
    str_good_rows = [str(row) for row in good_rows]

    # CREATE DATAFRAME
    t = "<table>"
    t += "".join(str_good_rows)
    t += "</table>"
    df = pd.read_html(t)[0]
    df.columns = [x.lower() for x in df.columns]

    # type handling
    df.starters = df.starters.astype(str)
    df['min'] = pd.to_numeric(df['min'], errors='coerce')
    df.fg = df.fg.astype(str)
    df['3pt'] = df['3pt'].astype(str)
    df.ft = df.ft.astype(str)
    df.oreb = pd.to_numeric(df.oreb, errors='coerce')
    df.dreb = pd.to_numeric(df.dreb, errors='coerce')
    df.reb = pd.to_numeric(df.reb, errors='coerce')
    df.ast = pd.to_numeric(df.ast, errors='coerce')
    df.stl = pd.to_numeric(df.stl, errors='coerce')
    df.blk = pd.to_numeric(df.blk, errors='coerce')
    df.to = pd.to_numeric(df.to, errors='coerce')
    df.pf = pd.to_numeric(df.pf, errors='coerce')
    df.pts = pd.to_numeric(df.pts, errors='coerce')

    # TOTALS ROW
    tot_row = [row for row in all_rows if 'TEAM' in row.get_text()]
    tot_t = "<table>"
    tot_t += str(tot_row[0])
    tot_t += "</table>"

    df_tot = pd.read_html(tot_t)[0]

    df_tot.columns = df.columns
    # type handling
    df_tot.starters = df_tot.starters.astype(str)
    df_tot['min'] = pd.to_numeric(df_tot['min'], errors='coerce')
    df_tot.fg = df_tot.fg.astype(str)
    df_tot['3pt'] = df_tot['3pt'].astype(str)
    df_tot.ft = df_tot.ft.astype(str)
    df_tot.oreb = pd.to_numeric(df_tot.oreb, errors='coerce')
    df_tot.dreb = pd.to_numeric(df_tot.dreb, errors='coerce')
    df_tot.reb = pd.to_numeric(df_tot.reb, errors='coerce')
    df_tot.ast = pd.to_numeric(df_tot.ast, errors='coerce')
    df_tot.stl = pd.to_numeric(df_tot.stl, errors='coerce')
    df_tot.blk = pd.to_numeric(df_tot.blk, errors='coerce')
    df_tot.to = pd.to_numeric(df_tot.to, errors='coerce')
    df_tot.pf = pd.to_numeric(df_tot.pf, errors='coerce')
    df_tot.pts = pd.to_numeric(df_tot.pts, errors='coerce')

    # START BY CLEANING PLAYER BOXSCORES
    # GET PLAYER IDS
    ids = [x.find("a")["href"].split("/")[-2]
           for x in good_rows if x.find("a")]
    # GET POSITION OF PLAYERS
    pos = [x[-1] for x in df["starters"]]
    # GET INFO ABOUT STARTERS
    start = [True if i < 5 else False for i in range(len(df))]

    # CLEAN PLAYER COLUMN
    df["starters"] = [x[: len(x) // 2] for x in df["starters"]]
    df = df.rename(columns={"starters": "player"})

    # SPLIT UP THE FG FIELDS
    fgm = [x.split("-")[0] for x in df["fg"]]
    fga = [x.split("-")[1] for x in df["fg"]]
    thpm = [x.split("-")[0] for x in df["3pt"]]
    thpa = [x.split("-")[1] for x in df["3pt"]]
    ftm = [x.split("-")[0] for x in df["ft"]]
    fta = [x.split("-")[1] for x in df["ft"]]

    # GET RID OF UNWANTED COLUMNS
    df = df.drop(columns=["fg", "3pt", "ft"])

    # INSERT COLUMNS WHERE NECESSARY
    df.insert(0, "game_id", game_id)
    df.game_id = df.game_id.astype(str)
    df.insert(1, "team", team)
    df.team = df.team.astype(str)
    df.insert(3, "player_id", ids)
    df.player_id = df.player_id.astype(str)
    df.insert(4, "position", pos)
    df.position = df.position.astype(str)
    df.insert(5, "starter", start)
    df.starter = df.starter.astype(bool)
    df.insert(7, "fgm", fgm)
    df.fgm = pd.to_numeric(df.fgm, errors='coerce')
    df.insert(8, "fga", fga)
    df.fga = pd.to_numeric(df.fga, errors='coerce')
    df.insert(9, "2pm", [int(x) - int(y) for x, y in zip(fgm, thpm)])
    df['2pm'] = pd.to_numeric(df['2pm'], errors='coerce')
    df.insert(10, "2pa", [int(x) - int(y) for x, y in zip(fga, thpa)])
    df['2pa'] = pd.to_numeric(df['2pa'], errors='coerce')
    df.insert(11, "3pm", thpm)
    df['3pm'] = pd.to_numeric(df['3pm'], errors='coerce')
    df.insert(12, "3pa", thpa)
    df['3pa'] = pd.to_numeric(df['3pa'], errors='coerce')
    df.insert(13, "ftm", ftm)
    df['ftm'] = pd.to_numeric(df['ftm'], errors='coerce')
    df.insert(14, "fta", fta)
    df['fta'] = pd.to_numeric(df['fta'], errors='coerce')

    # THEN CLEAN TOTAL ROW
    # SPLIT UP THE FG FIELDS
    fgm = [x.split('-')[0]
           if x.split('-')[0] != ''
           else np.nan
           for x in df_tot['fg']]
    fga = [x.split('-')[1]
           if x.split('-')[1] != ''
           else np.nan
           for x in df_tot['fg']]
    thpm = [x.split('-')[0]
            if x.split('-')[0] != ''
            else np.nan
            for x in df_tot['3pt']]
    thpa = [x.split('-')[1]
            if x.split('-')[1] != ''
            else np.nan
            for x in df_tot['3pt']]
    ftm = [x.split('-')[0]
           if x.split('-')[0] != ''
           else np.nan
           for x in df_tot['ft']]
    fta = [x.split('-')[1]
           if x.split('-')[1] != ''
           else np.nan
           for x in df_tot['ft']]

    # GET RID OF UNWANTED COLUMNS
    df_tot = df_tot.drop(columns=["fg", "3pt", "ft"])

    df_tot = df_tot.rename(columns={"starters": "player"})
    df_tot['player'] = 'TEAM'

    # INSERT COLUMNS WHERE NECESSARY
    df_tot.insert(0, "game_id", game_id)
    df_tot.game_id = df_tot.game_id.astype(str)
    df_tot.insert(1, "team", team)
    df_tot.team = df_tot.team.astype(str)
    df_tot.insert(3, "player_id", 'TOTAL')
    df_tot.player_id = df_tot.player_id.astype(str)
    df_tot.insert(4, "position", 'TOTAL')
    df_tot.position = df_tot.position.astype(str)
    df_tot.insert(5, "starter", 0)
    df_tot.starter = df_tot.starter.astype(bool)
    df_tot.insert(7, "fgm", fgm)
    df_tot.fgm = pd.to_numeric(df_tot.fgm, errors='coerce')
    df_tot.insert(8, "fga", fga)
    df_tot.fga = pd.to_numeric(df_tot.fga, errors='coerce')
    df_tot.insert(9, "2pm", [float(x) - float(y) for x, y in zip(fgm, thpm)])
    df_tot['2pm'] = pd.to_numeric(df_tot['2pm'], errors='coerce')
    df_tot.insert(10, "2pa", [float(x) - float(y) for x, y in zip(fga, thpa)])
    df_tot['2pa'] = pd.to_numeric(df_tot['2pa'], errors='coerce')
    df_tot.insert(11, "3pm", thpm)
    df_tot['3pm'] = pd.to_numeric(df_tot['3pm'], errors='coerce')
    df_tot.insert(12, "3pa", thpa)
    df_tot['3pa'] = pd.to_numeric(df_tot['3pa'], errors='coerce')
    df_tot.insert(13, "ftm", ftm)
    df_tot['ftm'] = pd.to_numeric(df_tot['ftm'], errors='coerce')
    df_tot.insert(14, "fta", fta)
    df_tot['fta'] = pd.to_numeric(df_tot['fta'], errors='coerce')

    return pd.concat([df, df_tot])


def _get_pbp_map(soup):
    """A helper function to map plays in the play-by-play 
    to the teams who carried out the play"""
    # GET DIVS
    away_div = soup.find("div", {"class": "team away"})
    home_div = soup.find("div", {"class": "team home"})

    # FORMAT AWAY INFO
    away_name = (
        away_div.find("span", {"class": "long-name"}).get_text()
        + " "
        + away_div.find("span", {"class": "short-name"}).get_text()
    )
    try:
        away_logo = (
            away_div.find("div", {"class": "logo"})
            .find("img")["src"]
            .split(html.unescape("&amp;"))[0]
        )
    except:
        # in this case, the team is a non-d1 team
        away_logo = None

    # FORMAT HOME INFO
    home_name = (
        home_div.find("span", {"class": "long-name"}).get_text()
        + " "
        + home_div.find("span", {"class": "short-name"}).get_text()
    )
    try:
        home_logo = (
            home_div.find("div", {"class": "logo"})
            .find("img")["src"]
            .split(html.unescape("&amp;"))[0]
        )
    except:
        # in this case, the team is a non-d1 team
        home_logo = None

    return {home_logo: home_name, away_logo: away_name}


def _clean_pbp_table(table, info):
    """A helper function to clean the DataFrame returned by get_game_pbp"""
    team_map = info[0]
    num_halves = info[1]
    cur_half = info[2]
    game_id = info[3]
    home_name = list(team_map.values())[0]
    away_name = list(team_map.values())[1]
    body_rows = [x for x in table.find_all("tr") if not x.find("th")]

    # MAP THE LOGOS IN THE PBP TO THE TEAMS
    logos = [row.find("td", {'class': 'logo'}) for row in body_rows]
    links = [logo.find('img')['src'] if logo.find('img')
             else None for logo in logos]
    links = [x.split(html.unescape("&amp;"))[0] if x else x for x in links]
    pbp_teams = [team_map[x] for x in links]
    if num_halves == 2:
        tot_seconds_in_game = num_halves * 20 * 60
    else:
        tot_seconds_in_game = (2 * 20 * 60) + ((num_halves - 2) * 5 * 60)

    df = pd.read_html(str(table))[0]
    df = df.dropna(axis=1, how="all")
    df.columns = [x.lower() for x in df.columns]
    df = df.loc[:, ~df.columns.str.contains('unnamed')]
    df = df.rename(columns={'time_of_day': 'time'})

    # type handling
    df.time = df.time.astype(str)
    df.play = df.play.astype(str)
    df.score = df.score.astype(str)

    df["team"] = pbp_teams
    df.team = df.team.astype(str)
    df.insert(1, "game_id", game_id)
    df.game_id = df.game_id.astype(str)

    # SCORE FORMATTING
    score_splits = [x.split(" - ") for x in df.score]
    away_scores = [int(x[0]) for x in score_splits]
    home_scores = [int(x[1]) for x in score_splits]
    df.insert(3, "home_score", home_scores)
    df.home_score = pd.to_numeric(df.home_score, errors='coerce')
    df.insert(4, "away_score", away_scores)
    df.away_score = pd.to_numeric(df.away_score, errors='coerce')
    df = df.drop(columns=["score"])

    # HALF NUMBER
    df.insert(5, "half", cur_half)
    df.half = pd.to_numeric(df.half, errors='coerce')

    # TIME FORMATTING
    time_splits = [x.split(":") for x in df.time]
    minutes = [int(x[0]) for x in time_splits]
    seconds = [int(x[1]) for x in time_splits]
    min_to_sec = [x * 60 for x in minutes]
    tot_secs_left = [x + y for x, y in zip(min_to_sec, seconds)]
    reg_secs_left = [1200 + x if cur_half == 1 else x for x in tot_secs_left]

    df.insert(6, "secs_left_half", tot_secs_left)
    df.secs_left_half = pd.to_numeric(df.secs_left_half, errors='coerce')
    df.insert(7, "secs_left_reg", reg_secs_left)
    df.secs_left_reg = pd.to_numeric(df.secs_left_reg, errors='coerce')
    df = df.drop(columns=["time"])

    # ASSIGN PLAY TYPES
    p_types = []

    for play in df.play:
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
            p_types.append('')

    df["play_type"] = p_types
    df.play_type = df.play_type.astype(str)

    scoring_play = [
        True if not row.find("th") and row.has_attr("class") else False
        for row in body_rows
    ]
    df["scoring_play"] = scoring_play
    df.scoring_play = df.scoring_play.astype(bool)

    # FIND SHOOTERS
    scorers = [
        x[0].split(" made ")[0] if x[1] else "" for x in zip(df.play, df.scoring_play)
    ]

    non_scorers = [
        x[0].split(" missed ")[0] if x[1] == "shot" and not x[2] else ""
        for x in zip(df.play, df.play_type, df.scoring_play)
    ]

    shooters = [x[0] if not x[0] == "" else x[1]
                for x in zip(scorers, non_scorers)]

    df["shooter"] = shooters
    df.shooter = df.shooter.astype(str)

    # INSERT TEAMS
    df = df.rename(columns={"team": "play_team"})
    df = df.rename(columns={"play": "play_desc"})
    df.insert(1, "home_team", home_name)
    df.home_team = df.home_team.astype(str)
    df.insert(2, "away_team", away_name)
    df.away_team = df.away_team.astype(str)

    # GET ASSIST INFO
    is_assisted = [True if "Assisted" in x else False for x in df.play_desc]
    df["is_assisted"] = is_assisted
    df.is_assisted = df.is_assisted.astype(bool)
    assisted_pls = [
        x[0].split("Assisted by ")[-1].replace(".", "") if x[1] else ""
        for x in zip(df.play_desc, df.is_assisted)
    ]
    df["assist_player"] = assisted_pls
    df.assist_player = df.assist_player.astype(str)

    return df
