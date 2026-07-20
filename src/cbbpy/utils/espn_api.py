"""ESPN JSON API transport for game data (the default `source="api"`).

A single summary request per game
(`site.api.espn.com/.../summary?event={id}`) carries the header, boxscore, and
plays, so one fetch serves game info + boxscore + play-by-play. The parsed JSON
is reshaped into the exact structures the HTML helpers in ``cbbpy_utils`` already
consume (``_get_game_boxscore_helper`` / ``_get_game_pbp_helper``) so column
layout, dtypes, and text parsing stay identical across the two sources.

Retry/backoff/logging/impersonation and the ``pnf_`` dead-game cache are reused
from ``cbbpy_utils`` rather than duplicated.
"""

import re
import time
import traceback

import numpy as np
import pandas as pd
from datetime import timezone
from dateutil import parser
from pytz import timezone as tz

from cbbpy.utils import cbbpy_utils as cu


MENS_API_SUMMARY_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/"
    "mens-college-basketball/summary?event={}"
)
WOMENS_API_SUMMARY_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/"
    "womens-college-basketball/summary?event={}"
)
MENS_API_SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/"
    "mens-college-basketball/scoreboard?dates={}&groups=50&limit=500"
)
WOMENS_API_SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/"
    "womens-college-basketball/scoreboard?dates={}&groups=50&limit=500"
)


def _fetch_summary(game_id, game_type):
    """GET + parse the summary endpoint for a game, with retries.

    Returns the parsed JSON dict, or None on persistent failure. A 404 marks
    the game in ``cu.pnf_`` so the other scrapers skip it, mirroring the HTML path.
    """
    game_id = str(game_id)
    pre_url = MENS_API_SUMMARY_URL if game_type == "mens" else WOMENS_API_SUMMARY_URL
    js = None

    for i in range(cu.ATTEMPTS):
        try:
            header = {"Referer": str(np.random.choice(cu.REFERERS))}
            url = pre_url.format(game_id)
            page = cu.r.get(url, headers=header, impersonate=cu.IMPERSONATE, timeout=cu.REQUEST_TIMEOUT)
            js = page.json()

            # a missing game deterministically returns {"code": 404, ...} (HTTP
            # 404); bail immediately rather than retrying a game that won't exist
            if js.get("code") == 404 or getattr(page, "status_code", None) == 404:
                cu._log.error(f"{game_id} - API: Page not found error")
                cu.pnf_.append(game_id)
                return None

            # a valid summary always carries a header
            if "header" not in js:
                raise cu.CouldNotParseError(js.get("message", "no header in response"))

        except Exception as ex:
            if i + 1 == cu.ATTEMPTS:
                cu._log.error(f"{game_id} - API: {ex}\n{traceback.format_exc()}")
                return None
            else:
                time.sleep(np.random.uniform(low=1, high=3))
                continue
        else:
            break

    return js


def _game_status(summary):
    return summary["header"]["competitions"][0]["status"]["type"]["description"]


def _get_game_info_api(game_id, game_type, summary=None):
    game_id = str(game_id)
    if summary is None:
        summary = _fetch_summary(game_id, game_type)
    if summary is None:
        return pd.DataFrame([])

    try:
        gm_status = _game_status(summary)
        if gm_status not in cu.GOOD_GAME_STATUSES:
            cu._log.warning(f"{game_id} - {gm_status}")
        return _parse_game_info(summary, game_id, game_type)
    except Exception as ex:
        cu._log.error(f"{game_id} - Game Info (API): {ex}\n{traceback.format_exc()}")
        return pd.DataFrame([])


def _get_game_boxscore_api(game_id, game_type, summary=None):
    game_id = str(game_id)
    if summary is None:
        summary = _fetch_summary(game_id, game_type)
    if summary is None:
        return pd.DataFrame([])

    try:
        gm_status = _game_status(summary)
        if gm_status not in cu.GOOD_GAME_STATUSES:
            cu._log.warning(f"{game_id} - {gm_status}")
            return pd.DataFrame([])

        players = (summary.get("boxscore") or {}).get("players") or []
        if len(players) < 2:
            cu._log.warning(f"{game_id} - No boxscore available")
            return pd.DataFrame([])

        boxscore = _boxscore_adapter(players)
        return cu._get_game_boxscore_helper(boxscore, game_id).reset_index(drop=True)
    except Exception as ex:
        cu._log.error(f"{game_id} - Boxscore (API): {ex}\n{traceback.format_exc()}")
        return pd.DataFrame([])


def _get_game_pbp_api(game_id, game_type, summary=None):
    game_id = str(game_id)
    if summary is None:
        summary = _fetch_summary(game_id, game_type)
    if summary is None:
        return pd.DataFrame([])

    try:
        gm_status = _game_status(summary)
        if gm_status not in cu.GOOD_GAME_STATUSES:
            cu._log.warning(f"{game_id} - {gm_status}")
            return pd.DataFrame([])

        gamepackage = _pbp_adapter(summary)
        return cu._get_game_pbp_helper(gamepackage, game_id, game_type).reset_index(
            drop=True
        )
    except Exception as ex:
        cu._log.error(f"{game_id} - PBP (API): {ex}\n{traceback.format_exc()}")
        return pd.DataFrame([])


def _get_game_ids_api(date, game_type):
    if isinstance(date, str):
        date = cu._parse_date(date)

    pre_url = (
        MENS_API_SCOREBOARD_URL if game_type == "mens" else WOMENS_API_SCOREBOARD_URL
    )
    js = None

    for i in range(cu.ATTEMPTS):
        try:
            header = {"Referer": str(np.random.choice(cu.REFERERS))}
            d = date.strftime("%Y%m%d")
            url = pre_url.format(d)
            page = cu.r.get(url, headers=header, impersonate=cu.IMPERSONATE, timeout=cu.REQUEST_TIMEOUT)
            js = page.json()
            ids = [str(x["id"]) for x in js.get("events", [])]
        except Exception as ex:
            if i + 1 == cu.ATTEMPTS:
                cu._log.error(
                    f'{date.strftime("%D")} - IDs (API): {ex}\n{traceback.format_exc()}'
                )
                return []
            else:
                time.sleep(np.random.uniform(low=1, high=3))
                continue
        else:
            break

    return ids


# --- parsing / adapters -----------------------------------------------------


def _competitor_record(competitor, record_type):
    for rec in competitor.get("record") or []:
        if rec.get("type") == record_type:
            return rec.get("displayValue", "")
    return ""


def _nd_id(display_name):
    """Slug id ESPN gives non-D1 teams (mirrors the HTML path's fallback)."""
    slug = display_name.lower().replace(" ", "-")
    return "nd-" + re.sub(r"[^0-9a-zA-Z-]", "", slug)


def _parse_game_info(summary, game_id, game_type):
    header = summary["header"]
    comp = header["competitions"][0]

    ht = next(c for c in comp["competitors"] if c["homeAway"] == "home")
    at = next(c for c in comp["competitors"] if c["homeAway"] == "away")

    gm_status = comp["status"]["type"]["description"]
    home_team = ht["team"]["displayName"]
    away_team = at["team"]["displayName"]

    # non-D1 teams carry no records; ESPN renders them with a slug id
    home_id = ht["id"] if (ht.get("record") or []) else _nd_id(home_team)
    away_id = at["id"] if (at.get("record") or []) else _nd_id(away_team)

    home_rank = ht.get("rank")
    away_rank = at.get("rank")
    home_rank = np.nan if home_rank is None else float(home_rank)
    away_rank = np.nan if away_rank is None else float(away_rank)

    home_record = _competitor_record(ht, "total")
    away_record = _competitor_record(at, "total")

    home_score = int(ht.get("score", 0))
    away_score = int(at.get("score", 0))
    home_win = home_score > away_score and gm_status == "Final"

    is_postseason = header["season"].get("type") == 3
    is_conference = bool(comp.get("conferenceCompetition"))
    is_neutral = bool(comp.get("neutralSite"))
    tournament = header.get("gameNote") or ""

    gm_date = parser.parse(comp["date"])
    game_datetime = gm_date.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    game_date = gm_date.replace(tzinfo=timezone.utc).astimezone(tz=tz("US/Pacific"))
    # game_day/game_time (Pacific) are deprecated in favor of game_datetime; removal in 3.0
    game_day = game_date.strftime("%B %d, %Y")
    game_time = game_date.strftime("%I:%M %p %Z")

    home_ls = ht.get("linescores")
    away_ls = at.get("linescores")
    if home_ls and away_ls:
        # men, and women before the 15-16 season, use halves
        if (
            game_type == "mens"
            or game_date.replace(tzinfo=None) < cu.WOMEN_HALF_RULE_CHANGE_DATE
        ):
            h_ot, a_ot = len(home_ls) - 2, len(away_ls) - 2
        else:
            h_ot, a_ot = len(home_ls) - 4, len(away_ls) - 4
        assert h_ot == a_ot
        num_ots = h_ot
    else:
        cu._log.warning(f"{game_id} - No score info available")
        num_ots = -1

    game_info = summary.get("gameInfo") or {}
    venue = game_info.get("venue") or {}
    arena = venue.get("fullName") or ""
    address = venue.get("address") or {}
    loc = (
        address.get("city", "") + ", " + address.get("state", "")
        if address.get("city")
        else ""
    )
    capacity = float(venue["capacity"]) if venue.get("capacity") else np.nan
    attendance = (
        float(game_info["attendance"])
        if game_info.get("attendance") is not None
        else np.nan
    )

    officials = game_info.get("officials") or []
    ref_1 = officials[0]["displayName"] if len(officials) > 0 else ""
    ref_2 = officials[1]["displayName"] if len(officials) > 1 else ""
    ref_3 = officials[2]["displayName"] if len(officials) > 2 else ""

    broadcasts = comp.get("broadcasts") or []
    network = broadcasts[0]["media"]["shortName"] if broadcasts else ""

    # odds come from a single pickcenter entry (consensus provider preferred);
    # older archived games carry no pickcenter, leaving these empty/NaN
    pc = _select_pickcenter(summary)
    home_spread = _home_spread(pc) if pc is not None else ""
    over_under = _pc_float(pc, "overUnder") if pc is not None else np.nan
    home_ml = _pc_moneyline(pc, "homeTeamOdds") if pc is not None else np.nan
    away_ml = _pc_moneyline(pc, "awayTeamOdds") if pc is not None else np.nan

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


def _select_pickcenter(summary):
    """Return one pickcenter entry: the 'consensus' provider if present, else the first."""
    pc = summary.get("pickcenter") or []
    if not pc:
        return None
    for entry in pc:
        if (entry.get("provider") or {}).get("name") == "consensus":
            return entry
    return pc[0]


def _home_spread(pc):
    """Home-relative point spread as a signed string (favorite negative), e.g. "-5.5".

    ESPN's pickcenter ``spread`` is favorite-relative (always negative); flip it
    when the home team is the underdog. Matches the HTML path's string column.
    """
    spread = pc.get("spread")
    if spread is None:
        return ""
    home_fav = ((pc.get("homeTeamOdds") or {}).get("favorite")) is True
    val = spread if home_fav else -spread
    return f"{val:+g}"


def _pc_float(pc, key):
    val = pc.get(key)
    return float(val) if val is not None else np.nan


def _pc_moneyline(pc, side):
    val = (pc.get(side) or {}).get("moneyLine")
    return float(val) if val is not None else np.nan


def _boxscore_adapter(players):
    """Reshape ``boxscore.players`` into the structure _get_game_boxscore_helper wants.

    The API lists every athlete (starters + bench + DNPs) in one block with a
    ``starter`` flag; the HTML helper expects starters, bench, and team totals in
    three separate blocks. DNPs are dropped to match the HTML output.
    """
    teams = []
    for team in players:
        stat = team["statistics"][0]
        labels = stat["names"]

        starters, bench = [], []
        for a in stat["athletes"]:
            if a.get("didNotPlay"):
                continue
            ath = a["athlete"]
            entry = {
                "athlt": {
                    "shrtNm": ath.get("shortName", ""),
                    "uid": ath.get("id", ""),
                    "pos": (ath.get("position") or {}).get("abbreviation", ""),
                },
                "stats": a.get("stats", []),
            }
            (starters if a.get("starter") else bench).append(entry)

        teams.append(
            {
                "tm": {"dspNm": team["team"]["displayName"]},
                "stats": [
                    {"athlts": starters, "lbls": labels},
                    {"athlts": bench},
                    {"ttls": stat.get("totals", [])},
                ],
            }
        )
    return teams


def _pbp_adapter(summary):
    """Reshape API plays into the play dicts _get_game_pbp_helper consumes.

    Field renames (team.id -> homeAway, homeScore -> hmScr, type.text -> txt),
    and participant normalization: participants[0] becomes the primary athlete;
    participants[1] becomes an assist participant only when the text confirms it.
    No shot chart is attached, so shot coordinates come from the plays themselves.
    """
    comp = summary["header"]["competitions"][0]
    id_to_homeaway = {c["id"]: c["homeAway"] for c in comp["competitors"]}
    home = next(c for c in comp["competitors"] if c["homeAway"] == "home")
    away = next(c for c in comp["competitors"] if c["homeAway"] == "away")

    # the API's plays carry bare participant ids; the boxscore names them, so
    # build an id->full name map to inject play-level player/assist names
    id_to_name = {}
    for team in (summary.get("boxscore") or {}).get("players") or []:
        for a in (team.get("statistics") or [{}])[0].get("athletes") or []:
            ath = a.get("athlete") or {}
            if ath.get("id"):
                id_to_name[str(ath["id"])] = ath.get("displayName", "")

    plays = []
    for p in summary.get("plays") or []:
        adapted = {"id": p.get("id", "")}

        if "text" in p:
            adapted["text"] = p["text"]

        team_id = (p.get("team") or {}).get("id")
        if team_id in id_to_homeaway:
            adapted["homeAway"] = id_to_homeaway[team_id]

        if "homeScore" in p:
            adapted["hmScr"] = p["homeScore"]
        if "awayScore" in p:
            adapted["awScr"] = p["awayScore"]
        if "period" in p:
            adapted["period"] = p["period"]
        if "clock" in p:
            adapted["clock"] = p["clock"]
        if p.get("scoringPlay"):
            adapted["scoringPlay"] = True
        if p.get("shootingPlay"):
            adapted["shootingPlay"] = True
        if "type" in p:
            adapted["type"] = {
                "txt": p["type"].get("text", ""),
                "id": p["type"].get("id", ""),
            }

        participants = p.get("participants") or []
        if participants:
            pid = str((participants[0].get("athlete") or {}).get("id", ""))
            adapted["athlete"] = {"id": pid, "name": id_to_name.get(pid, "")}
        text = p.get("text", "") or ""
        if len(participants) > 1 and "assisted" in text.lower():
            aid = str((participants[1].get("athlete") or {}).get("id", ""))
            adapted["participants"] = [
                {
                    "id": aid,
                    "name": id_to_name.get(aid, ""),
                    "description": "AST",
                }
            ]

        if "coordinate" in p:
            adapted["coordinate"] = p["coordinate"]

        plays.append(adapted)

    # play_id -> home win probability in [0,1]; ESPN's summary already reports it
    # home-relative on a 0-1 scale
    win_prob = {
        str(e["playId"]): e["homeWinPercentage"]
        for e in (summary.get("winprobability") or [])
        if e.get("playId") is not None and e.get("homeWinPercentage") is not None
    }

    return {
        "pbp": {
            "tms": {
                "home": {"nm": home["team"]["displayName"]},
                "away": {"nm": away["team"]["displayName"]},
            },
            "plays": plays,
        },
        "gmInfo": {"dtTm": comp["date"]},
        "win_prob": win_prob,
    }
