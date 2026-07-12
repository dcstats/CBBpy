"""
Maintainer script: update the static team map CSVs with a new season's data
from ESPN's API.

Usage:
    python -m cbbpy.utils.update_team_map           # adds current season
    python -m cbbpy.utils.update_team_map 2027      # adds a specific season
    python -m cbbpy.utils.update_team_map --check   # show existing seasons, no writes

ESPN API endpoints used:
    /teams?limit=500&season=YYYY        — all D1 teams with IDs and names
    /groups?season=YYYY                 — conference assignments (partial)
    /scoreboard/conferences             — conference ID → name/abbreviation mapping
    /teams/{id}                         — individual team conference (fallback)
"""

import argparse
from pathlib import Path

import pandas as pd
from curl_cffi import requests as r

from cbbpy.utils.cbbpy_utils import IMPERSONATE, _get_current_season

SPORTS = {
    "mens": "mens-college-basketball",
    "womens": "womens-college-basketball",
}

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball"

CSV_DIR = Path(__file__).resolve().parent

REQUEST_TIMEOUT = 30


def _get_json(url):
    page = r.get(url, impersonate=IMPERSONATE, timeout=REQUEST_TIMEOUT)
    page.raise_for_status()
    return page.json()


def get_conference_lookup(sport: str) -> dict:
    """Map ESPN group IDs to (conference_name, conference_abb) using the scoreboard endpoint."""
    confs = _get_json(f"{ESPN_BASE}/{sport}/scoreboard/conferences")["conferences"]
    return {str(c["groupId"]): (c["name"], c["shortName"].lower()) for c in confs}


def get_teams(sport: str, season: int) -> dict:
    """Get all D1 teams for a season: {team_id: {team, location}}."""
    js = _get_json(f"{ESPN_BASE}/{sport}/teams?limit=500&season={season}")
    entries = js["sports"][0]["leagues"][0]["teams"]
    return {
        int(t["team"]["id"]): {
            "team": t["team"]["displayName"],
            "location": t["team"]["location"],
        }
        for t in entries
    }


def get_conference_assignments(sport: str, season: int) -> dict:
    """Get team → conference mapping from the groups endpoint (covers most teams)."""
    data = _get_json(f"{ESPN_BASE}/{sport}/groups?season={season}&limit=100")
    team_conf = {}
    for group in data.get("groups", []):
        for child in group.get("children", []):
            name = child["name"]
            abb = child["abbreviation"].lower()
            for t in child.get("teams", []):
                team_conf[int(t["id"])] = (name, abb)
    return team_conf


def fill_missing_conferences(sport: str, team_ids: set, conf_lookup: dict) -> dict:
    """For teams not in the groups endpoint, hit individual team pages for conference."""
    filled = {}
    for tid in sorted(team_ids):
        try:
            js = _get_json(f"{ESPN_BASE}/{sport}/teams/{tid}")
        except Exception:
            continue
        gid = str(js.get("team", {}).get("groups", {}).get("id", ""))
        if gid in conf_lookup:
            filled[tid] = conf_lookup[gid]
    return filled


def scrape_season(sport_key: str, sport_path: str, season: int) -> pd.DataFrame:
    """Scrape all teams + conferences for one sport/season combo."""
    print(f"  Fetching {sport_key} teams for {season}...")
    teams = get_teams(sport_path, season)
    print(f"    {len(teams)} teams found")

    conf_lookup = get_conference_lookup(sport_path)
    team_conf = get_conference_assignments(sport_path, season)
    matched = len(team_conf)

    missing_ids = set(teams.keys()) - set(team_conf.keys())
    if missing_ids:
        print(f"    {len(missing_ids)} teams missing conference, fetching individually...")
        filled = fill_missing_conferences(sport_path, missing_ids, conf_lookup)
        team_conf.update(filled)

    still_missing = set(teams.keys()) - set(team_conf.keys())
    if still_missing:
        print(f"    WARNING: {len(still_missing)} teams still have no conference data")

    print(f"    {len(team_conf)} teams with conferences ({matched} from groups, {len(team_conf) - matched} from fallback)")

    rows = []
    for tid, info in teams.items():
        conf_name, conf_abb = team_conf.get(tid, ("Unknown", "unk"))
        rows.append({
            "season": season,
            "id": tid,
            "team": info["team"],
            "location": info["location"],
            "conference": conf_name,
            "conference_abb": conf_abb,
        })

    return pd.DataFrame(rows).sort_values(["conference", "location"]).reset_index(drop=True)


def main():
    parser = argparse.ArgumentParser(description="Update cbbpy team map CSVs with new season data")
    parser.add_argument("season", nargs="?", type=int, default=None, help="Season year to add (default: current)")
    parser.add_argument("--check", action="store_true", help="Show existing seasons without writing")
    args = parser.parse_args()

    for sport_key, sport_path in SPORTS.items():
        csv_path = CSV_DIR / f"{sport_key}_team_map.csv"
        existing = pd.read_csv(csv_path)
        seasons = sorted(existing.season.unique())

        if args.check:
            print(f"{sport_key}: {csv_path.name} has seasons {seasons[0]}-{seasons[-1]} ({len(existing)} rows)")
            continue

        season = args.season or _get_current_season()

        if season in seasons:
            print(f"{sport_key}: season {season} already exists in {csv_path.name} — skipping")
            continue

        df = scrape_season(sport_key, sport_path, season)

        # Normalize abbreviations to match existing CSV format: ESPN's group
        # abbreviations drift (and sometimes carry region names like "South"),
        # so a conference already in the CSV keeps its abb. Map from the latest
        # season only — older seasons carry per-division abbs (belte/beltw,
        # Patriot League North/South, ...) that would poison a last-wins dict.
        latest = existing[existing.season == existing.season.max()]
        name_to_abb = (
            latest[["conference", "conference_abb"]]
            .drop_duplicates()
            .set_index("conference")["conference_abb"]
            .to_dict()
        )
        df["conference_abb"] = df.apply(
            lambda r: name_to_abb.get(r["conference"], r["conference_abb"]), axis=1
        )

        # Flag any new conferences that didn't have a mapping
        new_confs = df[~df.conference.isin(name_to_abb)][["conference", "conference_abb"]].drop_duplicates()
        if not new_confs.empty:
            print("    New conferences (using ESPN abbreviation):")
            for _, row in new_confs.iterrows():
                print(f"      {row.conference_abb}: {row.conference}")

        combined = pd.concat([existing, df], ignore_index=True)
        combined.to_csv(csv_path, index=False)
        print(f"  Wrote {len(df)} new rows → {csv_path.name} ({len(combined)} total)")


if __name__ == "__main__":
    main()
