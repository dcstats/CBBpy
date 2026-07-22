"""Command-line interface for CBBpy: pull ESPN NCAA basketball data to files."""

import argparse
import re
import subprocess
import sys
from pathlib import Path

from cbbpy.utils.scraper import GameScraper
from cbbpy.utils.cbbpy_utils import _get_current_season


def _slugify(text):
    """Lowercase, spaces to dashes, drop anything not filesystem-safe."""
    text = str(text).lower().replace(" ", "-")
    return re.sub(r"[^a-z0-9_-]", "", text)


def _resolve_season(season):
    return season if season is not None else _get_current_season()


def _write(df, path, fmt):
    if fmt == "parquet":
        df.to_parquet(path)
    else:
        df.to_csv(path, index=False)


def _page(df):
    """Wide-frame stdout: on a TTY scroll it through `less -S`, else emit CSV."""
    if not sys.stdout.isatty():
        df.to_csv(sys.stdout, index=False)
        return
    try:
        pager = subprocess.Popen(["less", "-SFX"], stdin=subprocess.PIPE, text=True)
    except FileNotFoundError:
        df.to_csv(sys.stdout, index=False)
        return
    try:
        pager.communicate(df.to_string(index=False))
    except BrokenPipeError:
        pass


def _emit(command, slug, kind, df, args):
    """Write one frame to disk (or stdout), skipping empties. Returns True if emitted."""
    if df.empty:
        label = f"{command} {kind}" if kind else command
        print(f"[cbbpy] skipping empty {label} frame", file=sys.stderr)
        return False
    if getattr(args, "stdout", False):
        if command in ("info", "player"):
            print("\n\n".join(row.to_frame().to_string(header=False) for _, row in df.iterrows()))
        else:
            _page(df)
        return True
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ext = "parquet" if args.format == "parquet" else "csv"
    parts = [args.gender, command, _slugify(slug)] + ([kind] if kind else [])
    path = out_dir / ("_".join(parts) + "." + ext)
    _write(df, path, fmt=args.format)
    print(path)
    return True


def _emit_frames(command, slug, frames, args):
    """Write the info/boxscore/pbp tuple, honoring the --no-* flags."""
    info, box, pbp = frames
    for kind, df, wanted in [
        ("info", info, args.info),
        ("boxscore", box, args.box),
        ("pbp", pbp, args.pbp),
    ]:
        if wanted:
            _emit(command, slug, kind, df, args)


def _cmd_game(scraper, args):
    import pandas as pd

    results = [
        scraper.get_game(gid, info=args.info, box=args.box, pbp=args.pbp, source=args.source)
        for gid in args.game_id
    ]
    frames = tuple(pd.concat(dfs, ignore_index=True) for dfs in zip(*results))
    _emit_frames("game", "-".join(args.game_id), frames, args)


def _cmd_frame(scraper, args):
    import pandas as pd

    method = {
        "info": scraper.get_game_info,
        "box": scraper.get_game_boxscore,
        "pbp": scraper.get_game_pbp,
    }[args.command]
    df = pd.concat(
        [method(gid, source=args.source) for gid in args.game_id], ignore_index=True
    )
    _emit(args.command, "-".join(args.game_id), None, df, args)


def _cmd_range(scraper, args):
    frames = scraper.get_games_range(
        args.start_date, args.end_date,
        info=args.info, box=args.box, pbp=args.pbp,
        source=args.source, throttle=args.throttle, n_jobs=args.n_jobs,
    )
    _emit_frames("range", f"{args.start_date}_{args.end_date}", frames, args)


def _cmd_season(scraper, args):
    season = _resolve_season(args.season)
    frames = scraper.get_games_season(
        season, info=args.info, box=args.box, pbp=args.pbp,
        source=args.source, throttle=args.throttle, n_jobs=args.n_jobs,
    )
    _emit_frames("season", str(season), frames, args)


def _cmd_team(scraper, args):
    season = _resolve_season(args.season)
    frames = scraper.get_games_team(
        args.team, season, info=args.info, box=args.box, pbp=args.pbp,
        source=args.source, throttle=args.throttle, n_jobs=args.n_jobs,
    )
    _emit_frames("team", f"{args.team}_{season}", frames, args)


def _cmd_conference(scraper, args):
    season = _resolve_season(args.season)
    frames = scraper.get_games_conference(
        args.conference, season, info=args.info, box=args.box, pbp=args.pbp,
        source=args.source, throttle=args.throttle, n_jobs=args.n_jobs,
    )
    _emit_frames("conference", f"{args.conference}_{season}", frames, args)


def _cmd_ids(scraper, args):
    for gid in scraper.get_game_ids(args.date, source=args.source):
        print(gid)


def _cmd_player(scraper, args):
    df = scraper.get_player_info(args.player_id)
    _emit("player", args.player_id, None, df, args)


def _cmd_schedule(scraper, args):
    season = _resolve_season(args.season)
    if args.team:
        df = scraper.get_team_schedule(args.team, season)
        slug = f"{args.team}_{season}"
    else:
        df = scraper.get_conference_schedule(args.conference, season)
        slug = f"{args.conference}_{season}"
    _emit("schedule", slug, None, df, args)


def _build_parser():
    parser = argparse.ArgumentParser(prog="cbbpy", description="Scrape NCAA basketball data from ESPN.")
    sub = parser.add_subparsers(dest="command", required=True)

    gender = argparse.ArgumentParser(add_help=False)
    gender.add_argument("-g", "--gender", choices=["mens", "womens"], default="mens",
                        help="which scraper to use (default: mens)")

    io = argparse.ArgumentParser(add_help=False)
    io.add_argument("-o", "--output-dir", default=".", help="directory for output files (default: .)")
    io.add_argument("--format", choices=["csv", "parquet"], default="csv",
                    help="output file format (default: csv)")

    source = argparse.ArgumentParser(add_help=False)
    source.add_argument("--source", choices=["api", "html"], default="api",
                        help="data source (default: api)")

    frames = argparse.ArgumentParser(add_help=False)
    frames.add_argument("--no-info", dest="info", action="store_false", help="skip game metadata")
    frames.add_argument("--no-box", dest="box", action="store_false", help="skip boxscore")
    frames.add_argument("--no-pbp", dest="pbp", action="store_false", help="skip play-by-play")

    single = argparse.ArgumentParser(add_help=False)
    single.add_argument("--stdout", action="store_true",
                        help="print the table to stdout instead of writing a file")

    bulk = argparse.ArgumentParser(add_help=False)
    bulk.add_argument("--throttle", type=float, default=0.5,
                      help="mean per-worker delay in seconds (default: 0.5)")
    bulk.add_argument("--n-jobs", type=int, default=None,
                      help="parallel workers (default: CPU count minus 1)")

    game_data = [gender, io, source, frames]

    p = sub.add_parser("game", parents=game_data, help="scrape one or more games by ID")
    p.add_argument("game_id", nargs="+")
    p.set_defaults(func=_cmd_game)

    frame_data = [gender, io, source, single]
    for name, helptext in [
        ("info", "scrape game metadata for one or more games by ID"),
        ("box", "scrape the boxscore for one or more games by ID"),
        ("pbp", "scrape play-by-play for one or more games by ID"),
    ]:
        p = sub.add_parser(name, parents=frame_data, help=helptext)
        p.add_argument("game_id", nargs="+")
        p.set_defaults(func=_cmd_frame)

    p = sub.add_parser("range", parents=game_data + [bulk], help="scrape games in a date range")
    p.add_argument("start_date")
    p.add_argument("end_date")
    p.set_defaults(func=_cmd_range)

    p = sub.add_parser("season", parents=game_data + [bulk], help="scrape a whole season")
    p.add_argument("season", nargs="?", default=None, help="season (default: current)")
    p.set_defaults(func=_cmd_season)

    p = sub.add_parser("team", parents=game_data + [bulk], help="scrape a team's season")
    p.add_argument("team")
    p.add_argument("-s", "--season", default=None, help="season (default: current)")
    p.set_defaults(func=_cmd_team)

    p = sub.add_parser("conference", parents=game_data + [bulk], help="scrape a conference's season")
    p.add_argument("conference")
    p.add_argument("-s", "--season", default=None, help="season (default: current)")
    p.set_defaults(func=_cmd_conference)

    p = sub.add_parser("ids", parents=[gender, source], help="print game IDs for a date")
    p.add_argument("date")
    p.set_defaults(func=_cmd_ids)

    p = sub.add_parser("player", parents=[gender, io, single], help="scrape a player's bio")
    p.add_argument("player_id")
    p.set_defaults(func=_cmd_player)

    p = sub.add_parser("schedule", parents=[gender, io, single], help="scrape a team or conference schedule")
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument("--team")
    grp.add_argument("--conference")
    p.add_argument("-s", "--season", default=None, help="season (default: current)")
    p.set_defaults(func=_cmd_schedule)

    return parser


def main(argv=None):
    args = _build_parser().parse_args(argv)
    scraper = GameScraper(args.gender)
    args.func(scraper, args)


if __name__ == "__main__":
    main()
