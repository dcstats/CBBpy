"""Offline tests for the cbbpy CLI (replays recorded ESPN fixtures)."""

import pytest

from cbbpy import cli
from tests.conftest import SCOREBOARD_DATES

GAME_ID = "401581583"  # mens, has recorded api + html fixtures
DATE = SCOREBOARD_DATES["mens"]


def test_game_writes_three_csvs(offline_espn, tmp_path, capsys):
    cli.main(["game", GAME_ID, "-o", str(tmp_path)])
    written = {p.name for p in tmp_path.glob("*.csv")}
    assert written == {
        f"mens_game_{GAME_ID}_info.csv",
        f"mens_game_{GAME_ID}_boxscore.csv",
        f"mens_game_{GAME_ID}_pbp.csv",
    }
    for p in tmp_path.glob("*.csv"):
        assert p.stat().st_size > 0


def test_no_pbp_suppresses_pbp_file(offline_espn, tmp_path):
    cli.main(["game", GAME_ID, "-o", str(tmp_path), "--no-pbp"])
    written = {p.name for p in tmp_path.glob("*.csv")}
    assert f"mens_game_{GAME_ID}_pbp.csv" not in written
    assert f"mens_game_{GAME_ID}_info.csv" in written
    assert f"mens_game_{GAME_ID}_boxscore.csv" in written


def test_ids_prints_ids(offline_espn, capsys):
    cli.main(["ids", DATE])
    out = capsys.readouterr().out.split()
    assert len(out) > 0
    assert all(gid.isdigit() for gid in out)


@pytest.mark.parametrize("argv", [
    ["game", GAME_ID, "--source", "xml"],
    ["game", GAME_ID, "-g", "coed"],
])
def test_bad_choice_exits_nonzero(argv):
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code != 0
