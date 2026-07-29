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


def test_info_writes_one_file(offline_espn, tmp_path):
    cli.main(["info", GAME_ID, "-o", str(tmp_path)])
    written = [p for p in tmp_path.glob("*.csv")]
    assert [p.name for p in written] == [f"mens_info_{GAME_ID}.csv"]
    assert written[0].stat().st_size > 0


def test_info_stdout_prints_transposed(offline_espn, tmp_path, capsys):
    cli.main(["info", GAME_ID, "--stdout", "-o", str(tmp_path)])
    assert list(tmp_path.glob("*")) == []
    lines = capsys.readouterr().out.splitlines()
    # transposed: each column is its own line, field name at the start
    assert any(line.startswith("game_id") for line in lines)


def test_box_stdout_prints_csv(offline_espn, tmp_path, capsys):
    cli.main(["box", GAME_ID, "--stdout", "-o", str(tmp_path)])
    assert list(tmp_path.glob("*")) == []
    lines = capsys.readouterr().out.splitlines()
    header = lines[0].split(",")
    assert "game_id" in header


def test_box_stdout_tty_pages_aligned(offline_espn, tmp_path, monkeypatch):
    monkeypatch.setattr(cli.sys.stdout, "isatty", lambda: True, raising=False)
    captured = {}

    class FakePager:
        def communicate(self, text):
            captured["text"] = text

    monkeypatch.setattr(cli.subprocess, "Popen", lambda *a, **k: FakePager())
    cli.main(["box", GAME_ID, "--stdout", "-o", str(tmp_path)])
    text = captured["text"]
    header = text.splitlines()[0]
    # aligned to_string rendering: space-separated columns, not comma-joined CSV
    assert "game_id" in header
    assert "," not in header


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


def test_verbose_flag_sets_info(offline_espn, tmp_path, monkeypatch):
    # -v raises the log level to INFO before scraping; patch set_log_level so the
    # test neither mutates the real logger nor leaks CBBPY_LOG_LEVEL
    calls = []
    monkeypatch.setattr(cli, "set_log_level", lambda level: calls.append(level))
    cli.main(["game", GAME_ID, "-o", str(tmp_path), "-v"])
    assert calls == ["INFO"]
    assert list(tmp_path.glob("*.csv"))
