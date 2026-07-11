"""
A tool to scrape data for NCAA D1 Women's college basketball games.

Author: Daniel Cowan
"""

from cbbpy.utils.scraper import GameScraper

_scraper = GameScraper("womens")

get_game = _scraper.get_game
get_games_range = _scraper.get_games_range
get_games_season = _scraper.get_games_season
get_games_team = _scraper.get_games_team
get_games_conference = _scraper.get_games_conference
get_game_ids = _scraper.get_game_ids
get_game_boxscore = _scraper.get_game_boxscore
get_game_pbp = _scraper.get_game_pbp
get_game_info = _scraper.get_game_info
get_player_info = _scraper.get_player_info
get_teams_from_conference = _scraper.get_teams_from_conference
get_team_schedule = _scraper.get_team_schedule
get_conference_schedule = _scraper.get_conference_schedule
