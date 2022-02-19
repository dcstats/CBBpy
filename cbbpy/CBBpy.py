from bs4 import BeautifulSoup as bs
import requests as r


BASE_URL = "https://www.espn.com/mens-college-basketball/"


class CBBpy:
    def __init__(self):
        pass

    def scrape_game(self, id):
        pass

    def _get_game_pbp(self, id):
        pass

    def _get_game_box(self, id):
        pass

    def _get_game_tm_stats(self, id):
        pass

    def scrape_day(self, date):
        pass

    def scrape_season(self, season):
        pass

    def scrape_team_stats(self, team, season):
        pass

    def scrape_standings(self, season, conference):
        pass

    def scrape_rankings(self, season, week):
        pass
