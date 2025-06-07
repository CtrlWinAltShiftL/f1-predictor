from src.etl.data_fetcher import F1DataFetcher
from fastf1.core import Session
from fastf1 import get_session

def test_connectivity():
    fetcher = F1DataFetcher()
    assert fetcher.test_connection()

def test_rained():
    fetcher = F1DataFetcher()
    rainy_race = get_session(2024, "Brazil", "R")
    dry_race = get_session(2023, "Spain", "R")
    almost_dry_race = get_session(2024, "Spain", "R")
    rainy_race.load()
    dry_race.load()
    almost_dry_race.load()
    assert fetcher.rained(rainy_race) == True
    assert fetcher.rained(dry_race) == False
    assert fetcher.rained(almost_dry_race) == False

def test_track_temp():
    fetcher = F1DataFetcher()
    race = get_session(2024, "Miami", "R")
    race.load()
    avg_track_temp = fetcher.avg_track_temp(race)
    assert isinstance(avg_track_temp, float)
    assert avg_track_temp > 40
    assert avg_track_temp < 47

def test_team_points():
    fetcher = F1DataFetcher()
    race = get_session(2025, "Spain", "R")
    race.load()
    assert fetcher.team_points("4", race) == 43

def test_finishing_position():
    fetcher = F1DataFetcher()
    race = get_session(2025, "Spain", "R")
    quali = get_session(2025, "Spain", "Q")
    race.load()
    quali.load()
    assert fetcher.finishing_position("81", race) == 1
    assert fetcher.finishing_position("81", quali) == 1

def test_gained_lost():
    fetcher = F1DataFetcher()
    race = get_session(2025, "Spain", "R")
    race.load()
    assert fetcher.positions_gained_lost("1", race) == -7