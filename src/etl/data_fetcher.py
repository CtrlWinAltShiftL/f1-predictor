# Run using:
# uv run python -m src.etl.data_fetcher

import pandas as pd
import numpy as np
from fastf1 import get_session, get_events_remaining, get_event_schedule
from fastf1.core import Session
from fastf1.ergast import Ergast

from utils.config_loader import load_config
from utils.dataframe_utils import filter, countrows

class F1DataFetcher():
	def __init__(self):
		self.connected = self.test_connection()

		if not self.connected:
			raise ConnectionError("Could not load session data")
		
		self.config = load_config()
		self.ergast = Ergast()
	
	def test_connection(self):
		"""Tests that a connection can be made to the FastF1 API

		Returns:
			bool: whether data has been fetched
		"""		
		session = get_session(2023, "monza", "Q")
		return isinstance(session, Session)
	
	def get_next_race(self) -> pd.Series:
		"""Gets details about the next race in the series

		Returns:
			pd.Series: Details on the next race in the series.
			Keys include: `Country`, `RoundNumber`, `EventName` and more.
		"""		
		remaining_events = get_events_remaining()
		return remaining_events.sort_values(by="Session5DateUtc", ascending=True).iloc[0]
	
	def get_drivers(self, start_year: int | None = None, end_year: int | None = None) -> list:
		"""Provides a list of all drivers (by number) involved in the specified years.

		Args:
			start_year (int | None, optional): First year to consider. Defaults to None.
			end_year (int | None, optional): Final year to consider. Defaults to None.

		Returns:
			list: driver numbers
		"""		
		if not start_year:
			start_year = self.config['YEAR_RANGE'][0]
		if not end_year:
			end_year = self.config['YEAR_RANGE'][1]

		driver_codes = []

		for year in range(start_year, end_year+1): # type: ignore
			standings = self.ergast.get_driver_standings(season=year).content[0] # type: ignore
			driver_codes += list(standings['driverCode'])
		
		return list(set(driver_codes)) # Removes duplicates
	
	def load_driver_data(self, driver_number: str, race: Session, quali: Session, rolling_window: int = 5) -> pd.Series:

		driver_data = {}

		# collect data referring to this gp

		driver_data['fastest_quali_lap'] = self.fastest_quali_lap(driver_number, quali)
		driver_data['finishing_position'] = self.finishing_position(driver_number, race)
		driver_data['team_points'] = self.team_points(driver_number, race)
		driver_data['quali_position'] = self.finishing_position(driver_number, quali)
		driver_data['positions_gained_lost'] = self.positions_gained_lost(driver_number, race)

		# collect rolling aggregated data

		# rolling_average_positions_gained_lost = self.rolling_average_positions_gained_lost(driver_number, race, rolling_window)
		# rolling_average_team_points = self.rolling_average_team_points(driver_number, race, rolling_window)
		# rolling_average_finish_position = self.rolling_average_finish_position(driver_number, race, rolling_window)
		# rolling_average_quali_position = self.rolling_average_quali_position(driver_number, quali, rolling_window)

		driver_data['rained'] = self.rained(race)
		driver_data['avg_track_temp'] = self.avg_track_temp(race)

		return pd.Series(driver_data)

	def rained(self, race: Session) -> bool | None:
		try:
			df = race.weather_data
			filtered_df_raining = filter(df, "Rainfall", np.True_) # type: ignore
			num_raining_timestamps = countrows(filtered_df_raining)
			num_timestamps = countrows(df) # type: ignore

			if num_raining_timestamps / num_timestamps >= self.config['RAINY_THRESHOLD']:
				return True
			else:
				return False
		except:
			return None
		
	def avg_track_temp(self, race: Session) -> float | None:
		try:
			return race.weather_data['TrackTemp'].mean() # type: ignore
		except:
			return None
		
	def fastest_quali_lap(self, driver_number: str, quali: Session) -> float | None:
		try:
			lap = quali.laps.pick_drivers(driver_number).pick_fastest() # type: ignore
			return round(lap['LapTime']._value * 1e-9, 3) # type: ignore # In seconds, down to 1 thousandth
		except:
			return None
		
	def finishing_position(self, driver_number: str, race: Session) -> int | None:
		try:
			return int(self.interrogate_results_by_driver(race, "Position", driver_number))
		except:
			return None
		
	def starting_position(self, driver_number: str, race: Session) -> int | None:
		try:
			return int(self.interrogate_results_by_driver(race, "GridPosition", driver_number))
		except:
			return None
		
	def positions_gained_lost(self, driver_number: str, race: Session) -> int | None:
		try:
			return self.starting_position(driver_number, race) - self.finishing_position(driver_number, race) # type: ignore
		except:
			return None
		
	def team_points(self, driver_number: str, race: Session) -> int | None:
		try:
			driver_df = filter(race.results, "DriverNumber", driver_number)
			team_name = driver_df['TeamName'].iloc[0]
			team_df = filter(race.results, "TeamName", team_name)
			return int(team_df['Points'].sum())
		except:
			return None

	def interrogate_results_by_driver(self, race: Session, look_for: str, driver_number: str):
		return race.results.loc[race.results["DriverNumber"] == driver_number, look_for].iloc[0]
	
	def load(self, start_year: int | None = None, end_year: int | None = None) -> pd.DataFrame:

		# For each year since start year
			# For each gp in season
				# For each driver in gp
					# Get:
					# - Fastest quali lap [x]
					# - Team points this gp [x]
					# - Driver finish this gp [x]
					# - Driver quali position this gp [x]
					# - Driver position delta this gp [x]
					# - Driver position delta last x races [x]
					# - Team avg points last x races [x]
					# - Driver average finish last x races [x]
					# - Driver average quali position last x races
					# - weather conditions

		if not start_year:
			start_year = self.config['YEAR_RANGE'][0]
		if not end_year:
			end_year = self.config['YEAR_RANGE'][1]

		for year in range(start_year, end_year+1): # type: ignore
			for gp in get_event_schedule(year, include_testing=False):
				gp_name = gp['EventName']
				race = get_session(year, gp_name, "R")
				quali = get_session(year, gp_name, "Q")
				race.load()
				quali.load()
				for driver_number in race.drivers:
					pass

		return pd.DataFrame # type: ignore

if __name__=='__main__':
	fetcher = F1DataFetcher()
	drivers = fetcher.get_drivers(2012, 2012)
	print(drivers)