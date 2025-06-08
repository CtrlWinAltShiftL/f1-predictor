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
	
	def load_driver_data(
			self,
			driver_number: str,
			race: Session,
			quali: Session,
			rolling_race_window: list[Session],
			rolling_quali_window: list[Session]
			) -> pd.Series:

		driver_data = {}

		# collect data referring to this gp

		driver_data['fastest_quali_lap'] = self.fastest_quali_lap(driver_number, quali)
		driver_data['finishing_position'] = self.finishing_position(driver_number, race)
		driver_data['team_points'] = self.team_points(driver_number, race)
		driver_data['quali_position'] = self.finishing_position(driver_number, quali)
		driver_data['positions_gained_lost'] = self.positions_gained_lost(driver_number, race)

		# collect rolling aggregated data

		driver_data['rolling_average_positions_gained_lost'] = self.avg_positions_gained_lost(rolling_race_window, driver_number)
		driver_data['rolling_average_team_points'] = self.avg_team_points(rolling_race_window, driver_number)
		driver_data['rolling_average_finish_position'] = self.avg_finish_position(rolling_race_window, driver_number)
		driver_data['rolling_average_quali_position'] = self.avg_quali_position(rolling_quali_window, driver_number)

		driver_data['rained'] = self.rained(race)
		driver_data['avg_track_temp'] = self.avg_track_temp(race)

		driver_data['rolling_stats_window'] = self.config['STATS_ROLLING_WINDOW']

		return pd.Series(driver_data)
	
	def prev_race(self, race: Session, quali_mode: bool = False) -> Session | None: # type: ignore
		mode = "R"
		if quali_mode:
			mode = "Q"
		
		try:
			current_race_number = int(race.event.RoundNumber)
			current_year = race.event.EventDate.year
			if current_race_number == 1 and self.config['ROLLING_OVER_SEASONS'] == True:
				last_year = current_year - 1
				last_year_event_schedule = get_event_schedule(last_year, include_testing=False)
				last_race_name = last_year_event_schedule.iloc[-1]['EventName']
				return get_session(last_year, last_race_name, mode)
			else:
				race_number = current_race_number - 1
				return get_session(current_year, race_number, mode)
		except:
			return None
		
	def rolling_race_window(self, race: Session, inc_this_race: bool = False, quali_mode: bool = False) -> list[Session]:
		if inc_this_race:
			race_range = range(self.config['STATS_ROLLING_WINDOW'])
		else:
			race_range = range(1, self.config['STATS_ROLLING_WINDOW'] + 1)
		
		rolling_race_window = []

		for i in race_range:
			if i > min(race_range):
				race = prev_race
			prev_race = self.prev_race(race, quali_mode=quali_mode)
			prev_race.load() # type: ignore
			rolling_race_window.append(prev_race)

		return rolling_race_window
	
	def avg_positions_gained_lost(self, rolling_race_window: list[Session], driver_number: str) -> float:
		gained_lost = []
		for race in rolling_race_window:
			gained_lost.append(self.positions_gained_lost(driver_number, race))
		return float(np.array(gained_lost).mean())

	def avg_team_points(self, rolling_race_window: list[Session], driver_number: str) -> float:
		team_pts = []
		for race in rolling_race_window:
			team_pts.append(self.team_points(driver_number, race))
		return float(np.array(team_pts).mean())

	def avg_finish_position(self, rolling_race_window: list[Session], driver_number: str) -> float:
		position = []
		for race in rolling_race_window:
			position.append(self.finishing_position(driver_number, race))
		return float(np.array(position).mean())
	
	def avg_quali_position(self, rolling_quali_window: list[Session], driver_number: str) -> float:
		position = []
		for quali in rolling_quali_window:
			position.append(self.finishing_position(driver_number, quali))
		return float(np.array(position).mean())

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
				gp_name = gp['EventName'] # type: ignore

				race = get_session(year, gp_name, "R")
				quali = get_session(year, gp_name, "Q")
				race.load()
				quali.load()

				race_window = self.rolling_race_window(race)
				quali_window = self.rolling_race_window(race, quali_mode=True)

				for driver_number in race.drivers:
					pass

		return pd.DataFrame # type: ignore

	def get_and_load_session(self, year: int, gp: str | int, identifier: int | str | None) -> Session:
		session = get_session(year, gp, identifier=identifier)
		session.load()
		return session

if __name__=='__main__':
	fetcher = F1DataFetcher()
	r = fetcher.get_and_load_session(2025, "Spain", "R")
	q = fetcher.get_and_load_session(2025, "Spain", "Q")
	rw = fetcher.rolling_race_window(r)
	qw = fetcher.rolling_race_window(r, quali_mode=True)
	driver_data = fetcher.load_driver_data("44", r, q, rw, qw)
	pass