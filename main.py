from src.utils.config_loader import load_config
from src.utils.sql_utils import SQLUtils
from src.etl.data_fetcher import F1DataFetcher
from fastf1 import get_event_schedule, get_session

CONFIG = load_config()

def load(
		fetcher: F1DataFetcher,
		sql: SQLUtils,
		start_year: int | None = None,
		end_year: int | None = None
		):

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
			start_year = CONFIG['YEAR_RANGE'][0]
		if not end_year:
			end_year = CONFIG['YEAR_RANGE'][1]

		for year in range(start_year, end_year+1): # type: ignore
			for gp in get_event_schedule(year, include_testing=False):
				gp_name = gp['EventName'] # type: ignore

				race = fetcher.get_and_load_session(year, gp_name, "R")
				quali = fetcher.get_and_load_session(year, gp_name, "Q")

				race_window = fetcher.rolling_race_window(race)
				quali_window = fetcher.rolling_race_window(race, quali_mode=True)

				for driver_number in race.drivers:
					driver_data = fetcher.load_driver_data(driver_number, race, quali, race_window, quali_window)
