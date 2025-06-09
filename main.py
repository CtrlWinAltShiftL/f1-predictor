from src.utils.config_loader import load_config
from src.utils.sql_utils import SQLUtils
from src.etl.data_fetcher import F1DataFetcher
from fastf1 import get_event_schedule

CONFIG = load_config()

def load(
		fetcher: F1DataFetcher,
		sql: SQLUtils,
		start_year: int | None = None,
		end_year: int | None = None,
		clear_db:  bool = False
		):

	if clear_db:
		try:
			sql.delete_all("f1_race_data")
			sql.commit()
		except:
			print("⚠️ Error deleting table. Continuing...")

	sql.create_table("f1_stats_schema.sql")

	if not start_year:
		start_year = CONFIG['YEAR_RANGE'][0]
	if not end_year:
		end_year = CONFIG['YEAR_RANGE'][1]

	for year in range(start_year, end_year+1): # type: ignore
		for gp_name in get_event_schedule(year, include_testing=False)['EventName']:

			race = fetcher.get_and_load_session(year, gp_name, "R")
			quali = fetcher.get_and_load_session(year, gp_name, "Q")

			race_window = fetcher.rolling_race_window(race)
			quali_window = fetcher.rolling_race_window(race, quali_mode=True)

			for idx, driver_number in enumerate(race.drivers):
				driver_data = fetcher.load_driver_data(driver_number, race, quali, race_window, quali_window)
				
				if driver_data['finishing_position'] <= 3:
					finished_in_top_3 = True
				else:
					finished_in_top_3 = False

				sql.execute(
					"write_driver_data.sql",
					(
					driver_data['fastest_quali_lap'],
					driver_data['finishing_position'],
					driver_data['team_points'],
					driver_data['quali_position'],
					driver_data['positions_gained_lost'],
					driver_data['rolling_average_positions_gained_lost'],
					driver_data['rolling_average_team_points'],
					driver_data['rolling_average_finish_position'],
					driver_data['rolling_average_quali_position'],
					driver_data['rained'],
					driver_data['avg_track_temp'],
					driver_data['rolling_stats_window'],
					gp_name,
					year,
					driver_number,
					finished_in_top_3
					)
				)

				if idx % CONFIG['BATCH_SIZE'] == 0:
					sql.commit()
	sql.close()

fetcher = F1DataFetcher()
sql = SQLUtils("f1_data.db")

load(fetcher, sql, clear_db=True)