CREATE TABLE f1_race_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    gp_name TEXT NOT NULL,
    driver_number TEXT NOT NULL,
    fastest_quali_lap REAL,
    team_points_gp INTEGER,
    driver_finish_position INTEGER,
    driver_quali_position INTEGER,
    driver_position_delta_gp INTEGER,
    driver_position_delta_last_x INTEGER,
    team_avg_points_last_x REAL,
    driver_avg_finish_last_x REAL,
    driver_avg_quali_last_x REAL,
    rain_over_30_percent BOOLEAN,
    avg_track_temp REAL,
    x_race_window INTEGER,
    finished_top_3 BOOLEAN NOT NULL,
    -- Optional for future joins:
    FOREIGN KEY (driver_number) REFERENCES drivers(driver_number)
);
