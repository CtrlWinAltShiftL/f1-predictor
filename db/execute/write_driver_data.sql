INSERT INTO f1_race_data (
    fastest_quali_lap,
    driver_finish_position,
    team_points_gp,
    driver_quali_position,
    driver_position_delta_gp,
    driver_position_delta_last_x,
    team_avg_points_last_x,
    driver_avg_finish_last_x,
    driver_avg_quali_last_x,
    rain_over_30_percent,
    avg_track_temp,
    x_race_window,
    gp_name,
	gp_year,
    driver_number,
    finished_top_3
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
