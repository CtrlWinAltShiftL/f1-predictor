	
from src.utils.sql_utils import SQLUtils

def test_read_write():
	# Setup
	sql = SQLUtils('f1_data.db')
	test_tuple = ("test_gp", "44", True)

	# Test creating data
	sql.execute("test_create.sql", test_tuple)
	sql.commit()
	sql.cursor.execute("SELECT * FROM f1_race_data WHERE gp_name = ?", (test_tuple[0],))
	rows = sql.cursor.fetchall()
	assert rows[0]['driver_number'] == test_tuple[1]

	# Test deleting data
	sql.execute("test_delete.sql", (test_tuple[0],))
	sql.commit()
	sql.cursor.execute("SELECT * FROM f1_race_data WHERE gp_name = ?", (test_tuple[0],))
	res = sql.cursor.fetchall()
	assert len(res) == 0