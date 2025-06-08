import sqlite3

from pathlib import Path

class SQLUtils():
	def __init__(self):
		self.base_dir = Path(__file__).parent
		self.schema_dir = (self.base_dir / '..' / '..' / 'db' / 'schemas').resolve()
		self.db_dir = (self.base_dir / '..' / '..' / 'db' / 'dbs').resolve()

	def run_query(self, sql_filename: str, db_filename: str) -> None:
		db_path = str((self.db_dir / db_filename).resolve())
		sql_path = str((self.schema_dir / sql_filename).resolve())

		conn = sqlite3.connect(db_path)
		cursor = conn.cursor()

		with open(sql_path, "r") as f:
			sql_script = f.read()

		cursor.executescript(sql_script)  # executes all statements in the file

		conn.commit()
		conn.close()

if __name__ == "__main__":
	sql = SQLUtils()
	sql.run_query('f1_stats_schema.sql', 'f1_data.db')