import sqlite3

from pathlib import Path

class SQLUtils():
	def __init__(self, db_filename: str):
		self.base_dir = Path(__file__).parent
		self.schema_dir = (self.base_dir / '..' / '..' / 'db' / 'schemas').resolve()
		self.db_dir = (self.base_dir / '..' / '..' / 'db' / 'dbs').resolve()
		self.db_filename = db_filename
		self.db_path = str((self.db_dir / db_filename).resolve())
		self.conn = sqlite3.connect(self.db_path)

	def create_table(self, sql_filename: str) -> None:
		
		sql_path = str((self.schema_dir / sql_filename).resolve())
		cursor = self.conn.cursor()

		with open(sql_path, "r") as f:
			sql_script = f.read()

		cursor.executescript(sql_script)  # executes all statements in the file

	def commit(self):
		self.conn.commit()

	def close(self):
		self.conn.close()

if __name__ == "__main__":
	sql = SQLUtils('f1_data.db')
	sql.create_table('f1_stats_schema.sql')
	sql.commit()
	sql.close()