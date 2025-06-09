import sqlite3

from pathlib import Path

class SQLUtils():
	def __init__(self, db_filename: str):
		self.base_dir = Path(__file__).parent
		self.schema_dir = (self.base_dir / '..' / '..' / 'db' / 'schemas').resolve()
		self.db_dir = (self.base_dir / '..' / '..' / 'db' / 'dbs').resolve()
		self.execute_dir = (self.base_dir / '..' / '..' / 'db' / 'execute').resolve()
		self.db_filename = db_filename
		self.db_path = str((self.db_dir / db_filename).resolve())
		self.conn = sqlite3.connect(self.db_path)
		self.conn.row_factory = sqlite3.Row
		self.cursor = self.conn.cursor()

	def create_table(self, schema_filename: str) -> None:
		
		schema_path = str((self.schema_dir / schema_filename).resolve())
		cursor = self.conn.cursor()

		with open(schema_path, "r") as f:
			sql_script = f.read()

		cursor.executescript(sql_script)  # executes all statements in the file

	def execute(self, sql_filename: str, variables: tuple | None = None):
		execute_path = str((self.execute_dir / sql_filename).resolve())

		with open(execute_path, "r") as f:
			sql_script = f.read()
		
		try:
			if variables:
				self.cursor.execute(sql_script, variables)
			else:
				self.cursor.execute(sql_script)
		except Exception as e:
			print(f"⚠️ Error executing {sql_filename}: {e}")
			raise

	def commit(self):
		self.conn.commit()

	def close(self):
		self.conn.close()

	def delete_all(self, table_name: str):
		self.conn.execute(f"DELETE FROM {table_name}")