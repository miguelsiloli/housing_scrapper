from contextlib import contextmanager
import duckdb
from typing import Iterator

class DatabaseConnection:
    def __init__(self, db_file: str = None) -> None:
        """Class to connect to a DuckDB database.
        
        Args:
            db_file (str, optional): Database file for persistent storage. Defaults to None (in-memory database).
        """
        self._db_file = db_file or ':memory:'  # Use in-memory if no file is provided

    @contextmanager
    def managed_cursor(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Function to create a managed database connection.
        
        Yields:
            duckdb.DuckDBPyConnection: A DuckDB connection.
        """
        # Connect to DuckDB (either in-memory or using a file)
        conn = duckdb.connect(self._db_file)
        try:
            yield conn
        finally:
            conn.close()

    def __str__(self) -> str:
        return f'duckdb://{self._db_file}'

# Example usage
db = DatabaseConnection('data/myduckdb.db')  # Specify your database file here