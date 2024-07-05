from mysql.connector import connect, Error
from typing import Iterable


class DBConnection:
    def __init__(
        self,
        username: str,
        password: str,
        host: str = "localhost",
        database: str = "archives",
    ) -> None:
        self.username = username
        self._password = password
        self.host = host
        self.database = database

    def __enter__(self):
        try:
            connector = connect(
                host=self.host,
                user=self.username,
                password=self._password,
                database=self.database,
            )
        except Error as e:
            raise e
        else:
            self.connector = connector
            self.cursor = self.connector.cursor(buffered=True)
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is None:
            self.connector.commit()
        else:
            self.connector.rollback()
        self.cursor.close()
        self.connector.close()

    def commit(
        self,
        operation: str,
        seq_of_params: Iterable | None = None,
        commit_later: bool = False,
    ):
        self.cursor.execute(operation, seq_of_params)
        if not commit_later:
            self.connector.commit()

    def select(self, operation: str):
        self.cursor.execute(operation)
        result = self.cursor.fetchall()
        return result
