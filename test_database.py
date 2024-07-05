from getpass import getpass
import unittest
import os

from database import DBConnection

USER = os.environ["MYSQL_USER"]
PASS = os.environ["MYSQL_PASS"]

CREATE_STATEMENT = """
CREATE TABLE pytest (
    id INT(11) NOT NULL AUTO_INCREMENT,
    firstname VARCHAR(20),
    PRIMARY KEY (id)
)
"""
INSERT_STATEMENT = """
INSERT INTO pytest (firstname)
VALUES (%s), (%s), (%s)
"""
INSERT_PARAMS = ["charlemagne", "louis", "richard"]
VALUES = [(1, "charlemagne"), (2, "louis"), (3, "richard")]


class Database(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DBConnection(username=USER, password=PASS)

    def test_create_table(self):
        with self.db as db:
            db.commit("DROP TABLE IF EXISTS pytest")
            db.commit(CREATE_STATEMENT)
            db.commit(INSERT_STATEMENT, INSERT_PARAMS)
            contents = db.select("SELECT * FROM pytest")
            self.assertListEqual(VALUES, contents)


if __name__ == "__main__":
    unittest.main()
