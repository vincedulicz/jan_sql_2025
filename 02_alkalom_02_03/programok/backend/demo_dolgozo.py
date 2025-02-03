import os

import psycopg2
from abc import ABC, abstractmethod


class DatabaseConnection:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance.connection = psycopg2.connect(
                dbname='postgres',
                user='postgres',
                password='qwe123',
                host='localhost',
                port='5432'
            )
            return cls._instance

    def get_connection(self):
        return self.connection


class AbstractDatabase(ABC):
    def __init__(self):
        self.conn = DatabaseConnection().get_connection()
        self.cursor = self.conn.cursor()

    @abstractmethod
    def fetch_all(self):
        pass

    @abstractmethod
    def insert(self, *args):
        pass

    def close(self):
        self.cursor.close()
        self.conn.close()


class DolgozokDatabase(AbstractDatabase):
    def fetch_all(self):
        self.cursor.execute("SELECT * FROM dolgozok")
        return self.cursor.fetchall()

    def insert(self, id, nev, fizetes, beosztas, osztaly_id):
        self.cursor.execute(
            "INSERT INTO dolgozok "
            "(id, nev, fizetes, beosztas, osztaly_id) "
            "VALUES (%s, %s, %s, %s, %s)",
            (id, nev, fizetes, beosztas, osztaly_id)
        )
        self.conn.commit()

    def fetch_by_osztaly(self, osztaly_id):
        self.cursor.execute(
            "SELECT * FROM dolgozok "
            "WHERE osztaly_id = %s", (osztaly_id,)
        )
        return self.cursor.fetchall()

    def delete(self, id):
        self.cursor.execute("DELETE FROM dolgozok WHERE id = %s", (id,))
        self.conn.commit()


class SearchStrategy(ABC):
    @abstractmethod
    def search(self, cursor, *args):
        pass


class SearchByName(SearchStrategy):
    def search(self, cursor, name):
        cursor.execute("SELECT * FROM dolgozok WHERE nev = %s", (name,))
        return cursor.fetchall()


class SearchBySalary(SearchStrategy):
    def search(self, cursor, min_salary):
        cursor.execute("SELECT * FROM dolgozok WHERE fizetes >= %s", (min_salary,))
        return cursor.fetchall()


if __name__ == "__main__":
    db = DolgozokDatabase()

    # db.insert(14, 'Teszter Ellek', 866666, 'senior fejlesztő', 1)

    print(db.fetch_by_osztaly(1))

    search_strategy = SearchByName()
    print(search_strategy.search(db.cursor, 'Kovács Péter'))

    search_strategy = SearchBySalary()
    print(search_strategy.search(db.cursor, 500000))

    db.delete(14)

    db.close()
