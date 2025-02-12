import psycopg2
import yaml
from abc import ABC, abstractmethod


class DatabaseHandlerInterface(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def execute_query(self, query_key, params=None):
        pass

    @abstractmethod
    def fetch_all(self, query_key, params=None):
        pass

    @abstractmethod
    def execute_many(self, query_key, data):
        pass


class DatabaseHandler(DatabaseHandlerInterface):
    def __init__(self, db_config, sql_file="data/flight_sql.yaml"):
        self.db_config = db_config
        self.sql_queries = self.load_sql_queries(sql_file)
        self.connection = None

    @staticmethod
    def load_sql_queries(sql_file):
        with open(sql_file, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = True
            print("DB kapcsolat létrejött")
        except Exception as e:
            print(f'Hiba: {e}')
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            print("DB kapcsolat lezárva")

    def _get_query(self, query_key):
        return self.sql_queries.get(query_key)

    def execute_query(self, query_key, params=None):
        query = self._get_query(query_key)
        if query is None:
            return
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)

    def execute_many(self, query_key, data):
        query = self._get_query(query_key)
        if query is None:
            return
        with self.connection.cursor() as cursor:
            cursor.executemany(query, data)

    def fetch_all(self, query_key, params=None):
        query = self._get_query(query_key)
        if query is None:
            return []
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()


def step(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        print(result)
        return result
    return wrapper


class App:
    def __init__(self, db_config, sql_file):
        self.db = DatabaseHandler(db_config, sql_file)

    @step
    def create_tables(self):
        self.db.execute_query('create_tables')

    def insert_flight(self, flight_number, destination, seats):
        self.db.execute_query('insert_flight', (flight_number, destination, seats))

    def insert_passenger(self, name, email):
        self.db.execute_query('insert_passenger', (name, email))

    def create_booking(self, flight_id, passenger_id):
        self.db.execute_query('create_booking', (flight_id, passenger_id))

    @step
    def get_full_flights(self):
        return self.db.fetch_all('get_full_flights')

    @step
    def get_passengers_with_at_least_1_bookings(self):
        return self.db.fetch_all('get_passengers_with_at_least_1_bookings')

    def run(self):
        try:
            self.db.connect()
            self.get_full_flights()
            self.get_passengers_with_at_least_1_bookings()
        finally:
            self.db.close()


def main():
    db_config = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'pass',
        'host': 'localhost',
        'port': 5432
    }

    sql_file = "data/flight_sql.yaml"
    app = App(db_config, sql_file)
    app.run()


if __name__ == "__main__":
    main()
