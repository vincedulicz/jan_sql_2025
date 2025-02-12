import psycopg2
from abc import ABC, abstractmethod
import yaml


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
    def __init__(self, db_config, sql_file="data/mozi.yaml"):
        self.db_config = db_config
        self.sql_file = sql_file
        self.connection = None
        self.sql_queries = self.load_sql_queries(sql_file)

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
        if query_key not in self.sql_queries:
            print(f'Hiba: {query_key} nem található')
            return None
        return self.sql_queries[query_key]

    def execute_query(self, query_key, params=None):
        query = self._get_query(query_key)
        if query is None:
            return []

        with self.connection.cursor() as cursor:
            cursor.execute(query, params)

    def execute_many(self, query_key, data):
        query = self._get_query(query_key)
        if query is None:
            return []

        with self.connection.cursor() as cursor:
            cursor.executemany(query, data)
            print(f"execute_many() végrehajtva: {query_key}")

    def fetch_all(self, query_key, params=None):
        query = self._get_query(query_key)
        if query is None:
            return []

        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
            return result


class App:
    def __init__(self, db_config, sql_file):
        self.db = DatabaseHandler(db_config, sql_file)

    @staticmethod
    def execute_step(step_func):
        """ Különböző adatbázis műveletek végrehajtása """
        print(f"### Starting {step_func.__name__} ###")
        step_func()
        print(f"### Finished {step_func.__name__} ###\n")

    def execute_query_from_yaml(self, query_key, params=None):
        """ Lekérdezés végrehajtása YAML fájlból """
        if query_key in ['create_tables', 'insert_dummy_data']:  # Ha nem kell fetch, akkor execute-t használunk
            self.db.execute_query(query_key, params)
        else:
            result = self.db.fetch_all(query_key, params)
            self.print_results(result)

    @staticmethod
    def print_results(result):
        """ Eredmények kiírása """
        for row in result:
            print(row)

    def create_tables(self):
        """ Táblák létrehozása """
        self.execute_query_from_yaml('create_tables')
        print("Tábla létrehozva")

    def insert_dummy_data(self):
        """ Minta adatok beszúrása """
        self.execute_query_from_yaml('insert_dummy_data')
        print("Insert sikeres volt")

    def get_cinemas_and_employee_count(self):
        """ Minden mozi és a dolgozók száma """
        self.execute_query_from_yaml('get_cinemas_and_employee_count')

    def get_bars_and_employees_count(self):
        """ Büfék és az ott dolgozó alkalmazottak száma """
        self.execute_query_from_yaml('get_bars_and_employees_count')

    def get_movie_count_by_category(self):
        """ Hány film tartozik egy adott kategóriába? """
        self.execute_query_from_yaml('get_movie_count_by_category')

    def get_most_popular_movie_category(self):
        """ A legnépszerűbb film kategória """
        self.execute_query_from_yaml('get_most_popular_movie_category')

    def get_cinema_with_most_employees(self):
        """ Melyik moziban dolgozik a legtöbb alkalmazott? """
        self.execute_query_from_yaml('get_cinema_with_most_employees')

    def get_all_movies_in_alphabetical_order(self):
        """ Minden film betűrendben """
        self.execute_query_from_yaml('get_all_movies_in_alphabetical_order')

    def get_vip_customers_count(self):
        """ Hány VIP vásárló van? """
        self.execute_query_from_yaml('get_vip_customers_count')

    def get_avg_employees_per_cinema(self):
        """ Átlagosan hány dolgozó van egy moziban? """
        self.execute_query_from_yaml('get_avg_employees_per_cinema')

    def get_city_with_most_cinemas(self):
        """ Melyik városban van a legtöbb mozi? """
        self.execute_query_from_yaml('get_city_with_most_cinemas')

    def get_last_added_movie(self):
        """ Legutoljára hozzáadott film """
        self.execute_query_from_yaml('get_last_added_movie')

    def get_cinemas_and_employees_names(self):
        """ Minden mozi és az ott dolgozó alkalmazottak neve """
        self.execute_query_from_yaml('get_cinemas_and_employees_names')

    def get_movies_starting_with_e(self):
        """ Minden film, amelynek címe 'E' betűvel kezdődik """
        self.execute_query_from_yaml('get_movies_starting_with_e')

    def run(self):
        """ Az összes művelet végrehajtása """
        try:
            self.db.connect()
            steps = [
                self.create_tables,
                self.insert_dummy_data,
                self.get_cinemas_and_employee_count,
                self.get_bars_and_employees_count,
                self.get_movie_count_by_category,
                self.get_most_popular_movie_category,
                self.get_cinema_with_most_employees,
                self.get_all_movies_in_alphabetical_order,
                self.get_vip_customers_count,
                self.get_avg_employees_per_cinema,
                self.get_city_with_most_cinemas,
                self.get_last_added_movie,
                self.get_cinemas_and_employees_names,
                self.get_movies_starting_with_e,
            ]
            for step_func in steps:
                self.execute_step(step_func)
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
    sql_file = "data/mozi.yaml"

    app = App(db_config, sql_file)
    app.run()


if __name__ == "__main__":
    main()
