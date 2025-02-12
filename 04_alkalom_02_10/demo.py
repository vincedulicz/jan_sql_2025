import psycopg2
import yaml
from abc import ABC, abstractmethod


class DatabaseHandlerInterface(ABC):
    @abstractmethod
    def connect(self):
        """ csatlakozás az adatbázishoz """
        pass

    @abstractmethod
    def close(self):
        """ db kapcsolat lezárázása """
        pass

    @abstractmethod
    def execute_query(self, query_key, params=None):
        """ lekérdezés végrehajtása - yaml fájlból """
        pass

    @abstractmethod
    def fetch_all(self, query_key, params=None):
        """ lekérdezés végrehajtás - összes sor beolvasása """
        pass

    @abstractmethod
    def execute_many(self, query_key, data):
        """ adatbeszúrás végrehajtása """
        pass


class DatabaseHandler(DatabaseHandlerInterface):
    _instance = None

    def __new__(cls, db_config, sql_file="data/queries_example.yaml"):
        if cls._instance is None:
            cls._instance = super(DatabaseHandler, cls).__new__(cls)
            cls._instance._initalize(db_config, sql_file)
            return cls._instance

    def _initalize(self, db_config, sql_file):
        """ egyszeri init """
        self.db_config = db_config
        self.sql_queries = self.load_sql_queries(sql_file)
        self.connection = None
        self.initalized = True

    def load_sql_queries(self, sql_file):
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


def step(func):
    def wrapper(*args, **kwargs):
        print(f"### Starting {func.__name__} ###")
        result = func(*args, **kwargs)
        print(f"### Finished {func.__name__} ###\n")
        return result

    return wrapper


class App:
    def __init__(self, db_config, sql_file):
        self.db = DatabaseHandler(db_config, sql_file)

    @step
    def create_tables(self):
        self.db.execute_query('create_tables')
        print("Tábla létrehozva")

    @step
    def insert_dummy_data(self):
        self.db.execute_query('insert_customers')
        self.db.execute_query('insert_loans')
        print("Insert sikeres volt")

    @step
    def run_offset_query(self):
        customer_offset = self.db.fetch_all('query_offset')
        print("Ügyfelek: ")
        for row in customer_offset:
            print(row)

    @step
    def run_having_query(self):
        loans_having = self.db.fetch_all('query_having')
        print("Ügyfelek kölcsönszáma: ")
        for row in loans_having:
            print(row)

    @step
    def run_complex_join_query(self):
        complex_result = self.db.fetch_all('query_complex')
        print("Összes kölcsön > 5000: ")
        for row in complex_result:
            print(row)

    @step
    def run_subqery(self):
        subquery_result = self.db.fetch_all('query_subquery')
        print("Aktív vs inaktív kölcsönök: ")
        for row in subquery_result:
            print(row)

    @step
    def bulk_insert_customers(self):
        bulk_customers = [
            ('Teszt', 'Elek', 'testelek@tst.com', '666-666'),
            ('Teszt2', 'Elek2', '2testelek@tst.c2om', '6626-6266')
        ]
        self.db.execute_many('insert_bulk_customers', bulk_customers)

    @step
    def run_offset_join_query(self):
        offset_join_result = self.db.fetch_all('query_offset_join')
        for row in offset_join_result:
            print(row)

    @step
    def delete_closed_loans(self):
        self.db.execute_query('query_delete')
        print("closed státusz törölve")

    def run(self):
        try:
            self.db.connect()
            steps = [
                self.create_tables,
                self.insert_dummy_data,
                self.run_offset_join_query,
                self.run_having_query,
                self.run_complex_join_query,
                self.run_subqery,
                self.bulk_insert_customers,
                self.run_offset_join_query,
                self.delete_closed_loans
            ]
            for step_func in steps:
                print(f"### step_func: {step_func} ### \n")
                step_func()
        finally:
            self.db.close()


def main():
    db_config = {
        'dbname': 'dbname',
        'user': 'user',
        'password': 'password',
        'host': 'localhost',
        'port': 5432
    }
    sql_file = "data/queries_example.yaml"

    app = App(db_config, sql_file)
    app.run()


if __name__ == "__main__":
    main()
