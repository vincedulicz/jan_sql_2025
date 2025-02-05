import psycopg2
from abc import ABC, abstractmethod


class DatabaseConnection:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
            cls._instance.connection = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="qwe123",
                host="127.0.0.1",
                port="5432"
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
        """ adatok lekérdezée a táblából """
        pass

    @abstractmethod
    def insert(self, *args):
        """ beszúr egy új rekordot """
        pass

    def close(self):
        """ zárjuk a kapcsolatot """
        self.cursor.close()
        self.conn.close()


class IdojarasDatabase(AbstractDatabase):
    def fetch_all(self):
        self.cursor.execute("SELECT * FROM idojaras")
        return self.cursor.fetchall()

    def insert(self, varos, homerseklet, paratartalom, szelsebesseg):
        self.cursor.execute(
            "INSERT INTO idojaras"
            "(varos, homerseklet, paratartalom, szelsebesseg)"
            "VALUES (%s, %s, %s, %s)",
            (varos, homerseklet, paratartalom, szelsebesseg)
        )

    def fetch_by_varos(self, varos):
        self.cursor.execute(
            "SELECT * FROM idojaras"
            " WHERE varos = %s", (varos,)
        )
        return self.cursor.fetchall()

    def delete(self, id):
        self.cursor.execute(
            "DELETE FROM idojaras"
            " WHERE id = %s", (id,)
        )
        self.conn.commit()

    def update_temperature(self, id, new_temp):
        self.cursor.execute(
            "UPDATE idojaras "
            "SET homerseklet = %s "
            "WHERE id = %s", (new_temp, id)
        )
        self.conn.commit()

    def fetch_by_temp_range(self, min_temp, max_temp):
        self.cursor.execute(
            "SELECT * FROM idojaras"
            " WHERE homerseklet"
            " BETWEEN %s AND %s", (min_temp, max_temp)
        )
        self.cursor.fetchall()

    def fetch_ordered_by_date(self, desc=True):
        order = 'DESC' if desc else 'ASC'
        self.cursor.execute(
            f'SELECT * FROM idojaras ORDER BY datum {order}'
        )
        return self.cursor.fetchall()

    def count_records(self):
        self.cursor.execute(
            "SELECT COUNT(*) FROM idojaras"
        )
        return self.cursor.fetchone()[0] # todo

    def avg_temp(self):
        self.cursor.execute(
            "SELECT AVG(homerseklet) FROM idojaras"
        )
        return self.cursor.fetchone()[0]

    def max_humidity(self):
        self.cursor.execute(
            "SELECT MAX(paratartalom) FROM idojaras"
        )
        return self.cursor.fetchone()[0]


class SearchStrategy(ABC):
    @staticmethod
    @abstractmethod
    def search(cursor, param):
        pass


class SearchByTemp(SearchStrategy):
    @staticmethod
    def search(cursor, min_temp):
        cursor.execute(
            "SELECT * FROM idojaras"
            " WHERE homerseklet >= %s", (min_temp,)
        )
        return cursor.fetchall()


class SearchByHumidity(SearchStrategy):
    @staticmethod
    def search(cursor, max_humidity):
        cursor.execute(
            "SELECT * FROM idojaras"
            " WHERE paratartalom <= %s", (max_humidity,)
        )
        return cursor.fetchall()


if __name__ == "__main__":
    db = IdojarasDatabase()

    db.insert("Debrecen2", 12, 77, 66)

    print("Összes adat: ", db.fetch_all())

    print("Debrecen adatai: ", db.fetch_by_varos('Debrecen'))

    db.update_temperature(2, 18.0)

    print("Frissítve (ID=2):", db.fetch_by_varos('Debrecen'))

    print("15-18C között: ", db.fetch_by_temp_range(15.0, 18.0))

    print("Rekordok száma: ", db.count_records())

    print("Dátum szerint: ", db.fetch_ordered_by_date(True))

    print("Átlag hőm.: ", db.avg_temp())

    print("Max páratartalom: ", db.max_humidity())

    search_strategy = SearchByTemp()
    print("Stratégia: legalább 16C: ")
    search_strategy.search(db.cursor, 16.0)

    search_strategy = SearchByHumidity()
    print("Stratégia: max 66% páratartalom: ")
    search_strategy.search(db.cursor, 66)

    db.delete(1)
    print("Adatok törlés után: ", db.fetch_all())

    db.close()
