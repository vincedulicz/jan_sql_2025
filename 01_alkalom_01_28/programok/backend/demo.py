import psycopg2
from psycopg2 import sql
from threading import Lock
from abc import ABC, abstractmethod

""" TODO: pkg property observer for objects state """

class DatabaseConnection:
    """ Singleton DBconnect """
    _instance = None
    _lock = Lock()

    def __new__(cls, dbname, user, password, host="localhost", port="5432"):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DatabaseConnection, cls).__new__(cls)
                cls._instance._init_connection(dbname, user, password, host, port)
        return cls._instance

    def _init_connection(self, dbname, user, password, host, port):
        try:
            self.conn = psycopg2.connect(
                dbname=dbname,
                user=user,
                password=password,
                host=host,
                port=port)
            self.cursor = self.conn.cursor()
            print("Sikeres csatlakozás")
        except psycopg2.Error as e:
            print(f'Hiba: {e}')
            self.conn = None

    def __str__(self):
        return f'{self.conn}'

    def execute_query(self, query, params=None):
        try:
            self.cursor.execute(sql.SQL(query), params)
            self.conn.commit()
            print("SQL végrehajtva", query)
        except psycopg2.Error as e:
            print(f'Hiba: {e}')
            self.conn.rollback()

    def fetch_query(self, query, params=None):
        try:
            self.cursor.execute(sql.SQL(query), params)
            return self.cursor.fetchall()
        except psycopg2.Error as e:
            print(f'Hiba: {e}')
            return []

    def close_connection(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Kapcsolat lezárva")


class SQLStrategy(ABC):
    @abstractmethod
    def execute(self, db: DatabaseConnection):
        pass


class CreateUsersTable(SQLStrategy):
    def execute(self, db: DatabaseConnection):
        query = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            age INTEGER CHECK (age >= 0)
        );
        """
        db.execute_query(query)


class CreateEmployeesTable(SQLStrategy):
    def execute(self, db: DatabaseConnection):
        query = """
                CREATE TABLE IF NOT EXISTS employees (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                    position VARCHAR(100) NOT NULL,
                    salary INTEGER CHECK (salary >= 0)
                );
                """
        db.execute_query(query)


class InsertUser(SQLStrategy):
    def __init__(self, name, email, age):
        self.name = name
        self.email = email
        self.age = age

    def execute(self, db: DatabaseConnection):
        query = "INSERT INTO users (name, email, age) VALUES (%s, %s, %s) RETURNING id;"
        result = db.fetch_query(query, (self.name, self.email, self.age))
        return result[0][0] if result else None


class InsertEmployee(SQLStrategy):
    def __init__(self, user_id, position, salary):
        self.user_id = user_id
        self.position = position
        self.salary = salary

    def execute(self, db: DatabaseConnection):
        query = "INSERT INTO employees (user_id, position, salary) VALUES (%s, %s, %s);"
        db.execute_query(query, (self.user_id, self.position, self.salary))


class FetchUsers(SQLStrategy):
    def execute(self, db: DatabaseConnection):
        query = "SELECT * FROM users;"
        return db.fetch_query(query)


class FetchEmployees(SQLStrategy):
    def execute(self, db: DatabaseConnection):
        query = """
        SELECT employees.id, 
        users.name, 
        users.email, 
        employees.position, 
        employees.salary FROM employees 
        JOIN users ON employees.user_id = users.id; """
        return db.fetch_query(query)


class DeleteUser(SQLStrategy):
    def __init__(self, user_id):
        self.user_id = user_id

    def execute(self, db: DatabaseConnection):
        query = "DELETE FROM users WHERE id = %s"
        db.execute_query(query, (self.user_id,))


class Observer(ABC):
    @abstractmethod
    def update(self, event: str):
        pass


class TableCreationObserver(Observer):
    def update(self, event: str):
        print(f'\nLOG: {event}\n')


class DatabaseNotifier:
    def __init__(self):
        self.observers = []

    def add_observer(self, observer: Observer):
        self.observers.append(observer)

    def notify(self, event: str):
        for observer in self.observers:
            observer.update(event)


if __name__ == "__main__":
    notifier = DatabaseNotifier()
    observer = TableCreationObserver()
    notifier.add_observer(observer)

    db = DatabaseConnection(dbname="postgres", user="postgres", password="qwe123")
    notifier.notify(f'### DB: {db} connected ###')

    CreateUsersTable().execute(db)
    CreateEmployeesTable().execute(db)

    notifier.notify(f'### Tables created ###')

    user_te = InsertUser("Teszt Eleke", "te@example.com", 30).execute(db)
    user_et = InsertUser("Elek Teszer", "et23@google.admin.hu", 34).execute(db)

    if user_et and user_te:
        InsertEmployee(user_te, "junior software engineer", 3999).execute(db)
        InsertEmployee(user_et, "senior software engineer", 9999).execute(db)
        notifier.notify("### Insert data to db ###")

    print(f"\nFelhasználók: {FetchUsers().execute(db)}\n")
    print(f'\nDolgozók: {FetchEmployees().execute(db)}\n')

    notifier.notify("### Fetch users & employees ###")

    db.close_connection()

    notifier.notify("### Close connection ###")
