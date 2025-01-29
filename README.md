# SQL Tananyag


Prooktatáshoz használt SQL tananyag és a kapcsolódó Python használatot mutatja be.


## SQL Rész


### 1. 01.29 – Bevezetés az SQL-be
- SQL alapfogalmak: DDL, DQL, DML, DCL jelentése
- Adatbázis felépítése
- PostgreSQL és DBeaver telepítése
- `psycopg2` modul: `connect`, `cursor`, `execute`
- Tábla létrehozása
- Teszt az előző óra anyagából


### 2. 02.03 – SQL utasítások és lekérdezések
- **DDL:** `CREATE`, `DROP`
- **DML:** `INSERT INTO`
- **DQL:** `SELECT`
- Táblák összekapcsolása: `JOIN`
- Lekérdezések filterezése: `WHERE`
- Rendelési és csoportosítási műveletek: `ORDER BY`, `GROUP BY`
- Aggregáló függvények: `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`
- Szöveges függvények: `LEFT`, `RIGHT`, `CONCAT`, `LENGTH`, stb.


### 3. 02.05 – Haladó SQL utasítások
- Teszt az előző óra anyagából
- **DDL:** `CREATE`, `DROP`, `ALTER TABLE`
- **DML:** `INSERT INTO`, `DELETE FROM`, `UPDATE`
- **DQL:** `SELECT`
- Táblák összekapcsolása: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL JOIN`
- Összetett lekérdezések


### 4. 02.10 – Összetett SQL műveletek és Python kapcsolat
- Házi feladatok ellenőrzése
- **SQL:** `HAVING`, `OFFSET`, összetett lekérdezések, `DELETE` vs `TRUNCATE`
- **SQL használata Pythonban:**
  - `psycopg2` modul: `fetchall`, `executemany`


### 5. 02.12 – Biztonság és optimalizálás
- Vizsgafeladatok átbeszélése
- SQL injection elkerülése Pythonban
- Összetett lekérdezések gyakorlása
- Karakterlánc keresési operátorok: `ILIKE`, `SIMILAR TO`


### 6. 02.17 – Dátumkezelés és haladó SQL műveletek
- Dátum és idő kezelése SQL-ben
- Dátumfüggvények használata
- Boolean és `NULL` értékek kezelése
- Alapértelmezett értékek (`DEFAULT`) és frissítési műveletek (`UPDATE CASE`)


### 7. 02.19 – További dátumkezelési technikák és Python kapcsolódás
- Dátum és idő kezelése SQL-ben (2. rész)
- Autoincrement visszaállítása
- **SQL használata Pythonban:**
  - `psycopg2` modul: `fetchall`, `fetchone`, `fetchmany`, `execute`, `executemany`


## Használati útmutató
A tananyag gyakorlati példákat és Python kapcsolódási lehetőségeket is tartalmaz.
A `psycopg2` modul használatára külön figyelmet fordítunk.


Ha kérdésed van, nyugodtan nyiss egy issue-t! Jó tanulást! 🎓


## 📚 Könyvajánló
Ha szeretnél mélyebben elmerülni az SQL és adatbáziskezelés világában, az alábbi könyveket ajánljuk:


- **Adatbázisok** - Gajdos Sándor
- **SQL for Beginners** – John Russel
- **Learning SQL** – Alan Beaulieu
- **PostgreSQL: Up and Running** – Regina O. Obe, Leo S. Hsu
- **SQL Cookbook** – Anthony Molinaro
- **Effective SQL** – John Viescas, Douglas Steele, Ben G. Clothier


Ezek a könyvek segítenek az alapoktól a haladó szintig fejleszteni SQL tudásodat.
