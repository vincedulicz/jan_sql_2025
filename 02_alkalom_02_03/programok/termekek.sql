
create table osztalyok(
	id INT primary key,
	osztaly_nev varchar(100)
);


insert into osztalyok (id, osztaly_nev) values
(1, 'fejlesztés'),
(2, 'hr'),
(3, 'projektmenedzsment'),
(4, 'marketing'),
(5, 'informatika');



create table dolgozok(
	id int primary key,
	nev varchar(100),
	fizetes decimal(10, 2),
	beosztas varchar(50),
	osztaly_id INT,
	foreign key (osztaly_id) references osztalyok(id)
);




insert into dolgozok (id, nev, fizetes, beosztas, osztaly_id) values
(1, 'Kovács Péter', 550000, 'Fejlesztő', 1),
(2, 'Nagy Andrea', 480000, 'HR vezető', 2),
(3, 'Szabó Márton', 620000, 'Projektmenedzser', 3),
(4, 'Tóth László', 390000, 'Tesztelő', 1),
(5, 'Varga Katalin', 510000, 'Fejlesztő', 1),
(6, 'Kiss Gábor', 700000, 'Rendszergazda', 5),
(7, 'Horváth Zoltán', 450000, 'Üzleti elemző', 3),
(8, 'Farkas Dóra', 520000, 'Marketing vezető', 4),
(9, 'Papp Csaba', 610000, 'Fejlesztő', 1),
(10, 'Takács Eszter', 430000, 'Adminisztrátor', 2),
(11, 'Molnár Attila', 580000, 'Adatbázis admin', 5),
(12, 'Balogh Júlia', 490000, 'Ügyfélszolgálat', 2);




select dolgozok.nev, dolgozok.beosztas, osztalyok.osztaly_nev
from dolgozok
inner join osztalyok on dolgozok.osztaly_id = osztalyok.id;



select  from dolgozok;


select nev, fizetes from dolgozok where fizetes  400000;




select  from dolgozok order by fizetes desc;




select beosztas, count() as elofurdulas from dolgozok group by beosztas order by beosztas desc;




select avg(fizetes) as atlagfizu from dolgozok;




select left(nev, 1) from dolgozok;





select right(nev, 4) from dolgozok;

postgres_teszt_db



select concat(nev, ' - ', beosztas) from dolgozok;





select length(nev) from dolgozok;







