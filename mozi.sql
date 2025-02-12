create table if not exists mozik(
	id serial primary key,
	nev varchar(100) not null,
	varos varchar(100) not null
);

create table if not exists dolgozok(
	id serial primary key,
	nev varchar(100) not null,
	beosztas varchar(100),
	mozi_id int references mozik(id)
);

create table if not exists bufek(
	id serial primary key,
	nev varchar(100) not null,
	mozi_id int references  mozik(id)
);

create table if not exists vasarlok(
	id serial primary key,
	nev varchar(100) not null,
	vip boolean default false
);

create table if not exists filmek(
	id serial primary key,
	cim varchar(200) not null,
	kategoria varchar(50),
	besorolas varchar(25)
);



insert into mozik (nev, varos) values
('Cinema City', 'Budapest'),
('Pláza Mozi', 'Debrecen'),
('Belvárosi Mozi', 'Szeged'),
('FilmPalota', 'Pécs');



insert into dolgozok (nev, beosztas, mozi_id) values
('Kovács Péter', 'Pénztáros', 1),
('Nagy Anna', 'Ügyfélszolgálat', 2),
('Tóth Gábor', 'Vetítőgép-kezelő', 3),
('Szabó Katalin', 'Büfévezető', 4);



insert into bufek (nev, mozi_id) values
('Popcorn King', 1),
('Snack Bar', 2),
('Cinema Snacks', 3),
('Movie Bites', 4);



insert into vasarlok (nev, vip) values
('Horváth István', TRUE),
('Farkas Éva', FALSE),
('Varga Zoltán', TRUE),
('Kiss Júlia', FALSE);



insert into filmek (cim, kategoria, besorolas) values
('Eredet', 'Sci-Fi', '16+'),
('Titanic', 'Dráma', '12+'),
('Star Wars', 'Sci-Fi', '12+'),
('Joker', 'Thriller', '18+'),
('Eredet2', 'Sci-Fi', '16+');






select *
from filmek;



select *
from vasarlok
where vip = false;


select *
from filmek
where cim
ilike '%er'





select *
from filmek
where kategoria
similar to '%r'



select f.cim, m.nev
from filmek f
cross join mozik m;




select m.nev as mozi_nev, count(d.id) as dolgozok_szama
from mozik m
left join dolgozok d on m.id = d.mozi_id
group by m.nev
order by dolgozok_szama desc;






select b.nev as bufenev, count(d.id) as dolgozok_szama
from bufek b
left join dolgozok d on b.mozi_id = d.mozi_id
group by b.nev
having count(d.id) > 0;




select kategoria, count(*) as filmek_szama
from filmek
group by kategoria
order by filmek_szama desc;






select kategoria, count(*) as filmek_szama
from filmek
group by kategoria
having count(*) = (
	select max(film_count)
	from (select count(*) as film_count from filmek group by kategoria) as subquery
);





select m.nev as mozi_nev, count(d.id) as dolgozok_szama
from mozik m
left join dolgozok d on m.id = d.mozi_id
group by m.nev
order by dolgozok_szama desc
limit 1;







select * from filmek order by cim asc;



select count(*) as vip_vasarlok_szama
from vasarlok
where vip = true;






select avg(dolgozok_szama) as atlag_dolgozok
from (
	select count(*) as dolgozok_szama
	from dolgozok
	group by mozi_id
) as subquery;





select varos, count(*) as mozik_szama
from mozik
group by varos
order by mozik_szama desc;



select *
from filmek
order by id
desc limit 1;




select m.nev as mozi_nev, d.nev as dolgozo_nev, d.beosztas
from mozik m
join dolgozok d on m.id = d.mozi_id
order by m.nev, d.nev;






/*
ILIKE: 
Kis- és nagybetű független keresés.


SIMILAR TO:
Reguláris kifejezések alkalmazása a kereséshez.


CROSS JOIN:
Minden lehetséges kombináció létrehozása két tábla között, ahol nincs feltétel a kombinációra.
*/





