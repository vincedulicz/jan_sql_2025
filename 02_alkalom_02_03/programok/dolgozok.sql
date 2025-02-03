
create table Alkalmazottak(
	AlkalmazottID int primary key,
	Nev varchar(100),
	BelepesDatum DATE
);


create table Termekek(
	TermekID int primary key,
	TermekNev varchar(100),
	Ar decimal(10, 2)
);


create table Rendelesek(
	RendelesID int primary key,
	AlkalmazottID int,
	TermekID int,
	Mennyiseg int,
	foreign key (AlkalmazottID) references Alkalmazottak(AlkalmazottID),
	foreign key (TermekID) references Termekek(TermekID)
);



insert into Alkalmazottak (AlkalmazottID, Nev, belepesdatum)
values (1, 'Kiss János', '2023-01-02');



insert into Termekek (TermekID, TermekNev, Ar)
values (101, 'asztal', 150.00);



insert into Rendelesek (RendelesID, AlkalmazottID, TermekID, Mennyiseg)
values (1001, 1, 101, 2);



insert into Termekek (TermekID, TermekNev, Ar)
values (102, 'szek', 50.00);




create table Ugyfelek(
	UgyfelID int primary key,
	UgyfelNev varchar(100),
	Varos varchar(100)
);



insert into ugyfelek (ugyfelid, ugyfelnev, varos)
values
(1, 'teszt elek1', 'deb'),
(2, 'teszt elek2', 'bp'),
(3, 'teszt elek3', 'deb'),
(4, 'teszt elek4', 'bp');



select ugyfelid from ugyfelek;


update alkalmazottak set belepesdatum = '2023-11-03'
where AlkalmazottID = 1;



select varos, count(ugyfelid) as ugyfelekSzama
from ugyfelek
group by varos
having count(ugyfelid) > 1;




select min(Ar) as legolcsobbTermek from termekek;



select max(ar) as legdragabbTermek from termekek;



select avg(ar) as atlagAr from termekek;







