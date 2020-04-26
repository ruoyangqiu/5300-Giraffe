# 5300-Giraffe
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2020

Sprint Verano: 
- Milestone 1
SQL interpreter that takes statement from user command and returns a string of SQL statement.

How to run:
./sql5300 ../data
Example:
(sql5300: running with database environment at ../data)
SQL> create table foo (a text, b int, c double)
CREATE TABLE foo (a TEXT, b INT, c DOUBLE)
SQL> select * from foo left join goober on foo.x=goober.x
SELECT * FROM foo LEFT JOIN goober ON foo.x = goober.x
SQL> select * from foo as f left join goober on f.x = goober.x
SELECT * FROM foo AS f LEFT JOIN goober ON f.x = goober.x
SQL> select * from foo as f left join goober as g on f.x = g.x
SELECT * FROM foo AS f LEFT JOIN goober AS g ON f.x = g.x
SQL> select a,b,g.c from foo as f, goo as g
SELECT a, b, g.c FROM goo AS g, foo AS f
SQL> select a,b,c from foo where foo.b > foo.c + 6
SELECT a, b, c FROM foo WHERE foo.b > foo.c + 6
SQL> select f.a,g.b,h.c from foo as f join goober as g on f.id = g.id where f.z >1
SELECT f.a, g.b, h.c FROM foo AS f JOIN goober AS g ON f.id = g.id WHERE f.z > 1
SQL> foo bar blaz
Invalid SQL: foo bar blaz
SQL> quit

- Milestone 2
Rudimentary storage engine with layers of SlottedPage, HeapFile, and HeapTable.
All methods are supported except HeapTable "update" and "delete".

Command to test:
./sql5300 ../data
SQL> test
test_slotted_page: ok
test_heap_storage: create ok
drop ok
create_if_not_exsts ok
try insert
insert ok
select ok 1
project ok
ok
SQL> quit

General Steps:
1. Git clone or download this repo
2. Compile the code by runing "make"
3. Run "./sql5300 ../data" (Should make directory of "data" outside the repo first)
4. Use example commands like above
