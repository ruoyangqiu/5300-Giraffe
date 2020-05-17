# 5300-Giraffe
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2020

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop. Implemented SQL interpreter that supports CREATE and SELECT statements
- <code>Milestone2</code> Rudimentry heap storage engine. Implemented the basic functions needed for HeapTable, but only for two data types: integer and text.
- <code>Milestone3</code> Schema Storage - rudimentary implementation of CREATE TABLE, DROP TABLE, SHOW TABLE, SHOW COLUMNS in <code>SQLExec.cpp</code> .
- <code>Milestone4</code> Indexing Setup - rudimentary implementation of CREATE INDEX, DROP INDEX, SHOW INDEX in <code>SQLExec.cpp</code>

## Unit Tests
<code>Milestone2</code> - SlottedPage and HeapTable tests. They can be invoked from the <code>SQL</code> prompt:
```sql
SQL> test
```
<code>Milestone3</code> - Schema Storage tests. They can be invoked from the <code>SQL</code> prompt:
```sql
SQL> show tables
SQL> show columns from _tables
SQL> show columns from _columns
SQL> create table foo (id int, data text, x integer, y integer, z integer)
SQL> create table foo (goober int)
SQL> create table goo (x int, x text)
SQL> show tables
SQL> show columns from foo
SQL> drop table foo
SQL> show tables
SQL> show columns from foo
```
<code>Milestone4</code> - Indexing Setup tests. They can be invoked from the <code>SQL</code> prompt:
```sql
SQL> show tables
SQL> show columns from goober
SQL> create index fx on goober (x,y)
SQL> show index from goober
SQL> drop index fx from goober
SQL> show index from goober
SQL> create index fx on goober (x)
SQL> show index from goober
SQL> create index fx on goober (y,z)
SQL> show index from goober
SQL> create index fyz on goober (y,z)
SQL> show index from goober
SQL> drop index fx from goober
SQL> show index from goober
SQL> drop index fyz from goober
SQL> show index from goober
```
Be aware that failed tests may leave garbage Berkeley DB files lingering in your data directory. 
If you don't care about any data in there, you are advised to just delete them all after a failed test.
```sh
$ rm -f data/*
``` 

**How to run this project?**

1) Download and open this project directory

2) Run make

3) Run ./sql5300 ../data (Note: Here data is the subdirectory to hold our Berkeley DB database files. Make sure you create this directory before running this command)

4) To use sql interpreter, start typing sql commands like the examples shown above.

5) To test heap storage, type test and hit enter.



## Valgrind (Linux)
To run valgrind (files must be compiled with -ggdb):
```sh
$ valgrind --leak-check=full --suppressions=valgrind.supp ./sql5300 data
```
Note that we've added suppression for the known issues with the Berkeley DB library <em>vis-à-vis</em> valgrind.

## Video
- <code>Milestone3&4</code> - https://seattleu.instructuremedia.com/embed/98bbd00d-bfab-4d3c-b70e-03d2bf04ca72
