# 5300-Fossa
DB Relation Manager project for CPSC5300/4300 at Seattle U, Spring 2020

## Tags
- <code>Milestone1</code> is playing around with the AST returned by the HyLine parser and general setup of the command loop. Implemented SQL interpreter that supports CREATE and SELECT statements
- <code>Milestone2</code> Rudimentry heap storage engine. Implemented the basic functions needed for HeapTable, but only for two data types: integer and text.
- <code>Milestone3</code> Schema Storage - rudimentary implementation of CREATE TABLE, DROP TABLE, SHOW TABLE, SHOW COLUMNS in 
<code>SQLExec.cpp</code> .

## Unit Tests
There are some tests for SlottedPage and HeapTable. They can be invoked from the <clode>SQL</code> prompt:
```sql
SQL> test
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
