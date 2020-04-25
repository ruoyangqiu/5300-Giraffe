/**
 * @file sql5300.cpp - main entry for the relation manager SQL shell
 * @author Yi Niu, Matthew Echert
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"

using namespace std;
using namespace hsql;

string columnDefinitionString(const ColumnDefinition *col) {
	return "COLUMN";
}

string executeSelect(const SelectStatement *stmt) {
	return "Select not implemented";
}

string executeInsert(const InsertStatement *stmt) {
	return "INSERT ...";
}

string executeCreate(const CreateStatement *stmt) {
	string qstr("CREATE TABLE ");
	qstr += string(stmt->tableName) + " (";
	
	bool comma = false;
	for (ColumnDefinition *col : *stmt->columns) {
		qstr+= comma ? ", " + columnDefinitionString(col) : columnDefinitionString(col);
		comma = true;
	}
	qstr += ")";
	return qstr;
}

string execute(const SQLStatement *stmt) {
	switch (stmt->type()) {
        case kStmtSelect:
            return executeSelect((const SelectStatement *) stmt);
        case kStmtInsert:
            return executeInsert((const InsertStatement *) stmt);
        case kStmtCreate:
            return executeCreate((const CreateStatement *) stmt);
        default:
            return "Not implemented";
	}
}

int main(int argc, char *argv[]) {
	if (argc != 2) {
		cout << "Usage: sql5300 dbenvpath" << endl;
		return EXIT_FAILURE;
	}

	char *envPath = argv[1];
	cout << "(sql5300: running with database environment at " << envPath << ")" << endl;

	//Open a DB
	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	try {
		env.open(envPath, DB_CREATE | DB_INIT_MPOOL, 0);
	} catch (DbException& err) {
		cerr << "(sql5300: " << err.what() << ")" << endl;
		return 1;
	}

	//Read user input in a loop
	while (true) {
		cout << "SQL> ";
		string query;
		getline(cin, query);
		if (query.length() == 0)
			continue;
		if (query == "quit")
			break;
		SQLParserResult* result = SQLParser::parseSQLString(query);
		if (!result->isValid()) {
			cout << "Invalid SQL: " << query << endl;
			delete result;
			continue;		
		}

		for (int i = 0; i < result->size(); ++i) {
			cout << execute(result->getStatement(i)) << endl;
		}
	}

	env.close(0U);
	return 0;
}	

