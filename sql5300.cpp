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
	}

	//Read user input in a loop
	while (true) {
		cout << "SQL> ";
		string q;
		getline(cin, q);
		if (q.length() == 0)
			continue;
		if (q == "quit")
			break;

		//TODO: "parse the query"
		cout << "Your query was: " << q << endl;
	}

	return 0;
}	

