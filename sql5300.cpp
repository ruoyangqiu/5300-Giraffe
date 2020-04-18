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
	//Read user input in a loop
	while (true) {
		cout << "SQL> ";
		string q;
		getline(cin, q);
		if (q.length() == 0)
			continue;
		if (q == "quit")
			break;

		cout << "Your query was: " << q << endl;
	}	
	
}	

