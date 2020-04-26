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
#include "heap_storage.h"

using namespace std;
using namespace hsql;

//defined below
string joinString(const TableRef *ref);
string operatorString(const Expr *expr);

/**
* Format a column definition as a string
* @param col ColumnDefinition to be parsed
* @return String containing colName TYPE
*/
string columnDefinitionString(const ColumnDefinition *col) {
	string colstr(col->name);
	switch (col->type) {
		case ColumnDefinition::TEXT:
			colstr += " TEXT";
			break;
		case ColumnDefinition::INT:
			colstr += " INT";
			break;
		case ColumnDefinition::DOUBLE:
			colstr += " DOUBLE";
			break;
		default:
			colstr += " ...";
			break;
	}
	return colstr;
}

/**
* Format a valid SQL expression string from an Expr
* @param expr Expr object to be parsed
* @return formatted SQL expression
*/
string expressionString(const Expr *expr) {
	if (expr == NULL) {
		return "null";
	}
	string expression;

	switch(expr->type) {
		case kExprStar:
			expression = "*";
			break;
		case kExprColumnRef:
			if (expr->table != NULL)
				expression += string(expr->table) + ".";
		case kExprLiteralString:
			expression += expr->name;
			break;
		case kExprLiteralInt:
			expression += to_string(expr->ival);
			break;
		case kExprLiteralFloat:
			expression += to_string(expr->fval);
			break;
		case kExprOperator:
			expression += operatorString(expr);
			break;
		default:
			expression += "<?>";
			break;
	}

	if (expr->alias != NULL) {
		expression += string(" AS ") + expr->alias;
	}

	return expression;
}

/**
* Format a valid SQL table from a table reference
* @param ref TableRef to be parsed
* @return formatted SQL expression
*/
string tableString(const TableRef *ref) {
	string tables;

	switch (ref->type) {
		case kTableName:
			tables += ref->name;
			if (ref->alias != NULL) {
				tables += string(" AS ") + ref->alias;
			}
			break;
		case kTableJoin:
			tables += joinString(ref);
			break;
		case kTableCrossProduct:
			{
				bool comma = false;
				for (TableRef *table : *ref->list) {
					tables += comma ? ", " + tableString(table) : tableString(table);
					comma = true;
				}
			}
			break;
		default:
			tables += "<...>";
			break;
	}

	return tables;
}

/**
* Format a valid SQL join string from a kTableJoin reference
* @param ref TableRef to be parsed
* @return formatted SQL join expression
*/
string joinString(const TableRef *ref) {
	string join;
	
	//left of join
	join += tableString(ref->join->left);

	//type of join
	switch (ref->join->type) {
		case kJoinCross:
		case kJoinInner:
			join += " JOIN ";
			break;
		case kJoinOuter:
		case kJoinLeftOuter:
		case kJoinLeft:
			join += " LEFT JOIN ";
			break;
		case kJoinRightOuter:
		case kJoinRight:
			join += " RIGHT JOIN ";
			break;
		default:
			join += " <?> ";
			break;
	}

	//right of join
	join += tableString(ref->join->right);

	//join condition
	join += string(" ON ") + expressionString(ref->join->condition);

	return join;
}

/**
* Return the formatted SQL string for an operator statement
* *****FIXME: Currently only handles binary operations
* @param stmt SelectStatement to be parsed
* @return String of formatted SQL
*/
string operatorString(const Expr *expr) {
	if (expr == NULL) {
		return "null";
	}

	string op;
	op += expressionString(expr->expr) + " ";
	op += expr->opChar;
	op += " " + expressionString(expr->expr2);

	return op;
}

/**
* Return the formatted SQL string for a SELECT statement
* @param stmt SelectStatement to be parsed
* @return String of formatted SQL
*/
string executeSelect(const SelectStatement *stmt) {
	string qstr("SELECT ");
	bool comma = false;
	for (Expr *expr : *stmt->selectList) {
		qstr += comma ? ", " + expressionString(expr) : expressionString(expr);
		comma = true;
	}

	qstr += " FROM " + tableString(stmt->fromTable);
	
	if (stmt->whereClause != NULL) {
		qstr += " WHERE " + expressionString(stmt->whereClause);
	}

	return qstr;
}

/**
* Return the formatted SQL string for an INSERT statement
* @param stmt InsertStatement to be parsed
* @return String of formatted SQL
*/
string executeInsert(const InsertStatement *stmt) {
	return "INSERT ...";
}

/**
* Return the formatted SQL string for a CREATE statement
* @param stmt CreateStatement to be parsed
* @return String of formatted SQL
*/
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

/**
* Determine type of SQL statement and call the appropriate execute function
* @param stmt SQLStatement to be parsed
*/
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
		if (query == "test")
			cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
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

