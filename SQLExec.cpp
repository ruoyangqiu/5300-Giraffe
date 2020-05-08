/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Sonali D'souza , Pongpichit
 * @see "Seattle University, CPSC5300, Spring 2020"
 */
#include "SQLExec.h"
#include "schema_tables.h"
#include "ParseTreeToString.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    //Delete column_attributes pointer
    if (column_attributes != nullptr)
        delete column_attributes;
    
    //Delete column_names pointer
    if (column_names != nullptr)
        delete column_names;
    
    //Delete rows pointer
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present

    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
	}

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    //set column name
    column_name = col->name;

    // set column data type
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        default:
            throw SQLExecError("Invalid data type");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        default:
            return new QueryResult("Only CREATE TABLE is allowed");
	}
}

QueryResult *SQLExec::create_table(const CreateStatement * statement) {
	ColumnNames column_names;
    Identifier column_name;
	ColumnAttributes column_attributes;
	ColumnAttribute column_attribute;
	for (ColumnDefinition* column : *statement->columns) {
		column_definition(column, column_name, column_attribute);
		column_names.push_back(column_name);
		column_attributes.push_back(column_attribute);
	}

    ValueDict row;
    Identifier table_name = statement->tableName;
	row["table_name"] = table_name;

	SQLExec::tables->insert(&row);  
    Handles col_handles;
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    for (uint i = 0; i < column_names.size(); i++) {
        row["column_name"] = column_names[i];
        row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
        col_handles.push_back(columns.insert(&row));
    }

  
    DbRelation & table = SQLExec::tables->get_table(table_name);
    if (statement->ifNotExists)
        table.create_if_not_exists();
    else {
        table.create();
    }
        
	return new QueryResult("Created " + table_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
	case DropStatement::kTable:
		return drop_table(statement);
	default:
		return new QueryResult("Only DROP TABLE is allowed");
	}
}

QueryResult *SQLExec::drop_table(const DropStatement * statement) {
     return new QueryResult("not implemented"); // FIXME
}


QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        default:
            throw SQLExecError("Only SHOW table and columns type is allowed");
	}
}

QueryResult *SQLExec::show_tables() {
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

