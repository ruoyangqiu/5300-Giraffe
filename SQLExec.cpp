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
Indices* SQLExec::indices = nullptr;

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
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
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

/**
 * QueryResult destructor
 * */
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

/**
 * function for executing SQL query statement
 * @param	SQLStatement	statement
 * @return	QueryResult
 * */
QueryResult *SQLExec::execute(const SQLStatement *statement) {
    // initialize _tables table, if not yet present

    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
	}

    if (SQLExec::indices == nullptr) {
		SQLExec::indices = new Indices();
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

/**
 * function for setting Identifier and ColumnAttribute from ColumnDefinition
 * @param	ColumnDefinition	col
 * @param	Identifier		column_name (returned by reference)
 * @param	ColumnAttribute		column_attribute (returned by reference)
 * */
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

/**
 * function for executing SQL query "CREATE ..." statement
 * @param	CreateStatement	statement
 * @return	QueryResult
 * */
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

/**
 * function for handling "CREATE TABLE <table_name> ( <column_definitions> )" execution
 * @param	CreateStatement	statement
 * @resutn	QueryResult
 * */
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
	Handle table_handle = SQLExec::tables->insert(&row); 
	try {
		Handles col_handles;
		DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
		try {
			for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                col_handles.push_back(columns.insert(&row));
            }

			DbRelation & table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists) {
                table.create_if_not_exists();
            } else {
                table.create();
            }
		}
		catch (exception & e) {
			try {
				for (auto const& handle : col_handles)
					columns.del(handle);
			}
			catch (...) {}
			throw;
		}

	}
	catch (exception & e) {
		try {
			SQLExec::tables->del(table_handle);
		}
		catch (...) {}
		throw;
	}
	return new QueryResult("Created " + table_name);
}


/**
 * function for handling "CREATE INDEX " execution
 * @param	CreateStatement	statement
 * @result	QueryResult
 * */
QueryResult *SQLExec::create_index(const CreateStatement *statement) {
	Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;
    Identifier index_type = statement->indexType;

     // Get table
    DbRelation& table = SQLExec::tables->get_table(table_name); 
    
    // Check if columns exist in table
    const ColumnNames& table_columns = table.get_column_names();
    for (auto const& column_name: *statement->indexColumns) {
        if (std::find(table_columns.begin(), table_columns.end(), column_name) == table_columns.end()) {
             throw SQLExecError(std::string("'") + column_name + "' not found in " + table_name);
        }
    }
        
    // In index, insert a row for every column 
    ValueDict row;
    Handles index_handles;
    int seq_in_index = 0;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(index_type);
    if(std::string(index_type) ==  "BTREE")
        row["is_unique"] = Value(true);
    else
        row["is_unique"] = Value(false);


    try {
        for (auto const &column_name: *statement->indexColumns) {
            row["seq_in_index"] = Value(++seq_in_index);
            row["column_name"] = Value(column_name);
            index_handles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();

    } catch(exception& e) {
        for(auto const &handle : index_handles) {
            SQLExec::indices->del(handle);
        }
        throw;
    }
    return new QueryResult("Created index " + index_name);
}

/**
 * function for executing SQL query "DROP ..." statement
 * @param	DropStatement	statement
 * @return	QueryResult
 * */
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
	case DropStatement::kTable:
		return drop_table(statement);
    case DropStatement::kIndex:
            return drop_index(statement);
	default:
		return new QueryResult("Only DROP TABLE and DROP INDEX is allowed");
	}
}

/**
 * function for handling "DROP INDEX" execution
 * @param	DropStatement	statement
 * @return	QueryResult
 * */
QueryResult *SQLExec::drop_index(const DropStatement *statement){
    if (statement->type != DropStatement::kIndex)
		throw SQLExecError("Invalid drop type");

	Identifier table_name = statement->name;
	Identifier index_ID = statement->indexName;

    DbIndex & index = SQLExec::indices->get_index(table_name, index_ID);
    index.drop();

	ValueDict where;
	where["table_name"] = Value(table_name);
	where["index_name"] = Value(index_ID);

    Handles *handles = SQLExec::indices->select(&where);

	for(Handle handle : *handles) {
        SQLExec::indices->del(handle);
    }
	
	delete handles;
	return new QueryResult("Dropped index " + index_ID); 
}

/**
 * function for handling "DROP TABLE <table_name>" execution
 * @param	DropStatement	statement
 * @return	QueryResult
 * */
QueryResult *SQLExec::drop_table(const DropStatement * statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME) {
         throw SQLExecError("Cannot drop table");
    }
    
    DbRelation& table = SQLExec::tables->get_table(table_name);
    ValueDict where;
    where["table_name"] = Value(table_name);

    // Remove indices
    for (auto const& index_name: SQLExec::indices->get_index_names(table_name)) {
        DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
        index.drop();  
    }

    // Remove all rows from indices for each index
    Handles* handles = SQLExec::indices->select(&where);
    for (auto const& handle: *handles)
        SQLExec::indices->del(handle);  
    delete handles;

    // Remove from columns schema
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    handles = columns.select(&where);
    for (auto const& handle: *handles)
        columns.del(handle);
    delete handles;

    //Remove table
    table.drop();

    //Remove from tables schema
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

    return new QueryResult(string("Dropped ") + table_name);
}

/**
 * function for executing SQL query  "SHOW ..." statement
 * @param	ShowStatement	statement
 * @return	QueryResult
 * */
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
	}
}

/**
 * function for handling "SHOW INDEX" execution
 * @return	QueryResult
 * */
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames* column_names = new ColumnNames;
    ColumnAttributes* column_attributes = new ColumnAttributes;

    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
        
    column_names->push_back("index_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("column_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("seq_in_index");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back("index_type");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = Value(string(statement->tableName));
    Handles* handles = SQLExec::indices->select(&where);
   
    ValueDicts* rows = new ValueDicts;

    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;

    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(rows->size()) + " rows");
}

/**
 * function for handling "SHOW TABLES" execution
 * @return	QueryResult
 * */
QueryResult *SQLExec::show_tables() {
    ColumnNames* col_names = new ColumnNames;
    ColumnAttributes* col_attributes = new ColumnAttributes;
    Handles* handles = SQLExec::tables->select();
	ValueDicts* rows = new ValueDicts;

	col_names->push_back("table_name");
	col_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

	for (Handle handle : *handles) {
		ValueDict* row = SQLExec::tables->project(handle, col_names);
		Identifier table_name = row->at("table_name").s;
		if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME)
			rows->push_back(row);
	}
	delete handles;
	return new QueryResult(col_names, col_attributes, rows,
		"Returned " + to_string(rows->size()) + " rows");
}

/**
 * function for handling "SHOW COLUMNS <table_name>" execution
 * @param       ShowStatement	statement
 * @return      QueryResult
 * */
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {    
	ColumnNames* col_names = new ColumnNames;
    ColumnAttributes* col_attributes = new ColumnAttributes;
	col_names->push_back("table_name");
	col_names->push_back("column_name");
	col_names->push_back("data_type");
    col_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

	ValueDict where;
	where["table_name"] = Value(statement->tableName);
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
	Handles* handles = columns.select(&where);
	
	ValueDicts* rows = new ValueDicts;
	for (Handle handle : *handles) {
		ValueDict* row = columns.project(handle, col_names);
		rows->push_back(row);
	}
	delete handles;

	return new QueryResult(col_names, col_attributes, rows,
		"Returned " + to_string(rows->size()) + " rows");
}