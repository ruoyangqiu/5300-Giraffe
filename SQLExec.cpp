/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2020"
 */
#include "SQLExec.h"
#include "EvalPlan.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;
Indices *SQLExec::indices = nullptr;

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

QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
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
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

/**
 *  Get where clause from sql parser
 *  @param parse_where  The expression represent for where clause
 *  @return             where clause
 */
ValueDict* get_where_conjuction(const Expr *parse_where) {
    ValueDict* where_list = new ValueDict();
    if(parse_where->type == kExprOperator) {
        if(parse_where->opType == Expr::AND) {
            ValueDict* where1 = get_where_conjuction(parse_where->expr);
            ValueDict* where2 = get_where_conjuction(parse_where->expr2);
            where_list->insert(where1->begin(), where1->end());
            where_list->insert(where2->begin(), where2->end());
            delete where1;
            delete where2;
        } else if(parse_where->opType == Expr::SIMPLE_OP) {
            Identifier col_name = parse_where->expr->name;
            if(parse_where->opChar != '=') {
                throw SQLExecError("Only equality predicates currently supported");
            }
            if(parse_where->expr2->type == kExprLiteralString) {
                (*where_list)[col_name] = Value(parse_where->expr2->name);
            } else if(parse_where->expr2->type == kExprLiteralInt) {
                (*where_list)[col_name] = Value(parse_where->expr2->ival);
            } else {
                throw SQLExecError("Only support INT and TEXT data type");
            }
        } else {
            throw SQLExecError("Only support AND conjunctioins");
        }
    } else {
        throw SQLExecError("Only support operator where clause");
    }
    return where_list;
}

/**
 *  Execute select SQL statement
 *  @param statement    The SQL select statement will be executed
 *  @return             the query result (freed by caller)
 */
QueryResult *SQLExec::select(const SelectStatement *statement) {
    Identifier table_name = statement->fromTable->name;

    DbRelation &table = SQLExec::tables->get_table(table_name);

    ColumnNames* query_names = new ColumnNames();

    vector<Expr*>*  select_list = statement->selectList;

    EvalPlan* plan = new EvalPlan(table);

    if(statement->whereClause != nullptr) {
        plan = new EvalPlan(get_where_conjuction(statement->whereClause), plan);
        cout << "where" << endl;
    }

    if(select_list->at(0)->type == kExprStar) {
        *query_names = table.get_column_names();
        plan = new EvalPlan(EvalPlan::ProjectAll, plan);
    } else {
        for(auto const& expr : *statement->selectList) {
            query_names->push_back(string(expr->name));
            cout << string(expr->name) << endl;
        }
        plan = new EvalPlan(query_names, plan);
    }

    ColumnAttributes *column_attributes = table.get_column_attributes(*query_names);

    EvalPlan *best_plan = plan->optimize();
    
    ValueDicts *rows = best_plan->evaluate();

    delete best_plan;

    return new QueryResult(query_names, column_attributes, rows,
            "successfully returned " + to_string(rows->size()) + " rows");
}


 /**
 * Insert a row into a table indicated in a given statement
 * @param statement the given statement indicating row and table
 * @return the result of query execution
 */ 
QueryResult *SQLExec::insert(const InsertStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames columns;
    vector<Expr*> values;

    DbRelation &table = SQLExec::tables->get_table(table_name);
    IndexNames index_names = SQLExec::indices->get_index_names(table_name);

    for (auto const &col : *statement->columns)
        columns.push_back(col);
    
    for (auto const &val : *statement->values)
        values.push_back(val);

    // prepare row to insert
    ValueDict row;
    for (uint i = 0; i < columns.size(); i++) {
        Identifier col = columns[i];
        Expr* val = values[i];
        switch (val->type) {
            case kExprLiteralInt:
                row[col] = Value(val->ival);
                break;
            case kExprLiteralString:
                row[col] = Value(val->name);
                break;
            default:
                return new QueryResult("Data type not implemented");
        }
    }
    // insert row into table
    Handle table_handle = table.insert(&row); 

    // update indices
    string indices = "";
    if (index_names.size() != 0) {
        indices = " and index ";

        for (Identifier index_name : index_names) {
            DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
            index.insert(table_handle);
            indices += index_name;
            indices += ", ";
        }
        indices.resize(indices.size() - 2);
    }

    return new QueryResult("Successfully inserted 1 row into table " + table_name + indices);
}

/**
 * Delete rows from a table based on a given statement
 * @param statement the given statement with deletion information
 * @return the result of query execution
 */ 
QueryResult *SQLExec::del(const DeleteStatement *statement) {
    Identifier table_name = statement->tableName;

    DbRelation &table = SQLExec::tables->get_table(table_name);
    IndexNames index_names = SQLExec::indices->get_index_names(table_name);

    Expr* expr = statement->expr;
    
    // make evaluation plan
    EvalPlan* plan = new EvalPlan(table);

    if (expr != NULL) {
        ValueDict* where_list = get_where_conjuction(expr);
        plan = new EvalPlan(where_list, plan);
    }

    plan = plan->optimize();

    // get handles
    EvalPipeline pipeline = plan->pipeline();  // pair<DbRelation *, Handles *>
    Handles *handles = pipeline.second;

    // remove from indices
    string indices = "";
    if (index_names.size() != 0) {
        indices = " and index ";

        for (Identifier index_name : index_names) {
            DbIndex& index = SQLExec::indices->get_index(table_name, index_name);

            for (auto const &handle: *handles)
                index.del(handle);

            indices += index_name;
            indices += ", ";
        }
    }

    // remove from table
    for (auto const &handle: *handles)
        table.del(handle);

    return new QueryResult("Successfully deleted rows from table " + table_name + indices);
}



void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

// CREATE ...
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

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation &table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (...) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception &e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;

    // get underlying relation
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // check that given columns exist in table
    const ColumnNames &table_columns = table.get_column_names();
    for (auto const &col_name: *statement->indexColumns)
        if (find(table_columns.begin(), table_columns.end(), col_name) == table_columns.end())
            throw SQLExecError(string("Column '") + col_name + "' does not exist in " + table_name);

    // insert a row for every column in index into _indices
    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(string(statement->indexType) == "BTREE"); // assume HASH is non-unique --
    int seq = 0;
    Handles i_handles;
    try {
        for (auto const &col_name: *statement->indexColumns) {
            row["seq_in_index"] = Value(++seq);
            row["column_name"] = Value(col_name);
            i_handles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();

    } catch (...) {
        // attempt to remove from _indices
        try {  // if any exception happens in the reversal below, we still want to re-throw the original ex
            for (auto const &handle: i_handles)
                SQLExec::indices->del(handle);
        } catch (...) {}
        throw;  // re-throw the original exception (which should give the client some clue as to why it did
    }
    return new QueryResult("created index " + index_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    // remove any indices
    for (auto const &index_name: SQLExec::indices->get_index_names(table_name)) {
        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.drop();  // drop the index
    }
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);  // remove all rows from _indices for each index on this table
    delete handles;

    // remove from _columns schema
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    handles = columns.select(&where);
    for (auto const &handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    handles = SQLExec::tables->select(&where);
    SQLExec::tables->del(*handles->begin()); // expect only one row from select
    delete handles;

    return new QueryResult(string("dropped ") + table_name);
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    // drop index
    DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();

    // remove rows from _indices for this index
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
    Handles *handles = SQLExec::indices->select(&where);
    for (auto const &handle: *handles)
        SQLExec::indices->del(handle);
    delete handles;

    return new QueryResult("dropped index " + index_name);
}

// SHOW ...
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

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames *column_names = new ColumnNames;
    ColumnAttributes *column_attributes = new ColumnAttributes;
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
    Handles *handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_tables() {
    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles *handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
        else
            delete row;
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames *column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes *column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts *rows = new ValueDicts;
    for (auto const &handle: *handles) {
        ValueDict *row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + to_string(n) + " rows");
}
