#include "heap_storage.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include <cstring>
#include "SQLParser.h"
#include "sqlhelper.h"

using namespace std;
#define DB_BLOCK_SZ 256
DbEnv *_DB_ENV;
typedef u_int16_t u16;

/*******************
 SlottedPage Class
 ******************/

// Constructor for SlottedPage
SlottedPage::SlottedPage(Dbt &block,
                         BlockID block_id,
                         bool is_new) : DbBlock(block, block_id, is_new)
{
    if (is_new)
    {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    }
    else
    {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt *data)
{
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16)data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

// Get a record from the block. Return None if it has been deleted.
Dbt *SlottedPage::get(RecordID record_id)
{
    u16 size, loc;
    get_header(size, loc, record_id);

    if (loc == 0)
    {
        return nullptr;
    }
    return new Dbt(this->address(loc), size);
}

// Replace the record with the given data. Raises DbBlockNoRoomError if it won't fit.
void SlottedPage::put(RecordID record_id, const Dbt &data) //throw(DbBlockNoRoomError)
{
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16)data.get_size();

    if (new_size > size)
    {
        u16 extra = new_size - size;

        // if (!has_room(extra))
        //throw DbBlockNoRoomError("not enough room for enlarged record (SlottedPage::put)");

        slide(loc, loc - extra);
        memcpy(this->address(loc - extra), data.get_data(), new_size);
    }
    else
    {
        memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }
    this->get_header(size, loc, record_id);
    this->put_header(record_id, new_size, loc);
}

// Mark the given id as deleted by changing its size to zero and its location to 0.
// Compact the rest of the data in the block. But keep the record ids the same for everyone.
void SlottedPage::del(RecordID record_id)
{
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc + size);
}

// Sequence of all non-deleted record IDs.
RecordIDs *SlottedPage::ids(void)
{
    u16 size, loc;
    RecordIDs *records = new RecordIDs;
    for (u16 i = 1; i <= num_records; i++)
    {
        get_header(size, loc, i);
        if (loc != 0)
            records->push_back(i);
    }
    return records;
}

// Get the size and offset for given id. For id of zero, it is the block header.
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id)
{
    size = get_n((u16)4 * id);
    loc = get_n((u16)(4 * id + 2));
}

// Calculate if we have room to store a record with given size. The size should include the 4 bytes
// for the header, too, if this is an add.
bool SlottedPage::has_room(u_int16_t size)
{
    u16 available = this->end_free - (u16)(4 * (this->num_records + 2));
    return size <= available;
}

// If start < end, then remove data from offset start up to but not including offset end by sliding data
// that is to the left of start to the right. If start > end, then make room for extra data from end to start
// by sliding data that is to the left of start to the left.
// Also fix up any record headers whose data has slid. Assumes there is enough room if it is a left
// shift (end < start).
void SlottedPage::slide(u_int16_t start, u_int16_t end)
{
    u_int16_t shift = end - start;
    if (shift == 0)
        return;

    // slide data
    void *to = this->address((u16)(this->end_free + 1 + shift));
    void *from = this->address((u16)(this->end_free + 1));
    int bytes = start - (this->end_free + 1U);
    char temp[bytes];
    memcpy(temp, from, bytes);
    memcpy(to, temp, bytes);

    // fix up headers
    RecordIDs *record_ids = ids();
    for (auto const &record_id : *record_ids)
    {
        u16 size, loc;
        get_header(size, loc, record_id);
        if (loc <= start)
        {
            loc += shift;
            this->put_header(record_id, size, loc);
        }
    }
    delete record_ids;
    this->end_free += shift;
    put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset)
{
    return *(u16 *)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n)
{
    *(u16 *)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void *SlottedPage::address(u16 offset)
{
    return (void *)((char *)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc)
{
    if (id == 0)
    { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

/*******************
 HeapFile Class
 ******************/

// Constructor
// HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0)
// {
//     this->dbfilename = name + ".db";
// }

// Create file
void HeapFile::create(void)
{
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage *block = this->get_new();
    this->put(block);
    delete block;
}

// Delete file
void HeapFile::drop(void)
{
    close();
    Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

// Open file
void HeapFile::open(void)
{
    this->db_open();
}

// Close file
void HeapFile::close(void)
{
    this->db.close(0);
    this->closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage *HeapFile::get_new(void)
{
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    delete page;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, this->last);
}

// Get a block from the database file.
SlottedPage *HeapFile::get(BlockID block_id)
{
    char b[DbBlock::BLOCK_SZ];
    Dbt d(b, sizeof(b));
    Dbt key(&block_id, sizeof(block_id));
    this->db.get(nullptr, &key, &d, 0);
    SlottedPage *s = new SlottedPage(d, block_id, false);
    return s;
}

// Write a block back to the database file.
void HeapFile::put(DbBlock *block)
{
    BlockID id = block->get_block_id();
    void *data = block->get_data();
    Dbt k(&id, sizeof(id));
    Dbt block_data(data, DB_BLOCK_SZ);
    this->db.put(nullptr, &k, &block_data, 0);
}

// Sequence of all block ids.
BlockIDs *HeapFile::block_ids()
{
    BlockIDs *vec = new BlockIDs();
    for (BlockID block_id = 1; block_id <= this->last; block_id++)
        vec->push_back(block_id);
    return vec;
}

uint32_t HeapFile::get_block_count()
{
    DB_BTREE_STAT *stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    return stat->bt_ndata;
}

// Wrapper for Berkeley DB open, which does both open and creation.
void HeapFile::db_open(uint flags)
{
    if (!this->closed)
        return;
    this->db.set_re_len(DbBlock::BLOCK_SZ); // record length - will be ignored if file already exists
    this->dbfilename = this->name + ".db";
    this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);

    this->last = flags ? 0 : get_block_count();
    this->closed = false;
}

/*******************
 HeapTable Class
 ******************/

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes), file(table_name)
{
}

// execute CREATE TABLE <table name> (<>columns)
void HeapTable::create()
{
    this->file.create();
}

// execute CREATE TABLE IF NOT EXISTS <table_name> (<columns>)
void HeapTable::create_if_not_exists()
{
    try
    {
        this->open();
    }
    catch (DbException &e)
    {
        this->create();
    }
}

// execute DROP TABLE <table_name>
void HeapTable::drop()
{
    this->file.drop();
}

// open a table and enable: INSERT, UPDATE, DELETE, SELECT, and PROJECT
void HeapTable::open()
{
    this->file.open();
}

// close a table
void HeapTable::close()
{
    this->file.close();
}

// execute INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
Handle HeapTable::insert(const ValueDict *row)
{
    this->open();
    Handle h = this->append(this->validate(row));
    return h;
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {}

void HeapTable::del(const Handle handle) {}

// return list of handles for all rows
Handles *HeapTable::select()
{
    return select(nullptr);
}

// execute SELECT <handle> FROM <table_name> WHERE <where>
// return a list of handles for specified rows
Handles *HeapTable::select(const ValueDict *where)
{
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id : *block_ids)
    {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id : *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

// Return a sequence of all values for handle.
ValueDict *HeapTable::project(Handle handle)
{
    ColumnNames *v = &this->column_names;
    return project(handle, v);
}

// Return a sequence of values for handle given by column_names.
ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names)
{
    SlottedPage *block = this->file.get(handle.first);
    Dbt *data = block->get(handle.second);
    ValueDict *row = this->unmarshal(data);

    if (column_names->size() == 0)
    {
        return row;
    }
    else
    {
        ValueDict *result = new ValueDict();
        for (auto const &column_name : *column_names)
        {
            (*result)[column_name] = (*row)[column_name];
        }
        return result;
    }
}

// Check if the given row is acceptable to insert. Raise ValueError if not.
// Otherwise return the whole row dictionary.
ValueDict *HeapTable::validate(const ValueDict *row) const
{
    ValueDict *full_row = new ValueDict();
    u16 col_num = 0;
    for (auto &column_name : this->column_names)
    {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        if (column == row->end())
            throw DbRelationError("Column does not existed");
        Value val = row->at(column_name);
        full_row->at(column_name) = val;
    }
    return v;
}

// Assumes row is fully fleshed-out. Appends a record to the file.
Handle HeapTable::append(const ValueDict *row)
{
    Dbt *newData = marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID recordID;
    Handle result;
    try
    {
        recordID = block->add(newData);
    }
    catch (DbException &e)
    {
        block = this->file.get_new();
        recordID = block->add(newData);
    }
    this->file.put(block);
    delete[](char *) newData->get_data();
    delete block;
    result.first = file.get_last_block_id();
    result.second = recordID;
    return result;
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt *HeapTable::marshal(const ValueDict *row)
{
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name : this->column_names)
    {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT)
        {
            *(int32_t *)(bytes + offset) = value.n;
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
        {
            uint size = value.s.length();
            *(u16 *)(bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        }
        else
        {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

// Converts marshaled object back to original object type
ValueDict *HeapTable::unmarshal(Dbt *data)
{
    ValueDict *row = new ValueDict();
    Value value;
    char *bytes = (char *)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name : this->column_names)
    {
        ColumnAttribute ca = this->column_attributes[col_num++];
        value.data_type = ca.get_data_type();
        if (ca.get_data_type() == ColumnAttribute::DataType::INT)
        {
            value.n = *(int32_t *)(bytes + offset);
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
        {
            u16 size = *(u16 *)(bytes + offset);
            offset += sizeof(u16);
            char buffer[DbBlock::BLOCK_SZ];
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            value.s = string(buffer); // assume ascii for now
            offset += size;
        }
        //BOOLEAN not implemented (see storage_engine.h)
        // else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN)
        // {
        //     value.n = *(uint8_t *)(bytes + offset);
        //     offset += sizeof(uint8_t);
        // }
        else
        {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
        (*row)[column_name] = value;
    }
    return row;
}

// TEST FOR HEAP STORAGE
bool test_heap_storage()
{
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop(); // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles *handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
    table.drop();

    return true;
}

/**
 * Print out given failure message and return false.
 * @param message reason for failure
 * @return false
 */
bool assertion_failure(string message)
{
    cout << "FAILED TEST: " << message << endl;
    return false;
}
/**
 * Testing function for SlottedPage.
 * @return true if testing succeeded, false otherwise */
bool test_slotted_page()
{
    // construct one
    char blank_space[DbBlock::BLOCK_SZ];
    Dbt block_dbt(blank_space, sizeof(blank_space));
    SlottedPage slot(block_dbt, 1, true);
    // add a record
    char rec1[] = "hello";
    Dbt rec1_dbt(rec1, sizeof(rec1));
    RecordID id = slot.add(&rec1_dbt);
    if (id != 1)
        return assertion_failure("add id 1");
    // get it back
    Dbt *get_dbt = slot.get(id);
    string expected(rec1, sizeof(rec1));
    string actual((char *)get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 1 back " + actual);
    delete get_dbt;
    // add another record and fetch it back
    char rec2[] = "goodbye";
    Dbt rec2_dbt(rec2, sizeof(rec2));
    id = slot.add(&rec2_dbt);
    if (id != 2)
        return assertion_failure("add id 2");
    // get it back
    get_dbt = slot.get(id);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *)get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 2 back " + actual);
    delete get_dbt;
    // test put with expansion (and slide and ids)
    char rec1_rev[] = "something much bigger";
    rec1_dbt = Dbt(rec1_rev, sizeof(rec1_rev));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after expanding put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *)get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 2 back after expanding put of 1 " + actual);
    delete get_dbt;
    get_dbt = slot.get(1);
    expected = string(rec1_rev, sizeof(rec1_rev));
    actual = string((char *)get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 1 back after expanding put of 1 " + actual);
    delete get_dbt;
    // test put with contraction (and slide and ids)
    rec1_dbt = Dbt(rec1, sizeof(rec1));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after contracting put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *)get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 2 back after contracting put of 1 " + actual);
    delete get_dbt;
    get_dbt = slot.get(1);
    expected = string(rec1, sizeof(rec1));
    actual = string((char *)get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 1 back after contracting put of 1 " + actual);
    delete get_dbt;
    // test del (and ids)
    RecordIDs *id_list = slot.ids();
    if (id_list->size() != 2 || id_list->at(0) != 1 || id_list->at(1) != 2)
        return assertion_failure("ids() with 2 records");
    delete id_list;
    slot.del(1);
    id_list = slot.ids();
    if (id_list->size() != 1 || id_list->at(0) != 2)
        return assertion_failure("ids() with 1 record remaining");
    delete id_list;
    get_dbt = slot.get(1);
    if (get_dbt != nullptr)
        return assertion_failure("get of deleted record was not null");
    // try adding something too big
    rec2_dbt = Dbt(nullptr, DbBlock::BLOCK_SZ - 10); // too big, but only because we have a record in there
    try
    {
        slot.add(&rec2_dbt);
        return assertion_failure("failed to throw when add too big");
    }
    catch (const DbBlockNoRoomError &exc)
    {
        // test succeeded - this is the expected path
    }
    catch (...)
    {
        // Note that this won't catch segfault signals -- but in that case we also know the test failed
        return assertion_failure("wrong type thrown when add too big");
    }
    return true;
}
