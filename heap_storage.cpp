#include "heap_storage.h"
#include <stdio.h>
#include <stdlib.h>

/* FIXME FIXME FIXME */

/*******************
 SlottedPage Class
 ******************/

typedef u_int16_t u16;

// Constructor for SlottedPage
SlottedPage::SlottedPage(Dbt &block, 
                        BlockID block_id, 
                        bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

Dbt* SlottedPage::get(RecordID record_id) const {}

void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError) {}

void SlottedPage::del(RecordID record_id){}

RecordIDs* SlottedPage::ids(void) const {}

void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) const {}

void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {}

bool SlottedPage::has_room(u_int16_t size) {}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {}

/* FIXME FIXME FIXME */

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

/* FIXME FIXME FIXME */

/*******************
 HeapFile Class
 ******************/

// Constructor
HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
	this->dbfilename = this->name + ".db";
}

void HeapFile::create(void) {}

void HeapFile::drop(void) {}

void HeapFile::open(void) {}

void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}

SlottedPage* HeapFile::get_new(void) {}

SlottedPage* HeapFile::get(BlockID block_id) {}

void HeapFile::put(DbBlock* block){}

BlockIDs* HeapFile::block_ids() const {}

void HeapFile::db_open(uint flags){}


/*******************
 HeapTable Class
 ******************/

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes ) :
		DbRelation(table_name, column_names, column_attributes), file(table_name) {
}

// execute CREATE TABLE <table name> (<>columns)
void HeapTable::create() {
    this->file.create();
}

// execute CREATE TABLE IF NOT EXISTS <table_name> (<columns>)
void HeapTable::create_if_not_exists() {
    try{
        this->open();
    } catch (DbException& e) {
        this->create();
    }
}

// execute DROP TABLE <table_name>
void HeapTable::drop() {
    this->file.drop();
}

// open a table and enable: INSERT, UPDATE, DELETE, SELECT, and PROJECT
void HeapTable::open() {
    this->file.open();
}

// close a table
void HeapTable::close() {
    this->file.close();
}

// execute INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
Handle HeapTable::insert(const ValueDict* row) {
    this->open();    
    Handle h = this->append(this->validate(row));
    return h;
}

void HeapTable::update(const Handle handle, const ValueDict* new_values){}

void HeapTable::del(const Handle handle) {}

Handles* HeapTable::select(){}

// execute SELECT <handle> FROM <table_name> WHERE <where>
// Return a list of handles for specified rows
Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

ValueDict* HeapTable::project(Handle handle) {}

ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {}

ValueDict* HeapTable::validate(const ValueDict* row) {}

Handle HeapTable::append(const ValueDict* row) {}


// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict* HeapTable::unmarshal(Dbt* data) {}

bool test_heap_storage() {}
