/**
 * @file btree.cpp - implementation of BTreeIndex, etc.
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2020"
 */
#include "btree.h"

BTreeIndex::BTreeIndex(DbRelation &relation, Identifier name, ColumnNames key_columns, bool unique) : DbIndex(relation,
                                                                                                              name,
                                                                                                              key_columns,
                                                                                                              unique),
                                                                                                      closed(true),
                                                                                                      stat(nullptr),
                                                                                                      root(nullptr),
                                                                                                      file(relation.get_table_name() +
                                                                                                           "-" + name),
                                                                                                      key_profile() {
    if (!unique)
        throw DbRelationError("BTree index must have unique key");
    build_key_profile();
}

BTreeIndex::~BTreeIndex() {
    delete stat;
    delete root;
}

// Create the index.
void BTreeIndex::create() {
    file.create();
    stat = new BTreeStat(file, STAT, STAT + 1, key_profile);
    root = new BTreeLeaf(file, stat->get_root_id(), key_profile, true);
    closed = false;
    Handles *table_rows = relation.select();
    for (auto const &row: *table_rows)
        insert(row);
    delete table_rows;
}

// Drop the index.
void BTreeIndex::drop() {
    file.drop();
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {
    if (closed) {
        file.open();
        stat = new BTreeStat(file, STAT, key_profile);
        if (stat->get_height() == 1)
            root = new BTreeLeaf(file, stat->get_root_id(), key_profile, false);
        else
            root = new BTreeInterior(file, stat->get_root_id(), key_profile, false);
        closed = true;
    }
}

// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeIndex::close() {
    if (!closed) {
        file.close();
        delete stat;
        stat = nullptr;
        delete root;
        root = nullptr;
        closed = true;
    }
}



// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles *BTreeIndex::lookup(ValueDict *key_dict) const {
    //this->open();
    KeyValue *key = this->tkey(key_dict);
    Handles* handles = this->_lookup(this->root, this->stat->get_height(), key);
    delete key;
    return handles;
}

Handles *BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue *key) const {
    std::cout << "look up 1 " << height<< std::endl;
    if(height == 1) {
        Handles *handles = new Handles();
        auto *leaf = dynamic_cast<BTreeLeaf*>(node);
        Handle handle;
        try {
            handle = leaf->find_eq(key);
        } catch(...) {
            return handles;
        }
        handles -> push_back(handle);
        return handles;
    } else {
        std::cout << "look up 2 " << height<< std::endl;
        auto *interior = dynamic_cast<BTreeInterior*>(node);
        std::cout << "look up 3 " << height<< std::endl;
        return _lookup(interior->find(key, height), height - 1, key);
    }
}

Handles *BTreeIndex::range(ValueDict *min_key, ValueDict *max_key) const {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
    // FIXME
}

/**
 * Insert a row with the given handle. Row must exist in relation already.
 * @param handle the given handle of the row to insert
 */ 
void BTreeIndex::insert(Handle handle) {

    this->open();
    ValueDict *projection = this->relation.project(handle); // map<Identifier, Value>
    KeyValue *tkey = this->tkey(projection);
    Insertion insertion = this->_insert(this->root, this->stat->get_height(), tkey, handle); // pair<BlockID, KeyValue>

    if (!BTreeNode::insertion_is_none(insertion)) {
        BTreeInterior *new_root = new BTreeInterior(file, 0, this->key_profile, true);
        new_root->set_first(this->root->get_id());
        new_root->insert(&insertion.second, insertion.first);
        new_root->save();
        this->stat->set_root_id(new_root->get_id());
        this->stat->set_height(this->stat->get_height() + 1);
        this->stat->save();
        delete this->root;
        this->root = new_root;
        std::cout << "new root: " << *new_root << std::endl;
    }
    delete tkey;
    delete projection;
}

/**
 * Recursive insert. If a split happens at this level, return the (new node, boundary) of the split.
 * @param node the given node 
 * @param height the given height
 * @param key the given keyvalue
 * @param handle the given handle
 * @return insertion result
 */ 
Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue *key, Handle handle) {

    if (height == 1) {
        auto *leaf = dynamic_cast<BTreeLeaf *>(node); // BTreeLeaf *leaf = (BTreeLeaf *) node;
        return leaf->insert(key, handle);

    } else {
        auto *interior = dynamic_cast<BTreeInterior *>(node); // BTreeInterior *interior = (BTreeInterior *) node
        BTreeNode *tmp_node = interior->find(key, height);
        Insertion insertion = _insert(tmp_node, height - 1, key, handle);

        if (!BTreeNode::insertion_is_none(insertion))
            insertion = interior->insert(&insertion.second, insertion.first);

        delete tmp_node;
        return insertion;
    }
}

void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
    // FIXME
}

KeyValue *BTreeIndex::tkey(const ValueDict *key) const {
    KeyValue *key_value = new KeyValue();
    for (auto const &column_name: key_columns)
        key_value->push_back(key->find(column_name)->second);
    return key_value;
}

// Figure out the data types of each key component and encode them in key_profile, a list of int/str classes.
void BTreeIndex::build_key_profile() {
    std::map<const Identifier, ColumnAttribute::DataType> types_by_colname;
    const ColumnAttributes column_attributes = relation.get_column_attributes();
    uint col_num = 0;
    for (auto const &column_name: relation.get_column_names()) {
        ColumnAttribute ca = column_attributes[col_num++];
        types_by_colname[column_name] = ca.get_data_type();
    }
    for (auto const &column_name: key_columns)
        key_profile.push_back(types_by_colname[column_name]);
}

bool test_btree() {
    std::cout<<"test btree start 1 " << std::endl;
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    column_attributes.push_back(ColumnAttribute(ColumnAttribute::INT));
    column_attributes.push_back(ColumnAttribute(ColumnAttribute::INT));
    HeapTable table("__test_btree", column_names, column_attributes);
    std::cout<<"test btree start 2 " << std::endl;
    table.create();
    std::cout<<"test btree start 3 " << std::endl;
    ValueDict row1, row2;
    row1["a"] = Value(12);
    row1["b"] = Value(99);
    row2["a"] = Value(88);
    row2["b"] = Value(101);
    table.insert(&row1);
    std::cout<<"test btree start 4 " << std::endl;
    table.insert(&row2);
    std::cout<<"test btree start 5 " << std::endl;
    // for (int i = 0; i < 100 * 1000; i++) {
    for (int i = 0; i < 10000; i++) {
        ValueDict row;
        row["a"] = Value(i + 100);
        row["b"] = Value(-i);
        table.insert(&row);
    }
    std::cout<<"test btree start 6 " << std::endl;
    column_names.clear();
    column_names.push_back("a");
    BTreeIndex index(table, "fooindex", column_names, true);
    std::cout<<"test btree start 7 " << std::endl;
    index.create();
    //return true;  // FIXME

    std::cout<<"test btree start 8 " << std::endl;
    ValueDict lookup;
    std::cout<<"test btree start 9 " << std::endl;
    lookup["a"] = 12;
    std::cout<<"test btree start 10 " << std::endl;
    Handles *handles = index.lookup(&lookup);
    std::cout<<"test btree start 11 " << std::endl;
    ValueDict *result = table.project(handles->back());
    std::cout<<"test btree start 12 " << std::endl;
    if (*result != row1) {
        std::cout << "first lookup failed" << std::endl;
        return false;
    }
    delete handles;
    delete result;
    lookup["a"] = 88;
    std::cout<<"test btree start 13 " << std::endl;
    handles = index.lookup(&lookup);
    result = table.project(handles->back());
    if (*result != row2) {
        std::cout << "second lookup failed" << std::endl;
        return false;
    }
    delete handles;
    delete result;
    lookup["a"] = 6;
    handles = index.lookup(&lookup);
    if (handles->size() != 0) {
        std::cout << "third lookup failed" << std::endl;
        return false;
    }
    delete handles;
    std::cout<<"test btree start 14 " << std::endl;
    for (uint j = 0; j < 10; j++)
        // for (int i = 0; i < 1000; i++) {
        for (int i = 0; i < 100; i++) {
            std::cout<<"test btree start 15 : " << j << " i: " << i << std::endl;
            lookup["a"] = i + 100;
            
            handles = index.lookup(&lookup);
            std::cout << "for loop test 1" << std::endl;
            result = table.project(handles->back());
            std::cout << "for loop test 2" << std::endl;
            row1["a"] = i + 100;
            row1["b"] = -i;
            if (*result != row1) {
                std::cout << "lookup failed " << i << std::endl;
                return false;
            }
            delete handles;
            delete result;
        }

    // test delete
    ValueDict row;
    row["a"] = 44;
    row["b"] = 44;
    auto thandle = table.insert(&row);
    index.insert(thandle);
    lookup["a"] = 44;
    handles = index.lookup(&lookup);
    thandle = handles->back();
    delete handles;
    result = table.project(thandle);
    if (*result != row) {
        std::cout << "44 lookup failed" << std::endl;
        return false;
    }
    delete result;
    std::cout<<"insertion test done" << std::endl;

    // index.del(thandle);
    // table.del(thandle);
    // handles = index.lookup(&lookup);
    // if (handles->size() != 0) {
    //     std::cout << "delete failed" << std::endl;
    //     return false;
    // }
    // delete handles;

    // // test range
    // ValueDict minkey, maxkey;
    // minkey["a"] = 100;
    // maxkey["a"] = 310;
    // handles = index.range(&minkey, &maxkey);
    // ValueDicts *results = table.project(handles);
    // for (int i = 0; i < 210; i++) {
    //     if (results->at(i)->at("a") != Value(100 + i)) {
    //         ValueDict *wrong = results->at(i);
    //         std::cout << "range failed: " << i << ", a: " << wrong->at("a").n << ", b: " << wrong->at("b").n
    //                   << std::endl;
    //         return false;
    //     }
    // }
    // delete handles;
    // for (auto vd: *results)
    //     delete vd;
    // delete results;

    // // test range from beginning and to end
    // handles = index.range(nullptr, nullptr);
    // u_long count_i = handles->size();
    // delete handles;
    // handles = table.select();
    // u_long count_t = handles->size();
    // if (count_i != count_t) {
    //     std::cout << "full range failed: " << count_i << std::endl;
    //     return false;
    // }
    // for (u_long i = 0; i < count_t; i++)
    //     index.del((*handles)[i]);
    // delete handles;
    // handles = index.range(nullptr, nullptr);
    // count_i = handles->size();
    // delete handles;
    // if (count_i != 0) {
    //     std::cout << "delete everything failed: " << count_i << std::endl;
    //     return false;
    // }
    index.drop();
    table.drop();
    return true;
}

