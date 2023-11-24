#include <db/Insert.h>
#include <db/Database.h>
#include <db/IntField.h>

using namespace db;

std::optional<Tuple> Insert::fetchNext() {
    // TODO pa3.3: some code goes here
    BufferPool &bufferPool = Database::getBufferPool();
    int count = 0;
    while (c->hasNext()) {
        Tuple t = c->next();
        bufferPool.insertTuple(transactionId, tid, &t);
        count++;
    }
    TupleDesc td = c->getTupleDesc();
    Tuple countTuple(td);
    IntField countField(count);
    countTuple.setField(0, &countField);
    return countTuple;
}

Insert::Insert(TransactionId t, DbIterator *child, int tableId) {
    // TODO pa3.3: some code goes here
    this->transactionId = t;
    this->c = child;
    this->tid = tableId;
}

const TupleDesc &Insert::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    return c->getTupleDesc();
}

void Insert::open() {
    // TODO pa3.3: some code goes here
    Operator::open();
    c->open();
}

void Insert::close() {
    // TODO pa3.3: some code goes here
    Operator::close();
    c->close();
}

void Insert::rewind() {
    // TODO pa3.3: some code goes here
    Operator::rewind();
    c->rewind();
}

std::vector<DbIterator *> Insert::getChildren() {
    // TODO pa3.3: some code goes here
    return {c};
}

void Insert::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    if (!children.empty()) {
        c = children[0];
    }
}
