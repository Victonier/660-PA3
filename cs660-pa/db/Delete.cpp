#include <db/Delete.h>
#include <db/BufferPool.h>
#include <db/IntField.h>
#include <db/Database.h>

using namespace db;

Delete::Delete(TransactionId t, DbIterator *child) {
    // TODO pa3.3: some code goes here
    this->transactionId = t;
    this->c = child;
}

const TupleDesc &Delete::getTupleDesc() const {
    // TODO pa3.3: some code goes here
    return c->getTupleDesc();
}

void Delete::open() {
    // TODO pa3.3: some code goes here
    Operator::open();
    c->open();
}

void Delete::close() {
    // TODO pa3.3: some code goes here
    Operator::close();
    c->close();
}

void Delete::rewind() {
    // TODO pa3.3: some code goes here
    Operator::rewind();
    c->rewind();
}

std::vector<DbIterator *> Delete::getChildren() {
    // TODO pa3.3: some code goes here
    return {c};
}

void Delete::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.3: some code goes here
    if (!children.empty()) {
        c = children[0];
    }
}

std::optional<Tuple> Delete::fetchNext() {
    // TODO pa3.3: some code goes here
    BufferPool &bufferPool = Database::getBufferPool();
    int count = 0;
    while (c->hasNext()) {
        Tuple t = c->next();
        bufferPool.deleteTuple(transactionId, &t);
        count++;
    }
    TupleDesc td = c->getTupleDesc();
    Tuple countTuple(td);
    IntField countField(count);
    countTuple.setField(0, &countField);
    return countTuple;
}
