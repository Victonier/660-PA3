#include <db/Join.h>

using namespace db;

Join::Join(JoinPredicate *p, DbIterator *child1, DbIterator *child2): jPredicate(p), childIterator1(child1), childIterator2(child2) {
    // TODO pa3.1: some code goes here
    this->jPredicate = p;
    this->childIterator1 = child1;
    this->childIterator2 = child2;
}

JoinPredicate *Join::getJoinPredicate() {
    // TODO pa3.1: some code goes here
    return this->jPredicate;
}

std::string Join::getJoinField1Name() {
    // TODO pa3.1: some code goes here
    int index1 = jPredicate->getField1();
    return childIterator1->getTupleDesc().getFieldName(index1);
}

std::string Join::getJoinField2Name() {
    // TODO pa3.1: some code goes here
    int index2 = jPredicate->getField2();
    return childIterator2->getTupleDesc().getFieldName(index2);
}

const TupleDesc &Join::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    static TupleDesc merged = TupleDesc::merge(childIterator1->getTupleDesc(), childIterator2->getTupleDesc());
    return merged;
}

void Join::open() {
    // TODO pa3.1: some code goes here
    childIterator1->open();
    childIterator2->open();
}

void Join::close() {
    // TODO pa3.1: some code goes here
    childIterator1->close();
    childIterator2->close();
}

void Join::rewind() {
    // TODO pa3.1: some code goes here
    childIterator1->rewind();
    childIterator2->rewind();
}

std::vector<DbIterator *> Join::getChildren() {
    // TODO pa3.1: some code goes here
    return {childIterator1, childIterator2};
}

void Join::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    if (children.size() == 2) {
        childIterator1 = children[0];
        childIterator2 = children[1];
    }
}

std::optional<Tuple> Join::fetchNext() {
    // TODO pa3.1: some code goes here
    while (childIterator1->hasNext()) {
        Tuple tuple1 = childIterator1->next();
        while (childIterator2->hasNext()) {
            Tuple tuple2 = childIterator2->next();
            if (jPredicate->filter(&tuple1, &tuple2)) {
                TupleDesc mergedDesc = getTupleDesc();
                Tuple joinedTuple(mergedDesc);
                int index = 0;
                for (int i = 0; i < tuple1.getTupleDesc().numFields(); ++i) {
                    joinedTuple.setField(index++, &tuple1.getField(i));
                }
                for (int j = 0; j < tuple2.getTupleDesc().numFields(); ++j) {
                    joinedTuple.setField(index++, &tuple2.getField(j));
                }
                return joinedTuple;
            }
        }
        childIterator2->rewind();
    }
    return std::nullopt;
}
