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
    Operator::open();
    childIterator1->open();
    childIterator2->open();
}

void Join::close() {
    // TODO pa3.1: some code goes here
    Operator::close();
    childIterator1->close();
    childIterator2->close();
}

void Join::rewind() {
    // TODO pa3.1: some code goes here
    Operator::rewind();
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
    static std::optional<Tuple> leftTuple = std::nullopt;

    while (true) {
        if (!leftTuple.has_value() && childIterator1->hasNext()) {
            leftTuple = childIterator1->next();
        }
        while (leftTuple.has_value() && childIterator2->hasNext()) {
            Tuple tuple2 = childIterator2->next();
            if (jPredicate->filter(&leftTuple.value(), &tuple2)) {
                TupleDesc mergedDesc = getTupleDesc();
                Tuple joinedTuple(mergedDesc);
                int index = 0;
                // merge the tuple
                for (int i = 0; i < leftTuple.value().getTupleDesc().numFields(); ++i) {
                    joinedTuple.setField(index++, &leftTuple.value().getField(i));
                }
                for (int j = 0; j < tuple2.getTupleDesc().numFields(); ++j) {
                    joinedTuple.setField(index++, &tuple2.getField(j));
                }
                return joinedTuple;
            }
        }
        // rewind
        if (!childIterator2->hasNext()) {
            childIterator2->rewind();
            // no match -> reset the leftTuple
            leftTuple = std::nullopt;
        }
        if (!leftTuple.has_value() && !childIterator1->hasNext()) {
            return std::nullopt;
        }
    }
}