#include <db/HashEquiJoin.h>

using namespace db;

HashEquiJoin::HashEquiJoin(JoinPredicate p, DbIterator *child1, DbIterator *child2): joinPredicate(p), c1(child1), c2(child2) {
    // TODO pa3.1: some code goes here
    this->joinPredicate = p;
    this->c1 = child1;
    this->c2 = child2;
}

JoinPredicate *HashEquiJoin::getJoinPredicate() {
    // TODO pa3.1: some code goes here
    return &this->joinPredicate;
}

const TupleDesc &HashEquiJoin::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    static TupleDesc merged = TupleDesc::merge(c1->getTupleDesc(), c2->getTupleDesc());
    return merged;
}

std::string HashEquiJoin::getJoinField1Name() {
    // TODO pa3.1: some code goes here
    int index1 = joinPredicate.getField1();
    return c1->getTupleDesc().getFieldName(index1);
}

std::string HashEquiJoin::getJoinField2Name() {
    // TODO pa3.1: some code goes here
    int index2 = joinPredicate.getField2();
    return c2->getTupleDesc().getFieldName(index2);
}

void HashEquiJoin::open() {
    // TODO pa3.1: some code goes here
    Operator::open();
    c1->open();
    c2->open();
}

void HashEquiJoin::close() {
    // TODO pa3.1: some code goes here
    Operator::close();
    c1->close();
    c2->close();
}

void HashEquiJoin::rewind() {
    // TODO pa3.1: some code goes here
    Operator::rewind();
    c1->rewind();
    c2->rewind();
}

std::vector<DbIterator *> HashEquiJoin::getChildren() {
    // TODO pa3.1: some code goes here
    return {c1, c2};
}

void HashEquiJoin::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    if (children.size() == 2) {
        c1 = children[0];
        c2 = children[1];
    }
}

std::optional<Tuple> HashEquiJoin::fetchNext() {
    // TODO pa3.1: some code goes here
    static std::optional<Tuple> leftTuple = std::nullopt;

    while (true) {
        if (!leftTuple.has_value() && c1->hasNext()) {
            leftTuple = c1->next();
        }
        while (leftTuple.has_value() && c2->hasNext()) {
            Tuple tuple2 = c2->next();
            if (joinPredicate.filter(&leftTuple.value(), &tuple2)) {
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
        if (!c2->hasNext()) {
            c2->rewind();
            // no match -> reset the leftTuple
            leftTuple = std::nullopt;
        }
        if (!leftTuple.has_value() && !c1->hasNext()) {
            return std::nullopt;
        }
    }
}