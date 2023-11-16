#include <db/JoinPredicate.h>
#include <db/Tuple.h>

using namespace db;

JoinPredicate::JoinPredicate(int field1, Predicate::Op op, int field2): f1(field1), p(op), f2(field2) {
    // TODO pa3.1: some code goes here
    this->f1 = field1;
    this->p = op;
    this->f2 = field2;
}

bool JoinPredicate::filter(Tuple *t1, Tuple *t2) {
    // TODO pa3.1: some code goes here
    if (!t1 || !t2) {
        return false;
    }

    switch(p) {
        case Predicate::Op::EQUALS:
        case Predicate::Op::LESS_THAN:
        case Predicate::Op::GREATER_THAN:
            return t1->getField(f1).compare(p, &t2->getField(f2));
        default:
            return false;
    }
}

int JoinPredicate::getField1() const {
    // TODO pa3.1: some code goes here
    return f1;
}

int JoinPredicate::getField2() const {
    // TODO pa3.1: some code goes here
    return f2;
}

Predicate::Op JoinPredicate::getOperator() const {
    // TODO pa3.1: some code goes here
    return p;
}
