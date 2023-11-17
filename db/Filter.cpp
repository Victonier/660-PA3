#include <db/Filter.h>

using namespace db;

Filter::Filter(Predicate p, DbIterator *child) {
    // TODO pa3.1: some code goes here
    childOp=child;
    this->p=p;
}

Predicate *Filter::getPredicate() {
    // TODO pa3.1: some code goes here
    return &p;
}

const TupleDesc &Filter::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return childOp->getTupleDesc();
}

void Filter::open() {
    // TODO pa3.1: some code goes here
    Operator::open();
    childOp->open();
}

void Filter::close() {
    // TODO pa3.1: some code goes here
    Operator::close();
    childOp->close();
}

void Filter::rewind() {
    // TODO pa3.1: some code goes here
    childOp->rewind();
}

std::vector<DbIterator *> Filter::getChildren() {
    // TODO pa3.1: some code goes here
    auto v=std::vector<DbIterator *>();
    v.push_back(childOp);
    return v;
}

void Filter::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    if (children.size()<1) return;
    childOp=children[0];
}

std::optional<Tuple> Filter::fetchNext() {
    // TODO pa3.1: some code goes here
    while (childOp->hasNext()) {
        Tuple curT=childOp->next();
        if (p.filter(curT)) return curT;
    }
    return std::optional<Tuple>();
}
