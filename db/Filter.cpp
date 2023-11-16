#include <db/Filter.h>

using namespace db;

Filter::Filter(Predicate p, DbIterator *child): predicate(p), childIterator(child) {
    // TODO pa3.1: some code goes here
    this->predicate = p;
    this->childIterator = child;
}

Predicate *Filter::getPredicate() {
    // TODO pa3.1: some code goes here
    return &this->predicate;
}

const TupleDesc &Filter::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return childIterator->getTupleDesc();
}

void Filter::open() {
    // TODO pa3.1: some code goes here
    childIterator->open();
}

void Filter::close() {
    // TODO pa3.1: some code goes here
    childIterator->close();
}

void Filter::rewind() {
    // TODO pa3.1: some code goes here
    childIterator->rewind();
}

std::vector<DbIterator *> Filter::getChildren() {
    // TODO pa3.1: some code goes here
    return {childIterator};
}

void Filter::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    if (!children.empty()) {
        childIterator = children[0];
    }
}

std::optional<Tuple> Filter::fetchNext() {
    // TODO pa3.1: some code goes here
    while (childIterator->hasNext()) {
        Tuple nextTuple = childIterator->next();
        if (predicate.filter(nextTuple)) {
            return nextTuple;
        }
    }
    return std::nullopt;
}
