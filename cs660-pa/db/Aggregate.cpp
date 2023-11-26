#include <db/Aggregate.h>
#include <db/IntegerAggregator.h>
#include <db/StringAggregator.h>

using namespace db;

std::optional<Tuple> Aggregate::fetchNext() {
    // TODO pa3.2: some code goes here
    if (this->DbIter->hasNext()) return DbIter->next();
    return std::optional<Tuple>();
}

Aggregate::Aggregate(DbIterator *child, int afield, int gfield, Aggregator::Op aop) {
    // TODO pa3.2: some code goes here
    this->child=child;
    this->afield=afield;
    this->gfield=gfield;
    this->aop=aop;
}

int Aggregate::groupField() {
    // TODO pa3.2: some code goes here
    return this->gfield;
}

std::string Aggregate::groupFieldName() {
    // TODO pa3.2: some code goes here
    if (this->gfield!=Aggregator::NO_GROUPING)
        return this->child->getTupleDesc().getFieldName(this->gfield);
    return std::string();
}

int Aggregate::aggregateField() {
    // TODO pa3.2: some code goes here
    return afield;
}

std::string Aggregate::aggregateFieldName() {
    // TODO pa3.2: some code goes here
    return child->getTupleDesc().getFieldName(this->afield);
}

Aggregator::Op Aggregate::aggregateOp() {
    // TODO pa3.2: some code goes here
    return aop;
}

void Aggregate::open() {
    // TODO pa3.2: some code goes here
    Aggregator* agtor;
    Types::Type type;
    if (this->gfield!=Aggregator::NO_GROUPING){
        type=this->child->getTupleDesc().getFieldType(this->gfield);
    }
    if (this->child->getTupleDesc().getFieldType(afield)==Types::INT_TYPE)
        agtor=new IntegerAggregator(gfield,type,afield,aop);
    else
        agtor=new StringAggregator(gfield,type,afield,aop);
    child->open();
    while (child->hasNext()){
        auto t=child->next();
        agtor->mergeTupleIntoGroup(&t);
    }
    child->close();
    DbIter=agtor->iterator();
    DbIter->open();
    Operator::open();
}

void Aggregate::rewind() {
    // TODO pa3.2: some code goes here
    DbIter->rewind();
}

const TupleDesc &Aggregate::getTupleDesc() const {
    // TODO pa3.2: some code goes here
    std::string agname=to_string(aop)+child->getTupleDesc().getFieldName(afield);
    if (gfield==Aggregator::NO_GROUPING)
        return TupleDesc(std::vector<Types::Type>{Types::INT_TYPE},std::vector<std::string>{agname});
    return TupleDesc(std::vector<Types::Type>{child->getTupleDesc().getFieldType(gfield),Types::INT_TYPE},
                     std::vector<std::string>{child->getTupleDesc().getFieldName(gfield),agname});
}

void Aggregate::close() {
    // TODO pa3.2: some code goes here
    Operator::close();
    DbIter->close();
}

std::vector<DbIterator *> Aggregate::getChildren() {
    // TODO pa3.2: some code goes here
    auto v=std::vector<DbIterator *>();
    v.push_back(child);
    return v;
}

void Aggregate::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.2: some code goes here
    if (children.size()<1) return;
    child=children[0];
}
