#include <db/Join.h>

using namespace db;

Join::Join(JoinPredicate *p, DbIterator *child1, DbIterator *child2) {
    // TODO pa3.1: some code goes here
    predict=p;
    leftChild=child1;
    rightChild=child2;
}

JoinPredicate *Join::getJoinPredicate() {
    // TODO pa3.1: some code goes here
    return predict;
}

std::string Join::getJoinField1Name() {
    // TODO pa3.1: some code goes here
    return leftChild->getTupleDesc().getFieldName(predict->getField1());
}

std::string Join::getJoinField2Name() {
    // TODO pa3.1: some code goes here
    return rightChild->getTupleDesc().getFieldName(predict->getField2());
}

const TupleDesc &Join::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return TupleDesc::merge(leftChild->getTupleDesc(),rightChild->getTupleDesc());
}

void Join::open() {
    // TODO pa3.1: some code goes here
    Operator::open();
    leftChild->open();
    rightChild->open();
}

void Join::close() {
    // TODO pa3.1: some code goes here
    Operator::close();
    leftChild->close();
    rightChild->close();
}

void Join::rewind() {
    // TODO pa3.1: some code goes here
    leftChild->rewind();
    rightChild->rewind();

}

std::vector<DbIterator *> Join::getChildren() {
    // TODO pa3.1: some code goes here
    auto v=std::vector<DbIterator *>();
    v.push_back(leftChild);
    v.push_back(rightChild);
    return v;
}

void Join::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    if (children.size()<2) return;
    leftChild=children[0];
    rightChild=children[1];
}

std::optional<Tuple> Join::fetchNext() {
    // TODO pa3.1: some code goes here
    while (leftChild->hasNext())
    {
        Tuple leftTuple=leftChild->next();
        while (rightChild->hasNext())
        {
            Tuple rightTuple=rightChild->next();
            if (predict->filter(&leftTuple,&rightTuple)){
                Tuple tmpT=Tuple(this->getTupleDesc());
                size_t leftsize =leftChild->getTupleDesc().numFields();
                size_t rightsize= rightChild->getTupleDesc().numFields();
                for (int i=0;i<leftsize;i++)
                    tmpT.setField(i,&leftTuple.getField(i));
                for (int i=leftsize;i<leftsize+rightsize;i++)
                    tmpT.setField(i,&rightTuple.getField(i-leftsize));
                return tmpT;
            }
        }
        rightChild->rewind();
    }
    return std::optional<Tuple>();
}
