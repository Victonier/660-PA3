#include <db/HashEquiJoin.h>

using namespace db;

HashEquiJoin::HashEquiJoin(JoinPredicate p, DbIterator *child1, DbIterator *child2) {
    // TODO pa3.1: some code goes here
    this->p=p;
    leftchild=child1;
    rightchild=child2;
}

JoinPredicate *HashEquiJoin::getJoinPredicate() {
    // TODO pa3.1: some code goes here
    return &p;
}

const TupleDesc &HashEquiJoin::getTupleDesc() const {
    // TODO pa3.1: some code goes here
    return TupleDesc::merge(leftchild->getTupleDesc(),rightchild->getTupleDesc());
}

std::string HashEquiJoin::getJoinField1Name() {
    // TODO pa3.1: some code goes here
    return this->leftchild->getTupleDesc().getFieldName(this->p.getField1());
}

std::string HashEquiJoin::getJoinField2Name() {
    // TODO pa3.1: some code goes here
    return this->rightchild->getTupleDesc().getFieldName(this->p.getField2());
}

void HashEquiJoin::open() {
    // TODO pa3.1: some code goes here
    leftchild->open();
    rightchild->open();
    loadMap();
    Operator::open();
}

void HashEquiJoin::close() {
    // TODO pa3.1: some code goes here
    Operator::close();
    leftchild->close();
    rightchild->close();
}

void HashEquiJoin::rewind() {
    // TODO pa3.1: some code goes here
    leftchild->rewind();
    rightchild->rewind();
}

std::vector<DbIterator *> HashEquiJoin::getChildren() {
    // TODO pa3.1: some code goes here
    auto v=std::vector<DbIterator *>();
    v.push_back(leftchild);
    v.push_back(rightchild);
    return v;
}

void HashEquiJoin::setChildren(std::vector<DbIterator *> children) {
    // TODO pa3.1: some code goes here
    if (children.size()<2) return;
    leftchild=children[0];
    rightchild=children[1];
}

std::optional<Tuple> HashEquiJoin::fetchNext() {
    // TODO pa3.1: some code goes here
    Tuple leftTuple=leftchild->next();
    while (rightchild->hasNext())
    {
        Tuple rightTuple=rightchild->next();
        Field* f= const_cast<Field *>(&(rightTuple.getField(p.getField2())));
        auto it=TupleMap->find(f);
        if (it!=TupleMap->end()){
            Tuple tmpT=Tuple(this->getTupleDesc());
            size_t leftsize =leftchild->getTupleDesc().numFields();
            size_t rightsize= rightchild->getTupleDesc().numFields();
            for (int i=0;i<leftsize;i++)
                tmpT.setField(i,&leftTuple.getField(i));
            for (int i=leftsize;i<leftsize+rightsize;i++)
                tmpT.setField(i,&rightTuple.getField(i-leftsize));
            return tmpT;
        }
    }
    rightchild->rewind();
    return std::optional<Tuple>();
}


bool HashEquiJoin::loadMap()
{
    int cnt=0;
    TupleMap->clear();
    while (leftchild->hasNext())
    {
        Tuple t1=leftchild->next();
        Field* f= const_cast<Field *>(&(t1.getField(p.getField1())));
        auto it=TupleMap->find(f);
        if (it==TupleMap->end()){
            auto v=new std::vector<Tuple>();
            (*TupleMap)[f]=v;
        }
        (*TupleMap)[f]->push_back(t1);
        cnt++;
        if (cnt==20000) return true;
        ++leftchild;
    }
    return cnt>0;
}