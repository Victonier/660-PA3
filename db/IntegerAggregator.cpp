#include <db/IntegerAggregator.h>
#include <db/IntField.h>
#include <unordered_map>
using namespace db;

class IntegerAggregatorIterator : public DbIterator {
private:
    // TODO pa3.2: some code goes here
    int gbfield;
    TupleDesc td;
    std::unordered_map<Field *, int> count;
    std::vector<Tuple> tupleList;
    std::vector<Tuple>::iterator it;
public:
    IntegerAggregatorIterator(int gbfield,
                              const TupleDesc &td,
                              const std::unordered_map<Field *, int> &count) {
        // TODO pa3.2: some code goes here
        this->count=count;
        this->gbfield=gbfield;
        this->td=td;
        for (auto kv:count)
        {
            Field* key=kv.first;
            Tuple t=Tuple(td);
            t.setField(0,key);
            t.setField(1,new IntField(kv.second));
            tupleList.push_back(t);
        }
        it=tupleList.begin();
    }

    void open() override {
        // TODO pa3.2: some code goes here
        it=tupleList.begin();
    }

    bool hasNext() override {
        // TODO pa3.2: some code goes here
        return it!=tupleList.end();
    }

    Tuple next() override {
        // TODO pa3.2: some code goes here
        ++it;
        return *it;
    }

    void rewind() override {
        // TODO pa3.2: some code goes here
        close();
        open();
    }

    const TupleDesc &getTupleDesc() const override {
        // TODO pa3.2: some code goes here
        return td;
    }

    void close() override {
        // TODO pa3.2: some code goes here
        it=tupleList.end();
    }
};


IntegerAggregator::IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield,
                                     Aggregator::Op what) {
    // TODO pa3.2: some code goes here
    this->gbfield=gbfield;
    this->gbfieldtype=gbfieldtype;
    this->afield=afield;
    this->what=what;
}

void IntegerAggregator::mergeTupleIntoGroup(Tuple *tup) {
    // TODO pa3.2: some code goes here
    Field* key= nullptr;
    if (gbfield!=Aggregator::NO_GROUPING)
        key=const_cast<Field*>(&(tup->getField(this->gbfield)));
    IntField *intfield=(IntField*) &tup->getField(afield);
    int v=intfield->getValue();
    if (this->gbfield==Aggregator::NO_GROUPING || tup->getTupleDesc().getFieldType(gbfield)==this->gbfieldtype)
    {
        if (countMap.find(key)==countMap.end()) {
            countMap[key] = 1;
            vMap[key]=v;
        }
        else{
            countMap[key]=countMap[key]+1;
            int v1=vMap[key];
            int res=0;
            switch(what){
                case Aggregator::Op::MIN:
                    res=std::min(v1,v);
                case Aggregator::Op::MAX:
                    res=std::max(v1,v);
                case Aggregator::Op::SUM:
                    res=v1+v;
                case Aggregator::Op::AVG:
                    res=v1+v;
                case Aggregator::Op::COUNT:
                    res=v1+1;
            }
            vMap[key]=res;
        }

    }
}

DbIterator *IntegerAggregator::iterator() const {
    // TODO pa3.2: some code goes here
    std::unordered_map<Field *, int> count;
    auto unwraptype=std::move(*gbfieldtype);
    TupleDesc tdec=TupleDesc(std::vector<Types::Type>{unwraptype,Types::INT_TYPE});
    for (auto kv:vMap)
    {
        Field* key=kv.first;
        int v=kv.second;
        auto it=countMap.find(key);
        int cnt=it->second;
        if (what==Aggregator::Op::AVG) v=v/cnt;
        count[key]=v;
    }
    return new IntegerAggregatorIterator(gbfield,tdec,count);
}
