#include <db/IntegerAggregator.h>
#include <db/IntField.h>
#include <unordered_map>
#include <iostream>
using namespace db;

class IntegerAggregatorIterator : public DbIterator {
private:
    // TODO pa3.2: some code goes here
    int gbfield;
    TupleDesc td;
    std::unordered_map<Field *, int> count;
    std::vector<Tuple> tupleList;
    int tupleit=-1;
public:
    IntegerAggregatorIterator(int gbfield,
                              const TupleDesc &td,
                              const std::unordered_map<Field *, int> &count) {
        // TODO pa3.2: some code goes here
        this->count=count;
        this->gbfield=gbfield;
        this->td=td;
        //tupleList.push_back(Tuple(TupleDesc()));
        if (gbfield==Aggregator::NO_GROUPING)
        {
            Tuple t=Tuple(td);
            auto it =count.find(IntegerAggregator::NO_GROUP_KEY);
            int v=it->second;
            t.setField(0,new IntField(v));
            tupleList.push_back(t);
        }
        else
        {
            for (auto kv:count)
            {
                Field* key=kv.first;
                Tuple t=Tuple(td);
                t.setField(0,key);
                t.setField(1,new IntField(kv.second));
                tupleList.push_back(t);
            }
        }

    }

    void open() override {
        // TODO pa3.2: some code goes here
        tupleit=-1;
    }

    bool hasNext() override {
        // TODO pa3.2: some code goes here
        return tupleit+1 < tupleList.size();
    }

    Tuple next() override {
        // TODO pa3.2: some code goes here
        if (hasNext()) tupleit++;
        return tupleList[tupleit];
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
        tupleit=tupleList.size();
    }
};


IntegerAggregator::IntegerAggregator(int gbfield, std::optional<Types::Type> gbfieldtype, int afield,
                                     Aggregator::Op what) {
    // TODO pa3.2: some code goes here
    this->gbfield=gbfield;
    this->gbfieldtype=gbfieldtype;
    this->afield=afield;
    this->what=what;
    if (gbfield==Aggregator::NO_GROUPING){
        this->vMap[NO_GROUP_KEY]=0;
        this->countMap[NO_GROUP_KEY]=0;
    }

}

void IntegerAggregator::mergeTupleIntoGroup(Tuple *tup) {
    // TODO pa3.2: some code goes here
    Field* key= NO_GROUP_KEY;
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
                    break;
                case Aggregator::Op::MAX:
                    res=std::max(v1,v);
                    break;
                case Aggregator::Op::SUM:
                    res=v1+v;
                    break;
                case Aggregator::Op::AVG:
                    res=v1+v;
                    break;
                case Aggregator::Op::COUNT:
                    res=v1+1;
                    break;
            }
            vMap[key]=res;
        }

    }
}

DbIterator *IntegerAggregator::iterator() const {
    // TODO pa3.2: some code goes here
    std::unordered_map<Field *, int> count;
    auto unwraptype=std::move(*gbfieldtype);
    TupleDesc tdec;

    if (gbfield==Aggregator::NO_GROUPING)
    {
        tdec=TupleDesc(std::vector<Types::Type>{Types::INT_TYPE});
        Tuple *t=new Tuple(tdec);
        auto it=vMap.find(NO_GROUP_KEY);
        int v=it->second;
        auto it2=countMap.find(NO_GROUP_KEY);
        int cnt=it->second;
        if (this->what==Aggregator::Op::AVG) v/=cnt;
        count [NO_GROUP_KEY]=v;
        //std::cout << v << std::endl;
    }
    else
    {
        for (auto kv:vMap)
        {
            tdec=TupleDesc(std::vector<Types::Type>{unwraptype,Types::INT_TYPE});
            Field* key=kv.first;
            int v=kv.second;
            auto it=countMap.find(key);
            int cnt=it->second;
            if (what==Aggregator::Op::AVG) v=v/cnt;
            count[key]=v;
        }
    }

    return new IntegerAggregatorIterator(gbfield,tdec,count);
}
