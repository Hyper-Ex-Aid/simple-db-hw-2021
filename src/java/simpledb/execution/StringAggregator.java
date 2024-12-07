package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 * 知道如何在一组StringFields上计算一些聚合
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private Op what;
    //用一个HashMap存储结果
    private final HashMap<Field,Integer> hashMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        if(what!=Op.COUNT){
            throw new IllegalArgumentException("不支持此运算符");
        }
        hashMap = new HashMap<>();
    }

    /**
     * count聚合
     */
    private void countAggregator(Field gbfield,StringField afield){
        if(hashMap.containsKey(gbfield)){
            hashMap.put(gbfield,hashMap.get(gbfield)+1);
        }else {
            hashMap.put(gbfield,1);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //执行具体的聚合操作
        //获取要分组的字段

        Field gbfield = null;
        if(this.gbfield!=NO_GROUPING){
            gbfield=tup.getField(this.gbfield);
        }
        //获取要聚合的字段
        StringField afield = (StringField) tup.getField(this.afield);
        //执行聚合操作
        countAggregator(gbfield,afield);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // some code goes here
        //根据测试，需要返回TupleIterator，构造TupleIterator需要TupleDesc以及Iterator<Tuple>
        //构造TupleDesc
        Type[] types;
        String[] names;
        TupleDesc tupleDesc;
        //构造Iterator<Tuple>
        ArrayList<Tuple> arrayList = new ArrayList<>();
        //分组和没分组的返回方式不一样
        if(this.gbfield==NO_GROUPING){//如果没分组，则返回一个aggregateval
            //构造TupleDesc
            types = new Type[]{Type.INT_TYPE};
            names = new String[]{"aggregateVal"};
            tupleDesc= new TupleDesc(types,names);
            //生成tuple
            Tuple tuple = new Tuple(tupleDesc);
            IntField resultField = new IntField(hashMap.get(null));
            tuple.setField(0,resultField);
            arrayList.add(tuple);
        }else{
            //构造TupleDesc
            types = new Type[]{gbfieldtype,Type.INT_TYPE};
            names = new String[]{"groupVal","aggregateVal"};
            tupleDesc= new TupleDesc(types,names);
            //生成tuple
            for(Field field: hashMap.keySet()){
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0,field);
                IntField resultField = new IntField(hashMap.get(field));
                tuple.setField(1,resultField);
                arrayList.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc, arrayList);
    }

}
