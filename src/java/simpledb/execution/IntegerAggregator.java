package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 * 知道如何在一组IntFields上计算一些聚合
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    //需要一个集合来存储聚合后的元组集合
    private HashMap<Field,Integer> hashMap;
    //用于辅助记录，执行AVG聚合操作时，每个分组的已经聚合的元组数量
    private HashMap<Field,Integer> avgSupMapCount;
    private HashMap<Field,Integer> avgSupMapSum;


    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     *            元组中按字段分组的从0开始的索引，如果没有分组，则为NO_GROUPING
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     *            按字段分组的类型，如果没有分组，则为null
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     *            元组中聚合字段的从0开始的索引
     * @param what
     *            the aggregation operator
     *            聚合运算符
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        //分组字段
        this.gbfield=gbfield;
        //分组字段的类型
        this.gbfieldtype=gbfieldtype;
        //聚合字段
        this.afield=afield;
        //聚合操作
        this.what=what;
        hashMap=new HashMap<>();
        //如果执行的是AVG聚合操作，则初始化AVG辅助hashmap
        if(what== Op.AVG){
            avgSupMapCount=new HashMap<>();
            avgSupMapSum=new HashMap<>();
        }
    }


    //实现五个聚合操作，分别为COUNT、SUM、AVG、MIN、MAX


    /**
     * sum聚合，计算每个分组聚合的累积
     *
     * @param gbfield 要进行分组的字段
     *
     * @param afield 要进行聚合的字段
     */
    private void sumAggregator(Field gbfield, IntField afield){
        //如果hashMap里面有存储这个分组的聚合结果，则对该结果进行更新
        if(hashMap.containsKey(gbfield)){
            hashMap.put(gbfield, afield.getValue()+hashMap.get(gbfield));
        }else {
            //如果hashMap中不存在这个分组，则将该分组进行添加
            hashMap.put(gbfield,afield.getValue());
        }
    }

    /**
     * count，计算每个分组聚合的数量
     *
     * @param gbfield 要进行分组的字段
     *
     * @param afield 要进行聚合的字段
     */
    private void countAggregator(Field gbfield, IntField afield){
        //如果hashMap里面有存储这个分组的聚合结果，则对该结果进行更新
        if(hashMap.containsKey(gbfield)){
            hashMap.put(gbfield, 1+hashMap.get(gbfield));
        }else {
            //如果hashMap中不存在这个分组，则将该分组进行添加
            hashMap.put(gbfield,1);
        }
    }

    /**
     * count聚合，进行计数
     * //需要记录每个分组下聚合的元组数量
     */
    private void avgAggregator(Field gbfield, IntField afield){
        //如果hashMap里面有存储这个分组的聚合结果，则对该结果进行更新
        if(hashMap.containsKey(gbfield)){
            //更新count和sum
            avgSupMapCount.put(gbfield,1+avgSupMapCount.get(gbfield));
            avgSupMapSum.put(gbfield,afield.getValue()+avgSupMapSum.get(gbfield));
            //用sum/count更新avg
            hashMap.put(gbfield,avgSupMapSum.get(gbfield)/avgSupMapCount.get(gbfield));
        }else {
            //如果hashMap中不存在这个分组，则将该分组进行添加
            hashMap.put(gbfield,afield.getValue());
            avgSupMapCount.put(gbfield,1);
            avgSupMapSum.put(gbfield, afield.getValue());
        }
    }

    /**
     * min聚合，计算每个分组的最小值
     */
    private void minAggregator(Field gbfield, IntField afield){
        int min = afield.getValue();
        if(hashMap.containsKey(gbfield)){
            //如果新聚合的值，比已经记录的最小值还要小，则更新最小值结果
            if(min<hashMap.get(gbfield)){
                hashMap.put(gbfield,min);
            }
        }else{
            //如果该分组一开始不存在，则将min作为初始最小值
            hashMap.put(gbfield,min);
        }
    }

    /**
     * max聚合，计算每个分组的最大值
     */
    private void maxAggregator(Field gbfield, IntField afield){
        int max = afield.getValue();
        if(hashMap.containsKey(gbfield)){
            //如果新聚合的值，比已经记录的最小值还要小，则更新最小值结果
            if(max>hashMap.get(gbfield)){
                hashMap.put(gbfield,max);
            }
        }else{
            //如果该分组一开始不存在，则将min作为初始最小值
            hashMap.put(gbfield,max);
        }
    }


    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 将一个新的元组合并到聚合中，按照构造函数中的指示进行分组
     *
     * 该方法用来执行具体的聚合计算
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     *            包含聚合字段和按字段分组的元组
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //将一个新的元组加入聚合
        //首先获取要进行分组的字段
        //默认分组字段为null
        Field gbfield = null;
        //如果this.gbfield!=NO_GROUPING，则代表需要分组，要获取分组字段
        if(this.gbfield!=NO_GROUPING){
            //获取要分组的字段
            gbfield = tup.getField(this.gbfield);
        }
        //获取要聚合的字段，因为聚合字段都是IntField所以要加强制转换
        IntField afield = (IntField) tup.getField(this.afield);
        //然后根据不同类型的聚合运算符，执行不同的聚合操作
        switch (what){
            case SUM:
                sumAggregator(gbfield,afield);
                break;
            case COUNT:
                countAggregator(gbfield,afield);
                break;
            case AVG:
                avgAggregator(gbfield,afield);
                break;
            case MIN:
                minAggregator(gbfield,afield);
                break;
            case MAX:
                maxAggregator(gbfield,afield);
                break;
            default:
                throw new IllegalArgumentException("不支持当前运算符");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
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
