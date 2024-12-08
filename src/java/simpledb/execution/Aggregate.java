package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    //用于遍历操作的元组
    private final OpIterator child;
    //聚合的字段
    private final int afield;
    //分组的字段
    private final int gfield;
    //聚合的操作符
    private final Aggregator.Op aop;
    //进行聚合操作的类
    private Aggregator aggregator;
    private TupleDesc tupleDesc;
    private OpIterator opIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child= child;
        this.afield=afield;
        this.gfield=gfield;
        this.aop=aop;
        //判断是否分组
        Type gfieldtype = gfield==-1?null:child.getTupleDesc().getFieldType(gfield);
        //根据要聚合字段的类型，去初始化Aggregator到底是IntegerAggregator还是StringAggregator
        if(child.getTupleDesc().getFieldType(afield)==Type.INT_TYPE){
            //如果是整数聚合，初始化为IntegerAggregator
            aggregator = new IntegerAggregator(gfield,gfieldtype,afield,aop);
        }else{
            //否则，则为StringAggregator
            aggregator = new StringAggregator(gfield,gfieldtype,afield,aop);
        }
        // 组建 TupleDesc
        List<Type> typeList = new ArrayList<>();
        List<String> nameList = new ArrayList<>();

        if(gfieldtype != null){
            typeList.add(gfieldtype);
            nameList.add(child.getTupleDesc().getFieldName(gfield));
        }

        typeList.add(child.getTupleDesc().getFieldType(afield));
        nameList.add(child.getTupleDesc().getFieldName(afield));

        if(aop.equals(Aggregator.Op.SUM_COUNT)){
            typeList.add(Type.INT_TYPE);
            nameList.add("COUNT");
        }
        this.tupleDesc = new TupleDesc(typeList.toArray(new Type[typeList.size()]), nameList.toArray(new String[nameList.size()]));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        while (child.hasNext()){
            aggregator.mergeTupleIntoGroup(child.next());
        }
        opIterator = aggregator.iterator();
        opIterator.open();
        //使父类状态保持一致
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //返回聚合之后的tuple
        if(opIterator.hasNext()){
            return opIterator.next();
        }
        return null;
    }

    //重置迭代器
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 返回此聚合的TupleDesc，如果没有按字段分组，则将有一个字段-聚合列。如果有字段分组，则第一个字段将是字段分组，第二个字段将是聚合值列
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //封装初始化变量
        return this.tupleDesc;
    }

    public void close() {
        // some code goes here
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}
