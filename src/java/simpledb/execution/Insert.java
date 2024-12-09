package simpledb.execution;

import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 * 将从子运算符读取的元组插入构造函数中指定的tableId中
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private  TransactionId transactionId;
    private OpIterator opIterator;
    private int tableId;
    private TupleDesc tupleDesc;
    private boolean inserted;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     *            运行插入的事务
     * @param child
     *            The child operator from which to read tuples to be inserted.
     *            从中读取要插入的元组的子运算符
     * @param tableId
     *            The table in which to insert tuples.
     *            插入元组的表
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     *             如果子项的TupleDesc与我们要插入的表不同
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        //如果子项的TupleDesc与我们要插入的表的TupleDesc不同，则抛出异常
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))){
            throw new DbException("子项的TupleDesc与我们要插入的表的TupleDesc不同");
        }
        this.transactionId=t;
        this.opIterator=child;
        this.tableId=tableId;
        this.tupleDesc=new TupleDesc(new Type[]{Type.INT_TYPE});
        this.inserted=false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        opIterator.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        opIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        opIterator.rewind();
        inserted=false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 将从子级读取的元组插入到构造函数指定的tableId中。它返回一个单字段元组，其中包含插入的记录数。插入应通过BufferPool传递
     * BufferPool的实例可以通过DataBase。getBufferPool获得。请注意，插入之前不需要检查特定元组是否是重复的。
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     *         一个包含插入记录数的但字段元组，如果多次调用，则为null
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(!inserted){
            inserted=true;
            //记录插入操作数
            int count = 0;
            //如果存在下一个元组
            while (opIterator.hasNext()){
                //获取要插入的元组
                try{
                    Database.getBufferPool().insertTuple(transactionId,tableId,opIterator.next());
                    count++;
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            //返回一个元组，这个元组记录了进行了多少次插入操作
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0,new IntField(count));
            return tuple;
        }
        //如果进行了多次插入操作，则为null
        return null;
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
