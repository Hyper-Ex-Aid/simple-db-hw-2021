package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private  TransactionId transactionId;
    private OpIterator opIterator;
    private TupleDesc tupleDesc;
    private boolean deleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 构造函数指定此删除所属的事物以及要从中读取的子事物
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.transactionId=t;
        this.opIterator=child;
        this.tupleDesc=new TupleDesc(new Type[]{Type.INT_TYPE});
        this.deleted=false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
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
        deleted=false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 从子运算符读取元组时删除元组，删除时通过缓冲池处理的
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(!deleted){
            deleted=true;
            //记录删除的元组数
            int count=0;
            //执行删除操作
            while (opIterator.hasNext()){
                try{
                    Database.getBufferPool().deleteTuple(transactionId,opIterator.next());
                    count++;
                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0,new IntField(count));
            return tuple;
        }
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
