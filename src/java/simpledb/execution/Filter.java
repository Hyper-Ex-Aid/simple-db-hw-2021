package simpledb.execution;

import simpledb.storage.AbstractDbFileIterator;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 * Filter是一个实现关系选择的运算符
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate p;
    private OpIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read tuples to filter from. 构造函数接受一个要应用的谓词和一个子运算符来读取要过滤的元组
     *
     * @param p
     *         The predicate to filter tuples with 用于过滤元组的谓词
     * @param child
     *         The child operator 子运算符
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the child operator, applying the predicate to them and
     * returning those that pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * AbstractDbIterator。readNext实现。对子运算符中的元组进行迭代，将谓词应用于它们，并返回传递谓词的元组（即predicate。filter返回true的元组）
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        //只能返回一个tuple，如果这个迭代上有多个满足的tuple，不就只能返回一个
        //child.next（）
        while(child.hasNext()){
            Tuple tuple = child.next();
            if(getPredicate().filter(tuple)){
                return tuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
//        return new OpIterator[] { child };
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
//        this.child=children[0];
    }

}
