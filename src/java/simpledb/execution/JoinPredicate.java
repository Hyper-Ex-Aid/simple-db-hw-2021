package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 * JoinPredicate使用谓词比较两个元组的字段，JoinPredicate最有可能由Join运算符使用
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private final int field1;
    private final int field2;
    private final Predicate.Op op;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 在两个元组的两个字段上创建一个新的谓词
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        this.field1=field1;
        this.field2=field2;
        this.op=op;

    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 将谓词应用于两个指定的元组，可以通过field的比较方法进行比较
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        return t1.getField(field1).compare(op,t2.getField(field2));
    }
    
    public int getField1()
    {
        // some code goes here
        return field1;
    }
    
    public int getField2()
    {
        // some code goes here
        return field2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return op;
    }
}
