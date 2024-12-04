package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 * 谓词将元组与指定的字段值进行比较
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private int fieldNumber;
    private Op op;
    private Field operand;

    /** Constants used for return codes in Field.compare
     * Field.compare中用于返回代码的常量
     * */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 通过整数值访问操作的接口，便于命令行使用
         * 
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }
    
    /**
     * Constructor.
     * 
     * @param field
     *            field number of passed in tuples to compare against.
     *            元组中的第几个字段的index
     *            要比较的传入元组的字段数
     * @param op
     *            operation to use for comparison
     *            用于比较的操作
     * @param operand
     *            field value to compare passed in tuples to
     *            用于比较以元组形式传递的字段值
     */
    public Predicate(int field, Op op, Field operand) {
        // some code goes here
        this.fieldNumber=field;
        this.op=op;
        this.operand=operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        // some code goes here
        return fieldNumber;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        // some code goes here
        return op;
    }
    
    /**
     * @return the operand
     */
    public Field getOperand()
    {
        // some code goes here
        return operand;
    }
    
    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 使用构造函数中特定的运算符，将构造函数中指定的t的字段号与构造函数中所指定的操作数字段进行比较。可以通过Field的比较方法进行比较
     * 
     * @param t
     *            The tuple to compare against
     *            要比较的元组
     * @return true if the comparison is true, false otherwise.
     * 如果比较为真，则为true，否则为false
     */
    public boolean filter(Tuple t) {
        // some code goes here
//        return operand.compare(op,t.getField(fieldNumber));
        return t.getField(getField()).compare(getOp(),getOperand());
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
        // some code goes here
        String s = "f = "+getField()+" op = "+op.toString()+" operand = "+operand.toString();
        return s;
    }
}
