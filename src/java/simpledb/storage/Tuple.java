package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 * Tuple维护有关元组内容的信息。元组具有由TupleDesc对象指定的特定架构，并包含“Field”对象，其中包含每个字段的数据。
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    //元组的类型
    private TupleDesc td;

    //ReocrdId表示元组在磁盘上的位置
    private RecordId rid;

    //元组存储字段集合，字段是不同数据类型实现的接口
    private final Field[] fAr;


    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *         the schema of this tuple. It must be a valid TupleDesc instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.td = td;
        fAr=new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *         the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *         index of the field to change. It must be a valid index.
     * @param f
     *         new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if(i>=0&&i<fAr.length){
            fAr[i]=f;
        }
    }

    /**
     * @param i
     *         field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // some code goes here
        if(i>=0&&i< fAr.length){
            return fAr[i];
        }
        return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuilder result = new StringBuilder();
        for(int i=0;i<fAr.length;i++){
            result.append(fAr[i].toString());
            if(i<fAr.length-1){
                result.append("\\");
            }
        }
        return result.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     * 用来遍历元组所管理的每一个字段
     */
    public Iterator<Field> fields() {
        // some code goes here
        return (Iterator<Field>) Arrays.asList(fAr).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        this.td=td;
    }
}
