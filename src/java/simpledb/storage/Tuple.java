package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private RecordId rid;
    private Field[] fAr;

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
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        String result = "";
        for(int i=0;i<fAr.length;i++){
            result+=fAr[i].toString();
            if(i<fAr.length-1){
                result=result+"\\";
            }
        }
        return result;
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // some code goes here
        Iterator<Field> iterator = new Iterator<Field>() {
            int cursor =0;//指示当前位置

            @Override
            public boolean hasNext() {
                return (cursor<fAr.length);
            }

            @Override
            public Field next() {
                if(cursor+1<fAr.length){
                    return fAr[cursor+1];
                }
                return null;
            }
        };
        return iterator;
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        this.td=td;
    }
}
