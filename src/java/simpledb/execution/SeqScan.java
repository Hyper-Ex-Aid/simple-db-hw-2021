package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.DbFile;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private int tableId;

    private DbFileIterator iterator;

    private String tableAlias;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 作为指定事物的一部分，在指定表上创建顺序扫描
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     *            此描述作为其一部分运行的事务
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.transactionId=tid;
        this.tableId=tableid;
        this.tableAlias=tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId=tableid;
        this.tableAlias=tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        iterator=Database.getCatalog().getDatabaseFile(tableId).iterator(transactionId);
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc=Database.getCatalog().getTupleDesc(tableId);
        Type[] typeAr = new Type[tupleDesc.numFields()];
        String[] filedAr = new String[tupleDesc.numFields()];
        for(int i=0;i<tupleDesc.numFields();i++){
            typeAr[i]=tupleDesc.getFieldType(i);
            filedAr[i]=tableAlias+"."+tupleDesc.getFieldName(i);
        }
        TupleDesc result= new TupleDesc(typeAr,filedAr);
        return result;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(iterator==null){
            return false;
        }
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(iterator==null){
            throw new NoSuchElementException("no next tuple");
        }
        Tuple t = iterator.next();
        if(iterator==null){
            throw new NoSuchElementException("no next tuple");
        }
        return t;
    }

    public void close() {
        // some code goes here
        iterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        iterator.rewind();
    }
}
