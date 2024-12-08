package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte[] header;
    //用于存储元组
    final Tuple[] tuples;
    final int numSlots;
    //是否为脏页
    private boolean dirty;
    //事务id
    private TransactionId transactionId;

    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here
        //Math.floor向下取整
        return (int)Math.floor((BufferPool.getPageSize()*8*1.0)/(td.getSize()*8+1));

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
        
        // some code goes here
        //用来存储某个元组是否有效
        return (int)Math.ceil(getNumTuples()*1.0/8);
                 
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * 静态方法，用于生成与空HeapPage对应的字节数组。用于向文件中添加新的空页面。将此方法的结果传递给HeapPage构造函数将创建一个没有有效元组的HeapPage
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     *  从页面中删除指定的元组，应更新相应的标头位，以反映它不再存储在任何页面上
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     *         如果此元组不在此页面上，或者元组槽为空
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        RecordId recordId = t.getRecordId();
        //PageId用来判断此元组是否存储在该页上，如果不在，抛出异常
        PageId pageId = recordId.getPageId();
        if(!pid.equals(pageId)){
            throw new DbException("此元组不在此页面上");
        }
        //tupleNumber用来判断该元组是否存储在，该页的该位置，如果不在，则抛出异常
        int tupleNumber = recordId.getTupleNumber();
        if(tuples[tupleNumber]==null){
            throw new DbException("元组槽为空");
        }else{
            //如果元组确定存储在该页上，并且存储它的元组槽不为空，则将其删除
            //对页眉进行修改
            markSlotUsed(tupleNumber,true);
            //元组槽置空
            tuples[tupleNumber]=null;
        }
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        //如果没有空槽位，则抛出异常
        if(getNumEmptySlots()==0){
            throw new DbException("槽位已满");
        }
        //如果tupledesc不匹配则抛出异常
        if(!td.equals(t.getTupleDesc())){
            throw new DbException("TupleDesc不匹配");
        }
        //如果两个条件都满足，则进行插入操作
        //首先查找空的槽位
        for(int i=0;i<numSlots;i++){
            //如果这个槽位是空的，则将元组插入这个槽位
            if(!isSlotUsed(i)){
                //首先更新tuple的RecordId
                t.setRecordId(new RecordId(pid,i));
                //之后更新页眉
                markSlotUsed(i,false);
                //之后将该tuple存入数组
                tuples[i]=t;
                return;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
        this.dirty=dirty;
        this.transactionId=tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     * 返回上次弄脏此页面的事务的id，如果页面不脏，则返回bull
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        if(dirty){
            return transactionId;
        }
        return null;      
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int numEmptySlots = 0;
        for(int i=0;i<numSlots;i++){
            if(!isSlotUsed(i)){
                numEmptySlots+=1;
            }
        }
        return numEmptySlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        //首先确定第i个插槽的header存储在哪个字节中
        int location1 = i/8;
        //之后确定在第location1个字节的多少位
        int location2 = i%8;
        int bitidx = header[location1];
        int bit = (bitidx>>location2)&1;
        return bit==1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     * Abstraction用于填充或清除此页上的空白
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        // 找到槽位
        int slot = i / 8;
        // 偏移
        int move = i % 8;
        // 掩码
        //例如i%8=1,记录的是第二位，所以要向左移动一位
        byte mask = (byte) (1 << move);
        // 更新槽位
        if(value){
            //如果该标记是被占用的，则更新1为0，用于删除操作
            //该占用为和0做与操作
            header[slot] &= ~mask;
        }else{
            //如果标记未被占用，则更新0为1，用于插入操作
            //该空位与1做或操作
            header[slot] |= mask;
        }

    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        ArrayList<Tuple> filledTuples = new ArrayList<>();
        for(int i=0;i<numSlots;++i){
            if(isSlotUsed(i)){
                filledTuples.add(tuples[i]);
            }
        }
        return filledTuples.iterator();
    }

}

