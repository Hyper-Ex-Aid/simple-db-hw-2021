package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */

    private File file;
    private TupleDesc tupleDesc;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file=f;
        this.tupleDesc=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        //创建随机访问文件
        try{
            RandomAccessFile randomAccessFile = new RandomAccessFile(file,"r");
            //计算偏移
            //确定这是第几页
            int pgNo = pid.getPageNumber();
            //页数x每页的大小，确定从哪个位置开始读取数据
            int offset = pgNo*BufferPool.getPageSize();
            //将指针定位到读的位置
            randomAccessFile.seek(offset);
            //创建读取数组
            byte[] readBytes = new byte[BufferPool.getPageSize()];
            randomAccessFile.read(readBytes);
            //创建返回的HeapPage,HeapPage需要HeapPageId和byte[]
            //创建HeapPageId
            HeapPageId heapPageId = new HeapPageId(pid.getTableId(),pgNo);
            HeapPage heapPage = new HeapPage(heapPageId,readBytes);
            randomAccessFile.close();
            return heapPage;
        }catch (IOException e){
            e.printStackTrace();
        }
        throw new IllegalArgumentException("this pid is invalid");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //返回一共由多少页，页数等于文件总长度/单页长度，最后结果向下取整
        return (int)Math.floor((double) file.length() /BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    //遍历HeapFile中每个页面的图元
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(this,tid);
    }

    private static final class HeapFileIterator implements DbFileIterator{

        private final HeapFile heapFile;
        private final  TransactionId tid;

        private Iterator<Tuple> it;

        private int whichPage;

        public HeapFileIterator(HeapFile file,TransactionId tid){
            this.heapFile=file;
            this.tid=tid;
        }

        //获取一个页面的图元
        public Iterator<Tuple> getPageTuples(int pageNumber) throws TransactionAbortedException, DbException {
            HeapPageId pid = new HeapPageId(heapFile.getId(),pageNumber);
            HeapPage page =(HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            whichPage = 0;
            it = getPageTuples(whichPage);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(it==null){
                return false;
            }
            if(!it.hasNext()){
                if(whichPage<(heapFile.numPages())){
                    whichPage++;
                    it=getPageTuples(whichPage);
                    return it.hasNext();
                }else{
                    return false;
            }
            }else {
                return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(it==null||!it.hasNext()){
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            it=null;
        }
    }

}

