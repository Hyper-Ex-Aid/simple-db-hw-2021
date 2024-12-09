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
 * HeapFile是DbFile的一种实现，用于存储一组元组，且元组的顺序不固定。元组存储在页面中，每个页面的大小是固定的，整个文件仅仅是这些页面的集合。
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

    private final File file;
    private final TupleDesc tupleDesc;
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
        int tableId = pid.getTableId();
        //确定这是第几页
        int pgNo = pid.getPageNumber();
        RandomAccessFile randomAccessFile =null;
        //创建随机访问文件
        try{
            randomAccessFile = new RandomAccessFile(file,"r");
            if((long) (pgNo + 1) *BufferPool.getPageSize()>randomAccessFile.length()){
                randomAccessFile.close();
                throw new IllegalArgumentException("剩余的数据不足一页");
            }
            //创建读取数组
            byte[] readBytes = new byte[BufferPool.getPageSize()];
            //计算偏移
            //页数x每页的大小，确定从哪个位置开始读取数据
            //将指针定位到读的位置
            randomAccessFile.seek((long) pgNo *BufferPool.getPageSize());
            int read=randomAccessFile.read(readBytes,0,BufferPool.getPageSize());
            if(read!=BufferPool.getPageSize()){
                throw new IllegalArgumentException("数据未读满");
            }
            //创建返回的HeapPage,HeapPage需要HeapPageId和byte[]
            //创建HeapPageId
            HeapPageId heapPageId = new HeapPageId(pid.getTableId(),pgNo);
            HeapPage heapPage = new HeapPage(heapPageId,readBytes);
            return heapPage;
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try{
                randomAccessFile.close();
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException("this pid is invalid");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageId = page.getId().getPageNumber();
        if(pageId>numPages()){
            throw new IllegalArgumentException();
        }
        RandomAccessFile f = new RandomAccessFile(file,"rw");
        f.seek(pageId*BufferPool.getPageSize());
        f.write(page.getPageData());
        f.close();
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
        // not necessary for lab1
        ArrayList<Page> arrayList = new ArrayList<>();
        //查询现有页，如果有的页有空位，则将t插入
        for(int i=0;i<numPages();i++){
            //根据表id和该页存储在表的位置，查找该页
            HeapPageId heapPageId = new HeapPageId(getId(),i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
            if(heapPage.getNumEmptySlots()!=0){
                //如果该页为空的槽位不为0，则将原则插入到该页
                heapPage.insertTuple(t);
                arrayList.add(heapPage);
                return arrayList;
            }
        }
        //如果现有页都没有空位，则新创建一页，并插入
        //首先已追加模式，创建输出流
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file,true));
        //生成一页大小的空数据
        byte[] emptyPage = HeapPage.createEmptyPageData();
        //然后将空数据，输出到文件中，所以文件的大小就被扩充了
        outputStream.write(emptyPage);
        outputStream.close();

        //此时创建新的HeapPageId，页面的存储位置为最后一页，此时numPages()已经更新
        HeapPageId heapPageId = new HeapPageId(getId(),numPages()-1);
        //之后从BufferPool中读取该页面
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
        heapPage.insertTuple(t);
        arrayList.add(heapPage);
        return arrayList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        //必须使用BufferPool.getPage()方法访问页面
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        //将删除完成的存入ArrayList中并返回
        ArrayList<Page> arrayList = new ArrayList<>();
        arrayList.add(page);
        return arrayList;
    }

    // see DbFile.java for javadocs
    //遍历HeapFile中每个页面的图元
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

        return new HeapFileIterator(this,tid);
    }

    private static final class HeapFileIterator implements DbFileIterator{

        private final HeapFile heapFile;
        private final TransactionId tid;

        private Iterator<Tuple> it;

        private int whichPage;

        public HeapFileIterator(HeapFile file,TransactionId tid){
            this.heapFile=file;
            this.tid=tid;
        }

        //获取一个页面的图元
        public Iterator<Tuple> getPageTuples(int pageNumber) throws TransactionAbortedException, DbException {
            //如果该页是有效的
            if(pageNumber>=0&&pageNumber<heapFile.numPages()){
                HeapPageId pid = new HeapPageId(heapFile.getId(),pageNumber);
                HeapPage page =(HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_ONLY);
                return page.iterator();
            }else{
                throw new DbException("无效pageNumber");
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            whichPage = 0;
            //
            it = getPageTuples(whichPage);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            //如果是一个空迭代器的话，其本身不为空，但是其it.next为空，it.hasNext()为false
            // 如果迭代器为空，或者遍历结束，则检查其下一页
            if (it == null || !it.hasNext()) {
                // 如果当前页已经没有元素，检查是否有下一页
                while (whichPage < heapFile.numPages() - 1) {
                    // 跳转到下一页
                    whichPage++;
                    // 获取新的页面迭代器
                    it = getPageTuples(whichPage);
                    // 如果新迭代器有内容，则返回 true
                    if(it.hasNext()){
                        return true;
                    }
                }
                // 如果没有更多的页面或者迭代器已经没有元素，返回 false
                return false;
            }
            return true;
        }


        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(!it.hasNext()){
                throw new NoSuchElementException("没有下一个元组了");
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

