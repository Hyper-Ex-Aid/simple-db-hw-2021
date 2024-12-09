package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    public static int numPages = DEFAULT_PAGES;

    public static ConcurrentHashMap<Integer,Page> pageHashMap;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        setNumPages(numPages);
        pageHashMap = new ConcurrentHashMap<>();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    public static  void setNumPages(int numPages){BufferPool.numPages=numPages;}


    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException{
        // some code goes here
        //如果缓存池中能查找到检索到的页面，则将其返回
//        if(pageHashMap.containsKey(pid.hashCode())){
//            return pageHashMap.get(pid);
//        }else{
//            //
//            //如果不存在，则先请求该页，将其添加到缓存池中并返回
//            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
//            Page page = dbFile.readPage(pid);
//            //如果此时缓存池满了，抛出异常DbException
////            if(pageHashMap.size()>numPages){
////                throw new DbException("缓存池已满");
////            }
//            //如果缓存池未满，则将读取到也面添加到缓存池，并返回
//            pageHashMap.put(pid.hashCode(),page);
//            return pageHashMap.get(pid);
//        }
        //如果缓冲池中不存在某页面，则先将该页面添加到缓冲池
        if(!pageHashMap.containsKey(pid.hashCode())){
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            //如果缓冲池中页面的数量，超出了缓冲池的页面数量限制，则将一个页面驱逐
            if(pageHashMap.size()>=numPages){
//                ArrayList<Map.Entry<Integer,Page>> list = new ArrayList<>(pageHashMap.entrySet());
//                Random random = new Random();
//                Map.Entry<Integer,Page> entry = list.get(random.nextInt(list.size()));
//                pageHashMap.remove(entry.getKey());
                evictPage();
            }
            pageHashMap.put(pid.hashCode(),page);
        }
        return pageHashMap.get(pid.hashCode());
    }


    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        //首先找到要删除的文件
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        //之后删除文件
        //将被脏化的页面，添加到现有的缓存中
        for (Page page:heapFile.insertTuple(tid,t)){
            page.markDirty(true,tid);
            pageHashMap.put(page.getId().hashCode(),page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages.
     * 通过调用页面的markDirty位，标记操作所脏化的任何页面位脏页面，并将任何被脏化的页面的版本添加到缓存中（替换这些页面的现有版本），以确保未来的请求能够看到最新的页面
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        //将被脏化的页面，添加到现有的缓存中
        for (Page page:heapFile.deleteTuple(tid,t)){
            page.markDirty(true,tid);
            pageHashMap.put(page.getId().hashCode(),page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     * 将所有脏页刷新到磁盘。
     * 注意：使用这个方法时要小心，它会将脏数据写入磁盘，因此如果在NO STEAL模式下运行，会破坏simpledb
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(Page page:pageHashMap.values()){
            if(page.isDirty()!=null){
                flushPage(page.getId());
                page.markDirty(false,null);
            }
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageHashMap.remove(pid.hashCode());

    }

    /**
     * Flushes a certain page to disk
     * 将某个页面刷新到磁盘
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        //获取该页
        Page page = pageHashMap.get(pid.hashCode());
        //将该页刷新到磁盘
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Map.Entry<Integer,Page>> list = new ArrayList<>(pageHashMap.entrySet());
        Random random = new Random();
        Map.Entry<Integer,Page> entry = list.get(random.nextInt(list.size()));
        try {
            flushPage(entry.getValue().getId());
        }catch (IOException e){
            e.printStackTrace();
        }
        pageHashMap.remove(entry.getKey());
    }

}
