package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

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
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final ConcurrentHashMap<Integer, Page> pageTable;

    public final ConcurrentHashMap<PageId, LockManager> lockMap = new ConcurrentHashMap<>();

    private final LinkedList<Integer> pageList = new LinkedList<>();

    private int currentPageNums = 0;

    private int numPages;
    /**
     * Lock for the concurrent
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        lock.lock();
        pageTable = new ConcurrentHashMap<>();
        this.numPages = numPages;
        lock.unlock();
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
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        Page page = pageTable.get(pid.hashCode());
        // get the lockManager to lock this page
        LockManager lockManager = this.lockMap.get(pid);
        if (lockManager == null) {
            lockManager = new LockManager();
            this.lockMap.put(pid,lockManager);
        }
        lockManager.tryLock(tid,perm);
        if (page != null) {
            // move this pid to the first
            int index = pageList.indexOf(pid.hashCode());
            pageList.remove(index);
            pageList.addFirst(pid.hashCode());
            return page;
        }
        // get page from the dist file
        if (numPages <= pageList.size()) {
            evictPage();
        }
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        page = dbFile.readPage(pid);
        pageTable.put(pid.hashCode(), page);
        pageList.addFirst(pid.hashCode());
        return page;
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        LockManager lockManager = lockMap.get(pid);
        if (lockManager.check(tid)) {
            lockManager.unlock(tid);
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            flushPages(tid);
            unlockAll(tid);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        LockManager manager = this.lockMap.get(p);
        if (manager != null) {
            return manager.check(tid);
        } else{
            return false;
        }
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            try {
                flushPages(tid);
                for (Page value : pageTable.values()) {
                    if (value.isDirty() == tid) {
                        value.markDirty(false,tid);
                    }
                }
                unlockAll(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            for (Page value : pageTable.values()) {
                if (value.isDirty() == tid) {
                    discardPage(value.getId());
                }
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HeapFile dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);

        List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : pages) {
            LockManager lockManager = this.lockMap.get(page.getId());
            lockManager.tryLock(tid,Permissions.READ_WRITE);
            page.markDirty(true, tid);
            this.pageTable.put(page.getId().hashCode(), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        for (Page page : pages) {
            LockManager lockManager = this.lockMap.get(page.getId());
            lockManager.tryLock(tid,Permissions.READ_WRITE);
            page.markDirty(true, tid);
            this.pageTable.put(page.getId().hashCode(), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Page page : this.pageTable.values()) {
            this.flushPage(page.getId());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        int index = pageList.indexOf(pid.hashCode());
        if (index >= 0 && index < pageList.size()) {
            pageList.remove(index);
        }
        pageTable.remove(pid.hashCode());
        lockMap.remove(pid);
    }


    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = this.pageTable.get(pid.hashCode());
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Page page : this.pageTable.values()) {
            if (page.isDirty() == tid) {
                flushPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // remove this page from the cache
        for (int i = pageList.size() - 1; i >= 0; i--) {
            Integer integer = pageList.get(i);
            TransactionId dirty = pageTable.get(integer).isDirty();
            if (dirty == null) {
                discardPage(pageTable.get(integer).getId());
                return;
            }
        }
        throw new DbException("no enough space");

    }
    private void unlockAll(TransactionId tid) {
        for (LockManager value : this.lockMap.values()) {
            value.unlock(tid);
        }
    }
    class LockManager {
        // 一般一个线程一个事务
        // 一个事务一次最多可以获得多个page的锁，可能会 dead lock
        // 一个page中需要一个读写锁来保证线程安全，首先实现单个锁（读写都锁）
        public AtomicInteger integer; // 2 为 wlock 1 为  rlock，为 0 unlock
        public TransactionId tid;
        public Set<TransactionId> tidSet = new HashSet<>();;

        public LockManager() {
            this.integer = new AtomicInteger(0);
        }

        public synchronized void tryLock(TransactionId tid, Permissions permissions) throws TransactionAbortedException{
            boolean check = check(tid);
            if (permissions.equals(Permissions.READ_ONLY)) {
                long start = System.currentTimeMillis();
                long timeout = new Random().nextInt(333) + 33;

                while (this.integer.get() != 0 && !check) {
                    long now = System.currentTimeMillis();
                    if(now-start > timeout){
                        transactionComplete(tid, false);
                        throw new TransactionAbortedException();
                    }

                }
                this.integer.set(1);
                this.tidSet.add(tid);
            } else {
                // if not 0 , block
                long start = System.currentTimeMillis();
                long timeout = new Random().nextInt(100) + 444;

                while (this.integer.get() != 0 && !check) {
                    long now = System.currentTimeMillis();
                    if(now-start > timeout){
                        transactionComplete(tid, false);
                        throw new TransactionAbortedException();
                    }
                }
                this.integer.set(2);
                this.tid = tid;
            }
        }
        public synchronized void unlock(TransactionId tid) {
            if (this.integer.get() != 0) {
                if (this.integer.get() == 1) {
                    this.tidSet.remove(tid);
                    if (tidSet.size() == 0) {
                        this.integer.set(0);
                    }
                } else {
                    if (this.tid == tid) {
                        tid = null;
                        this.integer.set(0);
                    }
                }
            }
            this.notify();
        }


        public synchronized boolean check(TransactionId tid) {
            return this.tid == tid || this.tidSet.contains(tid);
        }

    }
}
