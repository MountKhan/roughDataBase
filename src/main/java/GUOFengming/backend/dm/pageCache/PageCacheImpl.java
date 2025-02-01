package GUOFengming.backend.dm.pageCache;

import GUOFengming.backend.common.AbstractCache;
import GUOFengming.backend.dm.page.Page;
import GUOFengming.backend.dm.page.PageImpl;
import GUOFengming.backend.utils.Panic;
import GUOFengming.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;      //记录当前数据库有多少页

    public PageCacheImpl(RandomAccessFile file, FileChannel fc,int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM){
            //当最大缓存资源数小于最小缓存页数时报错
            //因为此时缓存太小，指令难以命中，无法有效发挥缓存的作用
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try{
            length = file.length();
        }catch (IOException e){
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    @Override
    public int newPage(byte[] initData) {

        //数据库创建新页时，pageNumbers+1并赋值给pgno
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno,initData,null);
        flush(pg);
        return pgno;
    }

    @Override
    public Page getPage(int pgno) throws Exception {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void release(Page page) {

    }

    @Override
    public void truncateByPgno() {

    }

    @Override
    public int getPageNumber() {
        return 0;
    }

    @Override
    public void flushPage(Page pg) {

    }

    /**
     *根据pageNumber从数据库文件中读取数据，并包裹成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = PageCacheImpl.pageOffset(pgno);

        //准备一个大小为页面大小（这里是8kb）的缓存区
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);

        fileLock.lock();
        try{
            fc.position(offset);
            //读取一个页大小的数据
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        fileLock.unlock();
        //包装成一个page对象返回
        return new PageImpl(pgno,buf.array(),this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()){
            //如果pg是脏页面，需要刷新到数据库
            flush(pg);
            pg.setDirty(false);
        }
    }

    private void flush(Page pg){
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try{
            //读取page数据
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            //写入内存
            fc.write(buf);
            //保存到硬盘
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }
    }

    private static long pageOffset(int pgno){
        return (long) (pgno - 1) * PAGE_SIZE;      //页面页码从1开始
    }
}
