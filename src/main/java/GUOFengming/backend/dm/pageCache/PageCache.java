package GUOFengming.backend.dm.pageCache;

import GUOFengming.backend.dm.page.Page;

public interface PageCache {

    public static final int PAGE_SIZE = 1 << 13;        //设置一个页面的大小是8kb

    int newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);

    void truncateByPgno();
    int getPageNumber();
    void flushPage(Page pg);

}
