package GUOFengming.backend.dm.page;

import GUOFengming.backend.dm.pageCache.PageCache;
import GUOFengming.backend.utils.Parser;

import java.awt.event.PaintEvent;
import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {

    private static final short OF_FREE = 0;     //pageX的起始位置
    private static final short OF_DATA = 2;     //新数据前的2字节偏移量

    //一页最多写8192字节 - 2字节的数据（pageX开头偏移2字节）
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw,OF_DATA);
        return raw;
    }

    private static void setFSO(byte[] raw,short ofData){
        System.arraycopy(Parser.short2Byte(ofData),0,raw,OF_FREE,OF_DATA);
    }

    public static short getFSO(Page pg){
        return getFSO(pg.getDate());
    }

    private static short getFSO(byte[] raw){
        return Parser.parseShort(Arrays.copyOfRange(raw,0,2));
    }

    //将raw插入pg中，返回插入的位置
    public static short insert(Page pg,byte[] raw){
        pg.setDirty(true);
        //获取偏移量
        short offset = getFSO(pg.getDate());
        //将raw内的内容写入pg的以offset索引开始处
        System.arraycopy(raw,0,pg.getDate(),offset,raw.length);
        //更新偏移量FSO
        setFSO(pg.getDate(),(short)(offset + raw.length));
        return offset;
    }

    //获取页面的空闲空间大小
    public static int getFreeSpace(Page pg){
        return PageCache.PAGE_SIZE - (int) getFSO(pg.getDate());
    }

    //将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page pg,byte[] raw,short offset){
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getDate(),offset,raw.length);

        short rawFSO = getFSO(pg.getDate());
        if(rawFSO < offset + raw.length){
            setFSO(pg.getDate(),(short) (offset + raw.length));
        }
    }

    //将raw插入pg的offset位置，不更新offset
    public static void recoverUpdate(Page pg,byte[] raw,short offset){
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getDate(),offset,raw.length);
    }

}
