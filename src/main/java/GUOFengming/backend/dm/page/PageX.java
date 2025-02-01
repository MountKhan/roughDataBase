package GUOFengming.backend.dm.page;

import GUOFengming.backend.dm.pageCache.PageCache;
import GUOFengming.backend.utils.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节的无符号数 记录的是当前页面 （Page） 数据结尾的下一个字节处的索引，
 * 也就是 新数据应该插入的位置。
 */
public class PageX {

    private static final short OF_FREE = 0;     //pageX的起始位置
    private static final short OF_DATA = 2;     //新数据前的2字节偏移量

    //一页最多写8192 - 2字节的数据（pageX开头偏移2字节）
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw,OF_DATA);
        return raw;
    }

    /**
     * 将ofDate转换为字节数组覆盖掉raw的0-1（2个字节）位置
     * @param raw
     * @param ofData
     */
    private static void setFSO(byte[] raw,short ofData){
        System.arraycopy(Parser.short2Byte(ofData),0,raw,OF_FREE,OF_DATA);
    }

    public static short getFSO(Page pg){
        return getFSO(pg.getData());
    }

    /**
     * 获取FSO:实际也就是当前页面占用的大小
     */
    private static short getFSO(byte[] raw){
        return Parser.parseShort(Arrays.copyOfRange(raw,0,2));
    }

    //将raw插入pg中，返回插入的位置
    public static short insert(Page pg,byte[] raw){
        pg.setDirty(true);
        //获取偏移量
        short offset = getFSO(pg.getData());
        //将raw内的内容写入pg的以offset索引开始处
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
        //更新偏移量FSO
        setFSO(pg.getData(),(short)(offset + raw.length));
        return offset;
    }

    //获取页面的空闲空间大小
    public static int getFreeSpace(Page pg){
        return PageCache.PAGE_SIZE - (int) getFSO(pg.getData());
    }

    /**
     *将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
     *数据库的恢复是乱序的。
     * 有可能恢复的不是字节数组末尾的内容而是中间的内容，
     * 此时如果无条件更新fso相当于认为当前恢复的内容是字节数组末尾的内容，
     * 相当于把这个内容之后的所以内容都格式化了
     * （此时没有真正格式化，只是逻辑上格式化了，如果后续进行插入数据的操作，会把这部分覆盖）
     */
    public static void recoverInsert(Page pg,byte[] raw,short offset){
        pg.setDirty(true);
        //将raw（需要恢复插入的数据）复制到pg的offset处  但此处不更新fso！！！
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
        //获取原先fso
        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length){
            //更新fso
            setFSO(pg.getData(),(short) (offset + raw.length));
        }
    }

    //将raw插入pg的offset位置，不更新offset
    public static void recoverUpdate(Page pg,byte[] raw,short offset){
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
    }

}
