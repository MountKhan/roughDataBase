package GUOFengming.backend.dm.page;

import GUOFengming.backend.dm.pageCache.PageCache;
import GUOFengming.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * vc ->valid check
 * db启动时给100~107字节处填入一个随即字节，db关闭时将其拷贝到108~115字节处
 * 用于判断上一次数据库是否正常关闭Init
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    //初始化数据库时调用，创建第一页
    public static byte[] InitRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page pg){
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw){
        //在第一页的page对象pg的100~107字节处填入随机字节
        // 并将第一页标记为脏，在数据库正常关闭时写入硬盘文件
        //五个参数 ： 原数组、原数组起始位置、目标数组、目标数组起始位置、复制的长度
        System.arraycopy(RandomUtil.randomBytes(LEN_VC),0,raw,OF_VC,LEN_VC);
    }

    public static void setVcClose(Page pg){
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw){
        //数据库关闭时调用该方法，将第一页的page对象pg的100~107字节处的随机字节
        //拷贝到108~115字节处
        //五个参数 ： 原数组、原数组起始位置、目标数组、目标数组起始位置、复制的长度
        System.arraycopy(raw,OF_VC,raw,OF_VC + LEN_VC,LEN_VC);
    }

    public static boolean checkVc(Page pg){
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw){
        //比较第一页的page对象pg的100~108字节处与108~115字节处内容
        return Arrays.equals(
                Arrays.copyOfRange(raw,OF_VC,OF_VC + LEN_VC)
                ,Arrays.copyOfRange(raw,OF_VC + LEN_VC,OF_VC + LEN_VC + LEN_VC));
    }
}
