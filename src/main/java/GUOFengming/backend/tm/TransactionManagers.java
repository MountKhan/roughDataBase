package GUOFengming.backend.tm;

import GUOFengming.backend.utils.Panic;
import GUOFengming.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManagers {
    long begin();
    void commit(long xid);
    void abort(long xid);
    boolean isActive(long xid);
    boolean isAborted(long xid);
    boolean isCommitted(long xid);
    void close();

    //初始化.xid文件
    public static TransactionManagersImpl create(String path){
        //创建 #{path}.xid文件
        File f = new File(path + TransactionManagersImpl.XID_SUFFIX);
        try{
            //如果这个路径的文件已经存在，返回false，否则创建文件并返回true
            if(!f.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e){
            Panic.panic(e);
        }

        //检测读写权限
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;
        try{
            //以读写模式打开文件
            raf = new RandomAccessFile(f,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e){
            Panic.panic(e);
        }

        //写空XID文件头
        //包装一个八字节的数组为Byte Buffer
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagersImpl.LEN_XID_HEADER_LENGTH]);
        try{
            //定位到头部
            fc.position(0);
            //写入长度为8字节的文件头
            fc.write(buf);
        } catch (Exception e){
            Panic.panic(e);
        }

        return new TransactionManagersImpl(raf,fc);
    }

    public static TransactionManagersImpl open(String path){
        File f = new File(path + TransactionManagersImpl.XID_SUFFIX);
        if(!f.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }

        RandomAccessFile raf = null;
        FileChannel fc = null;

        try{
            raf = new RandomAccessFile(f,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e){
            Panic.panic(Error.FieldNotFoundException);
        }

        return new TransactionManagersImpl(raf,fc);
    }

}
