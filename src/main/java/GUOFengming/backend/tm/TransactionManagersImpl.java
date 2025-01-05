package GUOFengming.backend.tm;

import GUOFengming.backend.utils.Panic;
import GUOFengming.common.Error;
import GUOFengming.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagersImpl implements TransactionManagers {

    //head length of xid file
    static final int LEN_XID_HEADER_LENGTH = 8;

    //length of a transaction
    private static final int XID_FIELD_SIZE = 1;

    //status of transaction
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    //super transaction,which 's status is always committed
    public static final long SUPER_XID = 0;

    static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;

    TransactionManagersImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * verify that whether the .xid file is valid or not
     * read xid_counter of XID_FILE_HEADER,and compare the length
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);
        }

        //文件长度小于8，说明文件不合法
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);

        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    //根据事务xid计算这个事务在.xid文件中的位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    //更新事务的状态
    private void updateXID(long xid, byte status) {

        //计算事务的位置
        long offset = getXidPosition(xid);
        //将status写入一个字节的数组
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        //将tmp包装成一个 ByteBuffer 对象
        ByteBuffer buf = ByteBuffer.wrap(tmp);

        try {
            //定位到这个事务的位置
            fc.position(offset);
            //更新这个事务的状态
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            //FileChannel.force()将数据写入硬盘
            //参数
            // true：同时将文件的元数据（如文件大小、权限、最后修改时间等）写入磁盘。
            // false：只会强制将文件内容（数据部分）写入磁盘，而不包括元数据。
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    //将XID加一，并更新XID Header
    private void incrXIDCounter() {
        xidCounter++;
        //将xidCounter包装成一个Byte Buffer对象
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));

        try {
            //.xid文件的头8个字节记录 XID counter
            fc.position(0);
            //写入（覆盖原先的头8个字节）
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        try {
            //写入硬盘
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }


    //开始一个事务，返回XID
    @Override
    public long begin() {
        //上锁
        counterLock.lock();
        try {
            //得到当前事务的编号（xidCounter + 1）
            long xid = xidCounter + 1;
            //更新这个事务的状态
            updateXID(xid, FIELD_TRAN_ACTIVE);
            //修改.xid文件的头8个字节（事务总数量）
            incrXIDCounter();
            return xid;
        } finally {
            //解锁
            counterLock.unlock();
        }
    }

    //提交XID事务
    @Override
    public void commit(long xid) {
        //更新事务状态
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    //回滚XID事务
    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    //判断事务是否处于active状态
    @Override
    public boolean isActive(long xid) {
        //超级事务永远处于committed状态
        if (xid == SUPER_XID) {
            return false;
        }

        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isAborted(long xid) {
        //超级事务永远处于committed状态
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid,FIELD_TRAN_ABORTED);
    }

    @Override
    public boolean isCommitted(long xid) {
        //超级事务永远处于committed状态
        if (xid == SUPER_XID) {
            return false;
        }
        return checkXID(xid,FIELD_TRAN_COMMITTED);
    }


    @Override
    public void close() {
        try{
            fc.close();
            file.close();
        } catch (IOException e){
            Panic.panic(e);
        }
    }

    private boolean checkXID(long xid, byte status) {
        //找到这个事务在.xid文件的位置
        long offset = getXidPosition(xid);
        //包装一个1字节的Byte Buffer
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            //定位到事务的位置
            fc.position(offset);
            //读1字节
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        //将这个字节的内容与输入的status比较
//        return buf.array()[0] == status;
        byte[] array = buf.array();
        return array[0] == status;

    }
}
