package GUOFengming.backend.dm.logger;

import GUOFengming.backend.utils.Panic;
import GUOFengming.backend.utils.Parser;
import GUOFengming.common.Error;
import com.google.common.primitives.Bytes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LoggerImpl implements Logger{
    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.fc = fc;
        this.file = raf;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xCheckSum) {
        this.fc = fc;
        this.file = raf;
        this.xChecksum = xCheckSum;
        lock = new ReentrantLock();
    }

    /**
     * 初始化方法，每次打开log文件时执行
     * 读取.log文件的前四个字节得到xCheckSum
     * 判断是否有BadTail，并截断BadTail
     */
    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xCheckSum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xCheckSum;

        checkAndRemoveTail();
    }

    //根据每个日志的校验和求出.log文件的校验和
    private int calCheckSum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    private byte[] internNext() {
        // position 是当前日志文件读到的位置偏移 OF_DATA是当前日志的data的偏移量（data开始的索引）
        //确保当前还有日志待读取
        if (position + OF_DATA >= fileSize) {
            return null;    //读取到 .log 文件末尾，正常结束读取。
        }
        //开始读取当前日志
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        //日志条目可能损坏，拒绝解析该日志，避免错误恢复或程序崩溃。
        if (position + size + OF_DATA > fileSize) {
            return null;
        }

        //开始读取日志的data
        //读取size
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        //校验checkSum
        byte[] log = buf.array();
        //Arrays.copyOfRange(log,OF_DATA,log.length)    当前日志的data部分数据
        //calCheckSum(0, Arrays.copyOfRange(log,OF_DATA,log.length))    用calCheckSum计算当前日志的checkSum
        int checkSum1 = calCheckSum(0, Arrays.copyOfRange(log,OF_DATA,log.length));
        //读取当前日志的checkSum（储存于当前日志的  第4-7字节处）
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log,OF_CHECKSUM,OF_DATA));
        if (checkSum1 != checkSum2) {
            return null;
        }
        //position 更新到下一个日志开头处
        position += log.length;
        return log;
    }

    private void checkAndRemoveTail() {
        //将日志文件的 position 指针重置到文件的第四字节处，从而从头开始检查所有日志。
        rewind();

        int xCheck = 0;
        while(true) {
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calCheckSum(xCheck,log);
        }
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException); //.log文件有有BadTail
        }

        try {
            //截断BadTail（从position开始后面的字节）
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            //将文件指针移动到position处，以便后续写入新日志
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();

    }

    /**
     * 向日志文件写入日志
     * @param data 日志内容
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data); //将data转化为字节数组
        ByteBuffer buf = ByteBuffer.wrap(log);  //包装成buf
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXCheckSum(log);   //更新.log文件的XCheckSum
    }

    /**
     * 更新.log文件的XCheckSum
     * @param log 新写入的data（转化为了字节数组）
     */
    private void updateXCheckSum(byte[] log) {
        this.xChecksum = calCheckSum(this.xChecksum,log);   //计算出新的XCheckSum(int格式)
        try {
            fc.position(0); //指向.log文件的开头
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));  //写入新的XCheckSum(字节数组格式)
            fc.force(false);    //强转写入磁盘（不附加元数据（修改时间等））
        } catch (IOException e){
            Panic.panic(e);
        }
    }

    /**
     * 将data包装为日志格式
     * @param data
     * @return  data转化成的日志格式的字节数组
     */
    private byte[] wrapLog(byte[] data) {
        byte[] checkSum = Parser.int2Byte(calCheckSum(0,data)); //计算checkSum
        byte[] size = Parser.int2Byte(data.length); //计算data的大小
        return Bytes.concat(size,checkSum,data);    //将size、checkSum、data合并成一个字节数组
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log,OF_DATA,log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
