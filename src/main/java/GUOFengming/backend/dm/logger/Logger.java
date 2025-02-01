package GUOFengming.backend.dm.logger;

import GUOFengming.backend.utils.Panic;
import GUOFengming.backend.utils.Parser;
import GUOFengming.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * log文件格式：[XChecksum][Log1][Log2][Log3]...[LogN][BadTail]
 * XChecksum 是一个四字节的整数，是对后续所有日志计算的校验和。
 * Log1 ~ LogN 是常规的日志数据。
 * BadTail 是在数据库崩溃时，没有来得及写完的日志数据，这个 BadTail 不一定存在。
 *
 * 单条Log格式:[Size][Checksum][Data]
 * Size 是一个四字节整数，标识了 Data 段的字节数。
 * Checksum 是该条日志的校验和。
 */
public interface Logger {
    void log(byte[] data);
    void truncate(long x) throws Exception;
    byte[] next();
    void rewind();
    void close();

    public static Logger create(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new LoggerImpl(raf, fc, 0);
    }

    public static Logger open(String path) {
        File f = new File(path+LoggerImpl.LOG_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        LoggerImpl lg = new LoggerImpl(raf, fc);
        lg.init();

        return lg;
    }
}
