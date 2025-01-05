package GUOFengming.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import GUOFengming.common.Error;
/**
 * 使用引用技术策略管理缓存，
 * 通过维护一个计数器（通常称为 引用计数）来记录该资源被引用的次数，
 * 并据此决定资源何时可以安全地释放。
 *
 * 核心思想：
 * 每个资源（如对象、文件或内存块）都关联一个引用计数器。
 * 引用计数规则：
 * 1、当资源被引用时（如赋值给一个新变量或传递给函数），引用计数增加。
 * 2、当引用被释放时，引用计数减少。
 * 3、当引用计数变为 0 时，表示资源不再被任何地方使用，可以安全地释放或回收该资源。
 */
public abstract class AbstractCache<T> {
    private HashMap<Long,T> cache;              //实际缓存的数据
    private HashMap<Long,Integer> reference;   //资源引用个数
    private HashMap<Long,Boolean> getting;    //正在被获取的资源

    private int maxResource;                //缓存的最大缓存资源数
    private int count = 0;                 //缓存中的元素个数
    private Lock lock;

    public AbstractCache(int maxResource){
        this.maxResource = maxResource;
        cache = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    protected T get(long key) throws Exception{
        while(true){
            lock.lock();

            if(getting.containsKey(key)){
                //请求的资源正被其他线程获取，睡眠1ms
                lock.unlock();
                try{
                    Thread.sleep(1);
                } catch (InterruptedException e){
                    e.printStackTrace();
                    continue;
                }
                //跳过后续代码重新检查请求的资源是否正被其他线程获取
                continue;
            }

            if(cache.containsKey(key)){
                //资源在缓存中，直接返回
                T obj = cache.get(key);
                //当前资源的引用个数+1
                reference.put(key,reference.get(key) + 1);
                lock.unlock();
                return obj;
            }

            //当前请求的资源不在缓存中时，尝试从数据库获取该资源
            if(maxResource > 0 && count == maxResource){
                lock.unlock();
                throw Error.CacheFullException;
            }

            count ++;
            getting.put(key,true);
            lock.unlock();
            break;
        }

        T obj; //默认为null，用来装从数据库查询返回的内容
        try{
            obj = getForCache(key);
        } catch (Exception e){
            //如果从数据库获取key的资源时出错：
            //count的计数恢复（-1）
            //从正在被获取资源的map中去掉
            lock.lock();
            count --;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        //成功从数据库获取到资源
        lock.lock();
        getting.remove(key);
        //将获取到的资源放入缓存
        cache.put(key,obj);
        reference.put(key,1);
        lock.unlock();

        return obj;
    }

    /**
     * 使用引用计数策略，安全地释放一个缓存
     */
    protected void release(long key){
        lock.lock();
        //引用计数策略，只有当没有其他线程正在引用key对应的资源时才将其从缓存中删除
        //强行删除可能导致其他正在引用这个资源的线程出现空指针异常
        try{
            int ref = reference.get(key) - 1;
            if(ref == 0){
                T obj = cache.get(key);
                releaseForCache(obj);
                reference.remove(key);
                cache.remove(key);
                count --;
            }else{
                reference.put(key,ref);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close(){
        lock.lock();
        try{
            //获取当前cache里所有key对应的obj，挨个删除
            Set<Long> keys = cache.keySet();
            for (Long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                reference.remove(key);
                cache.remove(key);
            }
        }finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的回写行为
     */
    protected abstract void releaseForCache(T obj);
}
