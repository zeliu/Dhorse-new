package cn.wanda.dataserv.utils.elasticsearch;

import lombok.extern.log4j.Log4j;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.common.unit.TimeValue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;


/**
 * Created by songzhuozhuo on 2015/4/1
 */
@Log4j
public class BulkProcessorHelper {


    public static void flush(BulkProcessor bulkProcessor) {
        try {
            Field field = bulkProcessor.getClass().getDeclaredField("bulkRequest");
            if (field != null) {
                field.setAccessible(true);
                BulkRequest bulkRequest = (BulkRequest) field.get(bulkProcessor);
                if (bulkRequest.numberOfActions() > 0) {
                    Method method = bulkProcessor.getClass().getDeclaredMethod("execute");
                    if (method != null) {
                        method.setAccessible(true);
                        method.invoke(bulkProcessor);
                    }
                }
            }
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        }
    }


    public static boolean waitFor(BulkProcessor bulkProcessor, TimeValue maxWait) {
        Semaphore semaphore = null;
        boolean acquired = false;
        try {
            Field field = bulkProcessor.getClass().getDeclaredField("semaphore");
            if (field != null) {
                field.setAccessible(true);
                Field concurrentField = bulkProcessor.getClass().getDeclaredField("concurrentRequests");
                concurrentField.setAccessible(true);
                int concurrency = concurrentField.getInt(bulkProcessor);
                // concurreny == 1 means there is no concurrency (default start value)
                if (concurrency > 1) {
                    semaphore = (Semaphore) field.get(bulkProcessor);
                    acquired = semaphore.tryAcquire(concurrency, maxWait.getMillis(), TimeUnit.MILLISECONDS);
                    return semaphore.availablePermits() == concurrency;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("interrupted");
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
        } finally {
            if (semaphore != null && acquired) {
                semaphore.release();
            }
        }
        return false;
    }
}
