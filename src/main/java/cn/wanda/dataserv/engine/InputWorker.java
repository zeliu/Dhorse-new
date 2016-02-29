package cn.wanda.dataserv.engine;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import cn.wanda.dataserv.storage.StorageManager;
import cn.wanda.dataserv.storage.mem.DefaultStorageManager;
import lombok.extern.log4j.Log4j;

import cn.wanda.dataserv.config.DPumpConfig;
import cn.wanda.dataserv.config.InputConfig;
import cn.wanda.dataserv.utils.Printable;

/**
 * input executor
 *
 * @author songqian
 */
@Log4j
public class InputWorker implements Printable {

    private static final int MAX_CONCURRENCY = 64;

    public static Map<String, Throwable> INPUT_ERROR = new ConcurrentHashMap<String, Throwable>();

    private DPumpConfig config;
    private StorageManager storageManager;
    private Map<String, ThreadPoolExecutor> inputPoolMap;

    private boolean inputFinish = false;

    public InputWorker(DPumpConfig config, StorageManager sm) {
        this.config = config;
        this.storageManager = sm;
    }

    /**
     * init input task and run input task in thread pool
     */
    public void start() {
        log.info("start Input Worker");
        log.info("calculate Input Config...");

        List<InputConfig> inputConfigList = config.getSource();

        //input has split by parser
        log.info("DPump splits Input Task into " + inputConfigList.size() + " sub task.");

        //calc concurrency
        int concurrency = calcConcurrency(inputConfigList);

        log.info("prepare Input...");

        inputPoolMap = new HashMap<String, ThreadPoolExecutor>();

        log.info("Start Input Task");
        for (InputConfig param : inputConfigList) {
            //用各个inputConfig生成inputTask，并放入线程池执行
            log.info("Start Input Task, id: " + param.getId());
            ThreadPoolExecutor inputPool = this.inputPoolMap.get(param.getId());
            if (inputPool == null) {
                //init inputTask threadpool
                inputPool = new ThreadPoolExecutor(concurrency, concurrency, 1L, TimeUnit.SECONDS,
                        new LinkedBlockingQueue<Runnable>());
                inputPool.prestartAllCoreThreads();
                this.inputPoolMap.put(param.getId(), inputPool);
            }
            InputTask inputTask = new InputTask(param, this.config.getRuntime(), storageManager);

            inputPool.execute(inputTask);
        }
    }

    /**
     * 动态根据输入配置数量决定并发数目
     *
     * @param inputConfigList
     * @return
     */
    private int calcConcurrency(List<InputConfig> inputConfigList) {
        int concurrency = config.getRuntime().getInputConcurrency();

        concurrency = Math.min(concurrency,
                inputConfigList.size());
        if (concurrency <= 0) {
            concurrency = Math.min(MAX_CONCURRENCY,
                    inputConfigList.size());
            log.info(String.format(
                    "input concurrency set to auto (%d).",
                    concurrency));
        }
        if (concurrency > MAX_CONCURRENCY) {
            log.info(String.format(
                    "input concurrency set to be %d, make sure it must be between [%d, %d] .",
                    concurrency, 1, MAX_CONCURRENCY));
            concurrency = MAX_CONCURRENCY;
        }
        config.getRuntime().setInputConcurrency(concurrency);
        return concurrency;
    }

    public void shutdown() {
        for (ThreadPoolExecutor inputPool : inputPoolMap.values()) {
            inputPool.shutdown();
        }
    }

    public void shutdownNow() throws InterruptedException {
        for (ThreadPoolExecutor inputPool : inputPoolMap.values()) {
            inputPool.shutdownNow();
            inputPool.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    /**
     * remove finished input task and return if all input finished
     *
     * @return if all input finished
     */
    public boolean end() {

        //if finish already
        if (inputFinish) {
            return inputFinish;
        }

        inputFinish = true;

        ThreadPoolExecutor all = this.inputPoolMap.get(DefaultStorageManager.INPUT_ID_ALL);

        boolean allFinish = true;
        if (all != null) {
            allFinish = all.isTerminated();
        }

        for (Iterator<Entry<String, ThreadPoolExecutor>> i = inputPoolMap.entrySet().iterator(); i.hasNext(); ) {
            // 依次处理各id对应线程池，若id对应storage不会再放入记录，则关闭input
            Entry<String, ThreadPoolExecutor> entry = i.next();
            if (!entry.getValue().isTerminated()) {
                inputFinish = false;
            } else {
                if (allFinish && !(DefaultStorageManager.INPUT_ID_ALL.equals(entry.getKey()))) {
                    log.info(String.format("Input Task %s finished.", entry.getKey()));
                    //若该input id对应storage不会再放入记录，则关闭input
                    storageManager.closeInput(entry.getKey());
                    i.remove();
                }
            }
        }

        if (inputFinish) {
            storageManager.closeInput();
        }
        return inputFinish;
    }

    public void print() {
        for (Entry<String, ThreadPoolExecutor> entry : inputPoolMap.entrySet()) {
            log.info(String.format("Input Task %s : Active Threads %d .", entry.getKey(), entry.getValue().getActiveCount()));
        }
    }
}
