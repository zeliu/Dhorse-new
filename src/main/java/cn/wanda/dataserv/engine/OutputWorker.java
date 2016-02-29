package cn.wanda.dataserv.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import cn.wanda.dataserv.config.DPumpConfig;
import cn.wanda.dataserv.config.OutputConfig;
import cn.wanda.dataserv.output.Output;
import cn.wanda.dataserv.storage.StorageManager;
import cn.wanda.dataserv.storage.mem.DefaultStorageManager;
import lombok.extern.log4j.Log4j;

import cn.wanda.dataserv.config.RuntimeConfig;
import cn.wanda.dataserv.core.ObjectFactory;
import cn.wanda.dataserv.utils.Printable;

@Log4j
public class OutputWorker implements Printable {

    private static final int MAX_CONCURRENCY = 64;

    public static Map<String, Throwable> OUTPUT_ERROR = new ConcurrentHashMap<String, Throwable>();

    private DPumpConfig config;
    private StorageManager storageManager;
    private Map<String, ThreadPoolExecutor> outputPoolMap = new HashMap<String, ThreadPoolExecutor>();

    public OutputWorker(DPumpConfig config, StorageManager sm) {
        this.config = config;
        this.storageManager = sm;
    }

    public void start() {
        log.info("start Output Worker");

        List<OutputConfig> outputConfigList = config.getTarget();

        log.info("Calculate Output concurrency...");

        //calc concurrency
        int concurrency = calcConcurrency(outputConfigList);

        this.doPre(outputConfigList, this.config.getRuntime());

        //init outputTask threadpool
        for (OutputConfig outputConfig : outputConfigList) {
            log.info("Start Output Task, id: " + outputConfig.getId());
            ThreadPoolExecutor outputPool = outputPoolMap.get(outputConfig.getId());
            if (outputPool == null) {
                outputPool = new ThreadPoolExecutor(concurrency, concurrency,
                        1L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
                outputPool.prestartAllCoreThreads();
                outputPoolMap.put(outputConfig.getId(), outputPool);
            }
            OutputTask outputTask = new OutputTask(outputConfig, this.config.getRuntime(), storageManager);
            outputPool.execute(outputTask);
        }
    }

    /**
     * 动态根据输入配置数量决定并发数目
     *
     * @param outputConfigList
     * @return
     */
    private int calcConcurrency(List<OutputConfig> outputConfigList) {
        int concurrency = config.getRuntime().getOutputConcurrency();
        concurrency = Math.min(outputConfigList.size(), concurrency);

        if (concurrency <= 0) {
            concurrency = Math.min(MAX_CONCURRENCY,
                    outputConfigList.size());
            log.info(String.format(
                    "output concurrency set to auto (%d).",
                    concurrency));
        }
        if (concurrency > MAX_CONCURRENCY) {
            log.info(String.format(
                    "output concurrency set to be %d, make sure it must be between [%d, %d] .",
                    concurrency, 1, MAX_CONCURRENCY));
            concurrency = MAX_CONCURRENCY;
        }

        config.getRuntime().setOutputConcurrency(concurrency);
        return concurrency;
    }

    public boolean end(boolean isInputFinished) {
        boolean outputFinish = true;
        for (Iterator<Entry<String, ThreadPoolExecutor>> i = outputPoolMap.entrySet().iterator(); i.hasNext(); ) {
            Entry<String, ThreadPoolExecutor> entry = i.next();
            if (!entry.getValue().isTerminated()) {
                outputFinish = false;
            } else if (!isInputFinished && entry.getValue().isTerminated()) {
                // if output finished before input task
                log.warn(String.format("Output Task %s failed.", entry.getKey()));
                this.storageManager.remove(entry.getKey());
                i.remove();
                // output finished
                doPost(this.config.getTarget(), this.config.getRuntime(), entry.getKey());
            } else {
                // output finished
                doPost(this.config.getTarget(), this.config.getRuntime(), entry.getKey());
            }
        }
        return outputFinish;
    }

    public void doPre(List<OutputConfig> outputConfigList, RuntimeConfig runtime) {
        Set<String> idSet = new HashSet<String>();

        // do pre for each id
        for (OutputConfig config : outputConfigList) {
            String id = config.getId();
            if (idSet.contains(id)) {
                continue;
            }
            idSet.add(id);
            log.info("do Output PreTask... id: " + id);
            Output output = ObjectFactory.getInstance().createOutput(config, runtime);
            output.prepare();
            log.info("finished Output PreTask. id: " + id);
        }
    }

    public void doPost(List<OutputConfig> outputConfigList, RuntimeConfig runtime, String id) {
        Set<String> idSet = new HashSet<String>();

        if (idSet.contains(id)) {
            return;
        }
        // do post for each id
        for (OutputConfig config : outputConfigList) {
            if (!config.getId().equals(id)) {
                continue;
            }
            idSet.add(id);
            log.info("do Output PostTask... id: " + id);
            boolean success = taskSuccessed(id);
            Thread t = new Thread(new PostWorker(config, runtime, success));
            t.start();
            try {
                t.join();
            } catch (InterruptedException e) {
                log.warn("post thread is interrupted.");
            }
            log.info("finished Output PostTask. id: " + id);
            return;
        }
    }

    private boolean taskSuccessed(String id) {
        if (OutputWorker.OUTPUT_ERROR.get(id) != null) {
            return false;
        }
        if (InputWorker.INPUT_ERROR.get(id) != null || InputWorker.INPUT_ERROR.get(DefaultStorageManager.INPUT_ID_ALL) != null) {
            return false;
        }
        return true;
    }

    public void shutdown() {
        for (ThreadPoolExecutor outputPool : outputPoolMap.values()) {
            outputPool.shutdown();
        }
    }

    public void shutdownnow() throws InterruptedException {
        for (ThreadPoolExecutor outputPool : outputPoolMap.values()) {
            outputPool.shutdownNow();
            outputPool.awaitTermination(3, TimeUnit.SECONDS);
        }
    }

    public void print() {
        for (Entry<String, ThreadPoolExecutor> entry : outputPoolMap.entrySet()) {
            log.info(String.format("Output Task %s : Active Threads %d .", entry.getKey(), entry.getValue().getActiveCount()));
        }
    }

    public class PostWorker implements Runnable {

        private OutputConfig oc;
        private RuntimeConfig rt;
        private boolean success;

        public PostWorker(OutputConfig outputConfig, RuntimeConfig runtime, boolean success) {
            this.success = success;
            this.oc = outputConfig;
            this.rt = runtime;
        }

        public void run() {
            try {
                Output output = ObjectFactory.getInstance().createOutput(oc, rt);
                log.info("get output for post:" + output);
                output.post(success);
            } catch (Exception e) {
                OutputWorker.OUTPUT_ERROR.put(oc.getId() + "_post", e);
            }
        }
    }
}
