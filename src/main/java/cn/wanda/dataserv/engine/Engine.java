package cn.wanda.dataserv.engine;

import java.util.Map;
import java.util.Map.Entry;

import cn.wanda.dataserv.config.OutputConfig;
import cn.wanda.dataserv.storage.Storage;
import cn.wanda.dataserv.storage.StorageManager;
import lombok.extern.log4j.Log4j;

import cn.wanda.dataserv.config.DPumpConfig;
import cn.wanda.dataserv.core.ObjectFactory;

/**
 * Dpump start from here
 *
 * @author songqian
 */
@Log4j
public class Engine {

    public Engine() {
        Runtime.getRuntime().addShutdownHook(shutdownHook());
    }

    InputWorker inputWorker;
    OutputWorker outputWorker;


    /**
     * Shut down feeder instance by Ctrl-C
     *
     * @return shutdown thread
     */
    public Thread shutdownHook() {
        return new Thread() {
            public void run() {
                try {
                    shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private volatile boolean closed;

    public synchronized void shutdown() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        if (inputWorker != null) {
            inputWorker.shutdownNow();
            inputWorker = null;
        }

        if (outputWorker != null) {
            outputWorker.shutdownnow();
        }
    }

    public static void main(String[] args) {
    }

    public void run(DPumpConfig config) {
        this.closed = false;
        log.info("init DPump...");

        //create storageManager
        log.info("create storage manager");
        StorageManager storageManager = createStorageManager(config);

        //create input worker and output worker
        log.info("create input worker");
        inputWorker = new InputWorker(config, storageManager);
        log.info("create output worker");
        outputWorker = new OutputWorker(config, storageManager);

        log.info("");
        //start to execute input and output
        inputWorker.start();
        outputWorker.start();

        //Initiates an orderly shutdown in which input and output tasks are executed
        inputWorker.shutdown();
        outputWorker.shutdown();

        // waits for all input and output task finished.
        joinTask(config, storageManager, inputWorker, outputWorker);

        //print stat after task finished.
        storageManager.printTotalInfo();

        //print exceptions
        if (InputWorker.INPUT_ERROR.size() > 0 || OutputWorker.OUTPUT_ERROR.size() > 0) {
            for (Entry<String, Throwable> e : InputWorker.INPUT_ERROR.entrySet()) {
                log.error("input id: " + e.getKey() + " occured error. " + e.getValue().getMessage(), e.getValue());
            }
            for (Entry<String, Throwable> e : OutputWorker.OUTPUT_ERROR.entrySet()) {
                log.error("output id: " + e.getKey() + " occured error. " + e.getValue().getMessage(), e.getValue());
            }
            log.error("DPump Work failed!");
            System.exit(1);
        } else {
            log.info("DPump Work finished.");
            System.exit(0);
        }
        closed = true;
    }

    /**
     * waits for all input and output task finished.
     *
     * @param config
     * @param storageManager
     * @param inputWorker
     * @param outputWorker
     */
    private void joinTask(DPumpConfig config, StorageManager storageManager,
                          InputWorker inputWorker, OutputWorker outputWorker) {
        long monitorPeriod = config.getRuntime().getMonitorPeriod();
        long sleepTime = 0;
        while (true) {
            boolean inputWorkEnd = inputWorker.end();
            boolean outputWorkerEnd = outputWorker.end(inputWorkEnd);
            // dpump finished
            if (inputWorkEnd && outputWorkerEnd) {
                break;
            }
            // if output worker failed
            if ((!inputWorkEnd) && outputWorkerEnd) {
//				log.error("DPump Work failed!");
                try {
                    inputWorker.shutdownNow();
                } catch (InterruptedException e) {
                    log.debug(e.getMessage());
                }
//				System.exit(1);
                return;
            }

            try {
                Thread.sleep(1000);
                sleepTime++;
            } catch (InterruptedException e) {
                log.debug(e.getMessage());
            }
            if (monitorPeriod > 0 && sleepTime % monitorPeriod == 0) {
                inputWorker.print();
                outputWorker.print();
                //print storage stat info periodicity
                storageManager.print();
            }
        }
    }

    /**
     * create a storage for each output id
     *
     * @param config
     * @return
     */
    private StorageManager createStorageManager(DPumpConfig config) {
        StorageManager storageManager = ObjectFactory.getInstance().createStorageManager(config.getRuntime(), config.getInputProcessor(), config.getOutputProcessor());
        //create a store for each output id
        for (OutputConfig o : config.getTarget()) {
            if (storageManager.get(o.getId()) != null) {
                continue;
            }
            Storage store = ObjectFactory.getInstance().createStorage(config.getRuntime(), o.getId());
            storageManager.add(o.getId(), store);
        }
        return storageManager;
    }
}
