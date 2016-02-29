package cn.wanda.dataserv.engine;

import java.util.Date;

import cn.wanda.dataserv.config.OutputConfig;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.output.Output;
import cn.wanda.dataserv.storage.StorageManager;
import cn.wanda.dataserv.storage.mem.DefaultStorageManager;
import lombok.Getter;
import lombok.extern.log4j.Log4j;

import cn.wanda.dataserv.config.ConfigTypeVals;
import cn.wanda.dataserv.config.RuntimeConfig;
import cn.wanda.dataserv.core.ObjectFactory;

@Log4j
public class OutputTask extends Thread {
    @Getter
    private OutputConfig outputConfig;

    private StorageManager storageManager;

    private RuntimeConfig runtime;

    private String outputType;

    public OutputTask(OutputConfig outputConfig, RuntimeConfig runtime, StorageManager sm) {
        this.outputConfig = outputConfig;
        this.runtime = runtime;
        this.storageManager = sm;
        this.outputType = ConfigTypeVals.getByOutputLocation(outputConfig
                .getLocation().getClass()).type;
    }

    /**
     * write data, main execute logic code of output <br>
     * if catches any exception, input will put exception to a globle map and exit.
     */
    public void run() {
        String message = "output type: " + this.outputType + " id: " + outputConfig.getId();
        log.info(message + " create...");
        Output output = ObjectFactory.getInstance().createOutput(outputConfig, runtime);
        try {
            log.info(message + " init...");
            Date d = new Date();
            output.init();
            log.info(message + " init finished. time cost: " + calcTimeElapsed(d) + "s");


            log.info(message + " starts to write data...");
            d = new Date();
            doWrite(output);
            output.last(true);
            log.info(message + " write data finished. time cost: " + calcTimeElapsed(d) + "s");

        } catch (Exception e) {
            OutputWorker.OUTPUT_ERROR.put(outputConfig.getId(), e);
        } finally {
            log.info(message + " closing...");
            Date d = new Date();
            output.close();
            log.info(message + " closed. time cost: " + calcTimeElapsed(d) + "s");
        }
    }

    private void doWrite(Output output) {
        String id = outputConfig.getId();
        Line[] lines = new Line[storageManager.get(id).getLineLimit()];
        outerloop:
        while (true) {

            int readnum = storageManager.getLine(id, lines);

            for (int i = 0; i < readnum; i++) {
                Line l = lines[i];
                if (l != Line.EOF) {
                    output.writeLine(l);
                } else {
                    break outerloop;
                }
            }
        }
    }

    private long calcTimeElapsed(Date d) {
        return (new Date().getTime() - d.getTime()) / 1000;
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
}
