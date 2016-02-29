package cn.wanda.dataserv.engine;

import java.util.Date;

import cn.wanda.dataserv.config.FieldConfig;
import cn.wanda.dataserv.config.InputConfig;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.Input;
import cn.wanda.dataserv.process.LineSchema;
import cn.wanda.dataserv.storage.Storage;
import cn.wanda.dataserv.storage.StorageManager;
import lombok.extern.log4j.Log4j;

import cn.wanda.dataserv.config.ConfigTypeVals;
import cn.wanda.dataserv.config.RuntimeConfig;
import cn.wanda.dataserv.core.ObjectFactory;

/**
 * represents executor of input
 *
 * @author songqian
 */
@Log4j
public class InputTask extends Thread {
    private InputConfig inputConfig;

    private StorageManager storageManager;

    private RuntimeConfig runtime;

    private String inputType;

    public InputTask(InputConfig inputConfig, RuntimeConfig runtime, StorageManager storageManager) {
        this.inputConfig = inputConfig;
        this.storageManager = storageManager;
        this.runtime = runtime;
        LineSchema lineSchema = new LineSchema();
        if (inputConfig.getSchema() != null && inputConfig.getSchema().getFields() != null) {
            for (FieldConfig f : inputConfig.getSchema().getFields()) {
                lineSchema.addField(f);
            }
        }
        lineSchema.setFieldDelim(inputConfig.getSchema().getFieldDelim());
        this.storageManager.setInputLineSchema(lineSchema);
        this.inputType = ConfigTypeVals.getByInputLocation(inputConfig
                .getLocation().getClass()).type;
    }

    /**
     * read data, main execute logic code of input <br>
     * if catches any exception, input will put exception to a globle map and exit.
     */
    public void run() {
        String message = "input type: " + this.inputType + " id: " + inputConfig.getId();
        log.info(message + " create...");
        Input input = ObjectFactory.getInstance().createInput(inputConfig, runtime);
        try {
            log.info(message + " init...");
            Date d = new Date();
            input.init();
            log.info(message + " init finished. time cost: " + calcTimeElapsed(d) + "s");

            log.info(message + " starts to read data...");
            d = new Date();
            doRead(input);
            log.info(message + " read data finished. time cost: " + calcTimeElapsed(d) + "s");
        } catch (Exception e) {
            InputWorker.INPUT_ERROR.put(inputConfig.getId(), e);
        } finally {

            log.info(message + " do post...");
            Date d = new Date();
            boolean success = taskSuccessed(inputConfig.getId());
            input.post(success);
            log.info(message + " finish post. time cost: " + calcTimeElapsed(d) + "s");

            log.info(message + " closing...");
            d = new Date();
            input.close();
            log.info(message + " closed. time cost: " + calcTimeElapsed(d) + "s");
        }
    }

    private void doRead(Input input) {
        String id = inputConfig.getId();
        int lineLimit = 1;
        Storage s = storageManager.get(id);
        if (s != null) {
            lineLimit = s.getLineLimit();
        }
        Line[] lines = new Line[lineLimit];
        boolean flag = true;
        while (flag) {
            int i = 0;
            while (i < lines.length) {
                Line l = getLine(input);
                if (l == Line.EOF) {
                    flag = false;
                    break;
                }
                lines[i] = l;
                i++;
            }
            storageManager.putLine(id, lines, i);
        }
    }

    private long calcTimeElapsed(Date d) {
        return (new Date().getTime() - d.getTime()) / 1000;
    }

    protected Line getLine(Input input) {
        return input.readLine();
    }

    private boolean taskSuccessed(String id) {
        if (OutputWorker.OUTPUT_ERROR.get(id) != null) {
            return false;
        }
        if (InputWorker.INPUT_ERROR.get(id) != null) {
            return false;
        }
        return true;
    }

}
