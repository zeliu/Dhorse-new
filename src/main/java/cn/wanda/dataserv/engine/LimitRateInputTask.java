package cn.wanda.dataserv.engine;

import cn.wanda.dataserv.config.InputConfig;
import cn.wanda.dataserv.core.Line;
import cn.wanda.dataserv.input.Input;
import cn.wanda.dataserv.storage.StorageManager;
import lombok.extern.log4j.Log4j;

import cn.wanda.dataserv.config.RuntimeConfig;

/**
 * TODO : 在InputWork中判断是否设置limitRate，然后按情况实例化InputTask或LimitRateInputTask
 *
 * @author songqian
 */
@Log4j
public class LimitRateInputTask extends InputTask {

    /**
     * 1Mb
     */
    private static final long period = 1048576;

    /**
     * kb/s
     */
    private double limitRate;
    private long lastTime = 0;
    private long byteCount = 0;

    public LimitRateInputTask(InputConfig inputConfig, RuntimeConfig runtime,
                              StorageManager storageManager) {
        super(inputConfig, runtime, storageManager);
        this.limitRate = (double) runtime.getLimitRate() / (double) runtime.getInputConcurrency();
    }

    @Override
    protected Line getLine(Input input) {
        Line line = input.readLine();
        byteCount = byteCount + line.getLine().length() * 2;

        if (byteCount >= period) {
            limitRate();
        }

        return line;
    }

    protected void limitRate() {
        long currentTime = System.currentTimeMillis();

        //cost time : second
        double costTime = (double) (currentTime - lastTime) / 1000D;

        double expectTime = ((double) byteCount / 1024D) / limitRate;

        if (expectTime > costTime) {
            try {
                sleep((long) ((expectTime - costTime) * 1000));
            } catch (InterruptedException e) {
                log.debug(e.getMessage());
            }
        }

        lastTime = System.currentTimeMillis();
        byteCount = 0;
    }

}