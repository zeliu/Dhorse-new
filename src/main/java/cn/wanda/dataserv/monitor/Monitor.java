package cn.wanda.dataserv.monitor;

import java.text.SimpleDateFormat;
import java.util.Date;

import lombok.extern.log4j.Log4j;

@Log4j
public class Monitor {

    private StatInfo stat;

    private long period = 30;

    public Monitor(String id) {
        this.stat = new StatInfo(id);
    }

    public StatInfo getStat() {
        return stat;
    }

    public String getId() {
        return stat.getId();
    }

    public Date getBeginTime() {
        return stat.getBeginTime();
    }

    public Date getEndTime() {
        return stat.getEndTime();
    }

    public void setEndTime() {
        stat.setEndTime(new Date());
    }

    public long getlInputNum() {
        return stat.getlInputNum();
    }

    public void addlInputNum(int num) {
        stat.addlInputNum(num);
    }

    public long getlOutputNum() {
        return stat.getlOutputNum();
    }

    public void addlOutputNum(int num) {
        stat.addlOutputNum(num);
    }

    public long getbInputNum() {
        return stat.getbInputNum();
    }

    public void addbInputNum(int num) {
        stat.addbInputNum(num);
    }

    public long getbOutputNum() {
        return stat.getbOutputNum();
    }

    public void addbOutputNum(int num) {
        stat.addbOutputNum(num);
    }

    public long getInputRefused() {
        return stat.getInputRefused();
    }

    public void addInputRefused(int num) {
        stat.setInputRefused(stat.getInputRefused() + num);
    }

    public long getOutputRefused() {
        return stat.getOutputRefused();
    }

    public void addOutputRefused(int num) {
        stat.setOutputRefused(stat.getOutputRefused() + num);
    }

    public long getlInputTotalNum() {
        return stat.getlInputTotalNum();
    }

    public long getlOutputTotalNum() {
        return stat.getlOutputTotalNum();
    }

    public long getbInputTotalNum() {
        return stat.getbInputTotalNum();
    }

    public long getbOutputTotalNum() {
        return stat.getbOutputTotalNum();
    }

    public long getTotalSeconds() {
        return stat.getTotalSeconds();
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public void printInfoPeriodicity() {
        log.info(String.format("Storage %s: ", stat.getId()));
        log.info(String.format("input speed: %s | %s ",
                getSpeed(stat.getbInputNum(), this.period),
                getLineSpeed(stat.getlInputNum(), this.period)));
        log.info(String.format("output speed: %s | %s ",
                getSpeed(stat.getbOutputNum(), this.period),
                getLineSpeed(stat.getlOutputNum(), this.period)));
        log.info(String.format("transferred : %s lines, %s ",
                stat.getlInputTotalNum(), getTransferedByte(stat.getbInputTotalNum())));
        stat.clearAndAddToTotalPeriodicity();
    }

    private String getTransferedByte(long byteNum) {
        long result = 0;
        if ((result = byteNum / 1000000) > 0) {
            return result + "MB";
        } else if ((result = byteNum / 1000) > 0) {
            return result + "KB";
        } else {
            return byteNum + "B";
        }
    }

    private String getSpeed(long byteNum, long seconds) {
        if (seconds == 0) {
            seconds = 1;
        }
        long bytePerSecond = byteNum / seconds;
        long unit = bytePerSecond;
        if ((unit = bytePerSecond / 1000000) > 0) {
            return unit + "MB/s";
        } else if ((unit = bytePerSecond / 1000) > 0) {
            return unit + "KB/s";
        } else {
            if (byteNum > 0 && bytePerSecond <= 0) {
                bytePerSecond = 1;
            }
            return bytePerSecond + "B/s";
        }
    }

    private String getLineSpeed(long lines, long seconds) {
        if (seconds == 0) {
            seconds = 1;
        }
        long linePerSecond = lines / seconds;

        if (lines > 0 && linePerSecond <= 0) {
            linePerSecond = 1;
        }

        return linePerSecond + "L/s";
    }

    public void printTotalInfo() {
        stat.clearAndAddToTotalPeriodicity();

        stat.setEndTime(new Date());

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long timeElapsed = (stat.getEndTime().getTime() - stat.getBeginTime().getTime()) / 1000;

        log.info(String.format("\n"
                        + "Storage %s : \n"
                        + "%-26s: %-18s\n"
                        + "%-26s: %-18s\n"
                        + "%-26s: %19s\n"
                        + "%-26s: %19s\n"
                        + "%-26s: %19s\n"
                        + "%-26s: %19s\n",
                stat.getId(),
                "DPump starts work at", df.format(stat.getBeginTime()),
                "ends work at", df.format(stat.getEndTime()),
                "Total time costs", String.valueOf(timeElapsed) + "s",
                "Average byte speed", getSpeed(stat.getbInputTotalNum(), timeElapsed),
                "Average line speed", getLineSpeed(stat.getlInputTotalNum(), timeElapsed),
                "Total transferred records", String.valueOf(stat.getlInputTotalNum())));
    }

}
