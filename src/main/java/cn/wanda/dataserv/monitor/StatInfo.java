package cn.wanda.dataserv.monitor;

import java.util.Date;

public class StatInfo {
    private String id;

    private Date beginTime;

    private Date endTime;

    private long lInputNum;

    private long lOutputNum;

    private long bInputNum;

    private long bOutputNum;

    private long inputRefused;

    private long outputRefused;

    private long lInputTotalNum;

    private long lOutputTotalNum;

    private long bInputTotalNum;

    private long bOutputTotalNum;

    private long totalSeconds;

    public StatInfo(String id) {
        this.setId(id);
        lInputNum = 0;
        lOutputNum = 0;
        bInputNum = 0;
        bOutputNum = 0;
        inputRefused = 0;
        outputRefused = 0;
        lInputTotalNum = 0;
        lOutputTotalNum = 0;
        bInputTotalNum = 0;
        bOutputTotalNum = 0;
        totalSeconds = 0;
        beginTime = new Date();
    }

    public void clearAndAddToTotalPeriodicity() {
        this.bInputTotalNum += this.bInputNum;
        this.lInputTotalNum += this.lInputNum;
        this.bOutputTotalNum += this.bOutputNum;
        this.lOutputTotalNum += this.lOutputNum;
        lInputNum = 0;
        lOutputNum = 0;
        bInputNum = 0;
        bOutputNum = 0;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(Date beginTime) {
        this.beginTime = beginTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public long getlInputNum() {
        return lInputNum;
    }

    public void setlInputNum(long lInputNum) {
        this.lInputNum = lInputNum;
    }

    public long getlOutputNum() {
        return lOutputNum;
    }

    public void setlOutputNum(long lOutputNum) {
        this.lOutputNum = lOutputNum;
    }

    public long getbInputNum() {
        return bInputNum;
    }

    public void setbInputNum(long bInputNum) {
        this.bInputNum = bInputNum;
    }

    public long getbOutputNum() {
        return bOutputNum;
    }

    public void setbOutputNum(long bOutputNum) {
        this.bOutputNum = bOutputNum;
    }

    public long getInputRefused() {
        return inputRefused;
    }

    public void setInputRefused(long inputRefused) {
        this.inputRefused = inputRefused;
    }

    public long getOutputRefused() {
        return outputRefused;
    }

    public void setOutputRefused(long outputRefused) {
        this.outputRefused = outputRefused;
    }

    public long getlInputTotalNum() {
        return lInputTotalNum;
    }

    public void setlInputTotalNum(long lInputTotalNum) {
        this.lInputTotalNum = lInputTotalNum;
    }

    public long getlOutputTotalNum() {
        return lOutputTotalNum;
    }

    public void setlOutputTotalNum(long lOutputTotalNum) {
        this.lOutputTotalNum = lOutputTotalNum;
    }

    public long getbInputTotalNum() {
        return bInputTotalNum;
    }

    public void setbInputTotalNum(long bInputTotalNum) {
        this.bInputTotalNum = bInputTotalNum;
    }

    public long getbOutputTotalNum() {
        return bOutputTotalNum;
    }

    public void setbOutputTotalNum(long bOutputTotalNum) {
        this.bOutputTotalNum = bOutputTotalNum;
    }

    public long getTotalSeconds() {
        return totalSeconds;
    }

    public void setTotalSeconds(long totalSeconds) {
        this.totalSeconds = totalSeconds;
    }

    public void addlInputNum(int num) {
        this.lInputNum += num;
    }

    public void addlOutputNum(int num) {
        this.lOutputNum += num;
    }

    public void addbInputNum(int num) {
        this.bInputNum += num;
    }

    public void addbOutputNum(int num) {
        this.bOutputNum += num;
    }
}