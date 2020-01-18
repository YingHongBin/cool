package com.nus.coolService.model;

public class QueryInfo {

    private int workNumber;

    private long startTime;

    private QueryType queryType;

    public QueryInfo(int workNumber, long startTime, QueryType queryType) {
        this.workNumber = workNumber;
        this.startTime = startTime;
        this.queryType = queryType;
    }

    public int getWorkNumber() {
        return workNumber;
    }

    public void setWorkNumber(int workNumber) {
        this.workNumber = workNumber;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public QueryType getQueryType() {
        return queryType;
    }
}
