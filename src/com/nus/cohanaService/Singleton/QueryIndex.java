package com.nus.cohanaService.Singleton;

import com.nus.cohanaService.model.QueryInfo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueryIndex {

    private Map<String, QueryInfo> index = new ConcurrentHashMap<>();

    private static volatile QueryIndex instance = null;

    public static QueryIndex getInstance() {
        if (instance == null) {
            synchronized (QueryIndex.class) {
                if (instance == null) {
                    instance = new QueryIndex();
                }
            }
        }
        return instance;
    }

    private QueryIndex() {}

    public void put(String queryId, QueryInfo queryInfo) {
        this.index.put(queryId, queryInfo);
    }

    public QueryInfo get(String queryId) {
        return this.index.get(queryId);
    }
}
