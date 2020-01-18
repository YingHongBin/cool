package com.nus.coolService.Singleton;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerIndex {

    private Map<String, String> index = new ConcurrentHashMap<>();

    private static volatile WorkerIndex instance = null;

    private WorkerIndex() {}

    public static WorkerIndex getInstance() {
        if (instance == null) {
            synchronized (WorkerIndex.class) {
                if (instance == null) {
                    instance = new WorkerIndex();
                }
            }
        }
        return instance;
    }

    public void put(String worker, String parameter) {
        this.index.put(worker, parameter);
    }

    public List<String> checkDisconnected(List<String> workers) {
        List<String> parameters = new ArrayList<>();
        for (Map.Entry<String, String> entry : index.entrySet()) {
            if (!workers.contains(entry.getKey())) {
                parameters.add(entry.getValue());
                parameters.remove(entry.getKey());
            }
        }
        return parameters;
    }

    public void remove(String worker) {
        this.index.remove(worker);
    }

    public String toString()  {
        String content = null;
        try {
            content = new ObjectMapper().writeValueAsString(this.index);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return content;
    }
}
