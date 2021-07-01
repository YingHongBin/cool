package com.nus.cohanaService.Singleton;

import com.nus.cohanaService.model.Parameter;

import java.util.Queue;
import java.util.concurrent.PriorityBlockingQueue;

public class TaskQueue {

    private Queue<Parameter> q = new PriorityBlockingQueue<Parameter>();

    private static volatile TaskQueue instance = null;

    private TaskQueue() {}

    public static TaskQueue getInstance() {
        if (instance == null) {
            synchronized (TaskQueue.class) {
                if (instance == null) {
                    instance = new TaskQueue();
                }
            }
        }
        return instance;
    }

    public void add(Parameter p) {
        this.q.add(p);
    }

    public Parameter poll() {
        return this.q.poll();
    }

    public int size() {
        return this.q.size();
    }
}
