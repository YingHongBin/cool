package com.nus.cohanaService;

import com.nus.cohanaService.model.Parameter;
import com.nus.cohanaService.Singleton.TaskQueue;
import com.nus.cohanaService.Singleton.WorkerIndex;
import com.nus.cohanaService.Singleton.ZKConnection;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

public class WorkerWatcher implements Watcher {

    private ZooKeeper zk;

    public WorkerWatcher(ZKConnection zk) throws KeeperException, InterruptedException {
        this.zk = zk.getZK();
        this.zk.getChildren("/workers", this);
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                WorkerIndex workerIndex = WorkerIndex.getInstance();
                List<String> workers = zk.getChildren("/workers", this);
                List<String> parameters = workerIndex.checkDisconnected(workers);
                TaskQueue taskQueue = TaskQueue.getInstance();
                for (String parameter : parameters) {
                    Parameter p = new Parameter(0, parameter);
                    taskQueue.add(p);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
