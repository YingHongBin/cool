package com.nus.cohanaService;

import com.nus.cohanaService.Singleton.*;
import com.nus.cohanaService.model.Parameter;
import com.nus.cohanaService.model.QueryInfo;
import com.nus.cohanaService.model.QueryType;
import com.nus.cohanaService.model.Worker;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

public class BrokerThread extends Thread {
    public void run() {
        try {
            System.out.println("thread running");
            ZKConnection zk = ZKConnection.getInstance();
            zk.getZK().create("/brokerThread", "running".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            TaskQueue taskQueue = TaskQueue.getInstance();
            while (true) {
                if (taskQueue.size() == 0)
                    continue;
                List<Worker> workers = zk.getFreeWorkers();
                if (workers == null)
                    continue;
                WorkerIndex workerIndex = WorkerIndex.getInstance();
                for (Worker worker : workers) {
                    if (taskQueue.size() == 0)
                        break;
                    Parameter p = taskQueue.poll();
                    workerIndex.put(worker.getWokerName(), p.getContent());
                    String req = "http://" + worker.getInfo().getIp() + "/" + p.getContent() + "&worker="
                            + worker.getWokerName();
                    URL url = new URL(req);
                    URI uri = new URI(url.getProtocol(), null, url.getHost(), url.getPort(), url.getPath(),
                            url.getQuery(), null);
                    HttpGet get = new HttpGet(uri);
                    zk.allocateWorker(worker.getWokerName());
                    CloseableHttpAsyncClient client = HttpAsyncClients.createDefault();
                    client.start();
                    client.execute(get, new FutureCallback<HttpResponse>() {
                        @Override
                        public void completed(HttpResponse httpResponse) {
                            HttpEntity entity = httpResponse.getEntity();
                            String queryId = null;
                            try {
                                BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()));
                                queryId = reader.readLine();
                                check(queryId);
                            } catch (IOException | URISyntaxException e) {
                                System.out.println("fuck off");
                            }

                            workerIndex.remove(worker.getWokerName());
                            System.out.println(httpResponse.getStatusLine().getStatusCode());
                            System.out.println("complete");
                        }

                        @Override
                        public void failed(Exception e) {
                            System.out.println("failed");
                        }

                        @Override
                        public void cancelled() {
                            System.out.println("cancelled");
                        }
                    });

                    // TODO: remove timer
                    long st = System.currentTimeMillis();
                    int st_idx = p.getContent().indexOf("queryId=");
                    String queryId = p.getContent().substring(st_idx + 8, p.getContent().length());
                    System.out.println("waiting elapsed: " +
                            (st - QueryIndex.getInstance().get(queryId).getStartTime()));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void check(String queryId) throws IOException, URISyntaxException {
        long end = System.currentTimeMillis();
        QueryIndex queryIndex = QueryIndex.getInstance();
        QueryInfo queryInfo = queryIndex.get(queryId);
        HDFSConnection fs = HDFSConnection.getInstance();
        int completedNumber = fs.getResults(queryId).length;
        System.out.println(completedNumber + " tasks completed out of " + queryInfo.getWorkNumber());
        if (completedNumber == queryInfo.getWorkNumber()) {
            if (queryInfo.getQueryType() == QueryType.COHORT) {
                fs.getResultTuple(queryId);
            } else if (queryInfo.getQueryType() == QueryType.ICEBERG) {
                System.out.println("Merge for ICEBERG Query " + queryId);
                fs.getBaseResult(queryId);
            }
            System.out.println(queryId + " elapsed: " + (end - queryInfo.getStartTime()));
        }
    }
}
