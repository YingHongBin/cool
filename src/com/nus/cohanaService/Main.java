package com.nus.cohanaService;

import com.nus.cohanaService.Singleton.*;
import com.nus.cohanaService.handler.BrokerHandler;
import com.nus.cohanaService.handler.CohortHandler;
import com.nus.cohanaService.handler.IcebergHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

import java.net.InetAddress;

public class Main {

    public enum Role { WORKER, BROKER }

    public static void main(String[] args) throws Exception {
        System.out.println("version0.0.1");
        HDFSConnection fs = HDFSConnection.getInstance();
        ZKConnection zk = ZKConnection.getInstance();
        InetAddress addr = InetAddress.getLocalHost();
        String host = addr.getHostAddress();
        Role role = Role.valueOf(args[0]);
        switch (role) {
            case BROKER:
                zk.createBroker(host + ":9001");
                WorkerWatcher workerWatcher = new WorkerWatcher(zk);
                TaskQueue taskQueue = TaskQueue.getInstance();
                WorkerIndex workerIndex = WorkerIndex.getInstance();
                QueryIndex queryIndex = QueryIndex.getInstance();
                Thread thread = new BrokerThread();
                thread.start();
                break;
            case WORKER:
                zk.addWorker(host + ":9001");
                break;
            default:
                throw new IllegalArgumentException();
        }

        Server server = new Server(9001);

        ContextHandler context0 = new ContextHandler();
        context0.setContextPath("/iceberg");
        context0.setHandler(new IcebergHandler());

        ContextHandler context1 = new ContextHandler();
        context1.setContextPath("/broker");
        context1.setHandler(new BrokerHandler());

        ContextHandler context2 = new ContextHandler();
        context2.setContextPath("/cohort");
        context2.setHandler(new CohortHandler());

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.setHandlers(new Handler[] {context0, context1, context2});
        server.setHandler(contexts);
        server.start();
        server.join();
    }
}
