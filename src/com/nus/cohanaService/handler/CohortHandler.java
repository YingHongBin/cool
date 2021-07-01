package com.nus.cohanaService.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cohana.core.cohort.query.SigmodCohortQuery;
import com.nus.cohana.executor.CohortModel;
import com.nus.cohana.executor.LocalCohortLoader;
import com.nus.cohana.executor.QueryResult;
import com.nus.cohana.executor.ResultTuple;
import com.nus.cohanaService.Singleton.HDFSConnection;
import com.nus.cohanaService.Singleton.ZKConnection;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CohortHandler extends AbstractHandler {

    @Override
    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response) throws ServletException, IOException {
        System.out.println("version0.0.1");
        ZKConnection zk;
        try {
            zk = ZKConnection.getInstance();
        } catch ( IOException | InterruptedException e) {
            System.out.println("ZKConnection Error");
            throw new ServletException("ZKConnection Error");
        }

        HDFSConnection fs;
        try {
            fs = HDFSConnection.getInstance();
        } catch ( IOException | URISyntaxException e) {
            System.out.println("HDFSConnection Error");
            throw new ServletException("HDFSConnection Error");
        }

        System.out.println("process file: " + request.getParameter("file"));
        String path = request.getParameter("path");
        String file = request.getParameter("file");
        String queryId = request.getParameter("queryId");
        String workerName = request.getParameter("worker");

        SigmodCohortQuery query;
        try {
            query = fs.readCohortQuery(queryId);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("get query Error");
            throw new ServletException("get query Error");
        }
        String source = query.getDataSource();

        ByteBuffer buffer;
        try {
            buffer = fs.readCublet(path, file);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("get cublet error");
            throw new ServletException("get cublet error");
        }

        Map<String, DataOutputStream> map = null;
        if (query.getOutSource() != null) {
            map = new HashMap<>();
            path = path.replace(query.getDataSource(), query.getOutSource());
            map.put(file, fs.appendCublet(path, file));
        }

        List<ResultTuple> results;
        try {
            long begin = System.currentTimeMillis();
            CohortModel model = new CohortModel(buffer.order(ByteOrder.nativeOrder()),
                    fs.readTableSchema(path), fs.readCubeSchema(path), source, file);
            results = LocalCohortLoader.executeQuery(model.getCube(source), query, map);
            for (DataOutputStream dos : map.values()) {
                dos.flush();
                dos.close();
            }
            long end = System.currentTimeMillis();
            System.out.println("query elapsed: " + (end - begin));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("execute query error");
            throw new ServletException("execute query error");
        }
        try {
            String content = new ObjectMapper().writeValueAsString(results);
            fs.createResult(queryId, content);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("write results error");
            throw new ServletException("write results error");
        }
        try {
            zk.relaseWorker(workerName);
        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
            System.out.println("release worker error");
            throw new ServletException("release worker error");
        }
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println(queryId);
        baseRequest.setHandled(true);
    }

}


