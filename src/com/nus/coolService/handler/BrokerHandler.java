package com.nus.coolService.handler;

import com.nus.coolService.Singleton.*;
import com.nus.coolService.model.Parameter;
import com.nus.coolService.model.QueryInfo;
import com.nus.coolService.model.QueryType;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;

public class BrokerHandler extends AbstractHandler {

    @Override
    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response) {
        try {
            String queryId = request.getParameter("queryId");
            String queryType = request.getParameter("type");
            HDFSConnection fs = HDFSConnection.getInstance();
            String source = queryType.equals("cohort") ? fs.readCohortQuery(queryId).getDataSource()
                    : fs.readIcebergQuery(queryId).getDataSource();
            List<String> parameters = fs.getParameters(source, queryId, queryType);
            long begin = System.currentTimeMillis();
            QueryInfo queryInfo = new QueryInfo(parameters.size(), begin, QueryType.getEnum(queryType));
            QueryIndex queryIndex = QueryIndex.getInstance();
            queryIndex.put(queryId, queryInfo);
            TaskQueue taskQueue = TaskQueue.getInstance();
            for (String parameter : parameters) {
                Parameter p = new Parameter(1, parameter);
                taskQueue.add(p);
            }
        } catch (Exception e) {
            e.printStackTrace();
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            baseRequest.setHandled(true);
        }
    }
}
