package com.nus.cohanaService.handler;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class TestHandler extends AbstractHandler {

    @Override
    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response) throws IOException {
        System.out.println("success");

        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("success");
        baseRequest.setHandled(true);
    }
}
