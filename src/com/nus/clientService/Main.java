package com.nus.clientService;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception {
        //ObjectMapper mapper = new ObjectMapper();
        //IcebergQuery query = mapper.readValue(new File("tpc-h-data/append-test/1.json"), IcebergQuery.class);
        //HDFSConnection fs = HDFSConnection.getInstance();
        //String queryId = fs.createQuery(query);
        if (args.length != 1) {
            System.err.println("Pass in query id (Example: q1)");
            return;
        }

        CloseableHttpClient client = HttpClients.createDefault();
        String params;
        if (args[0].equals("q1")) {
            params = "queryId=1&type=cohort";
	} else if (args[0].equals("q2")) {
            params = "queryId=2&type=iceberg";
        } else if (args[0].equals("q3")) {
	    params = "queryId=3&type=iceberg";
        } else if (args[0].equals("q5a")) {
            params = "queryId=5a&type=cohort";
        } else if (args[0].equals("q5b")) {
            params = "queryId=5b&type=iceberg";
        } else {
            System.err.println("Unrecognized query id");
            return;
        }
        String ip = "";
        try (InputStream input = new FileInputStream("conf/app.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            ip = prop.getProperty("server.host");
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        String request = "http://" + ip + ":9001/broker?" + params;
        URL url = new URL(request);
        URI uri = new URI(url.getProtocol(), null, url.getHost(), url.getPort(), url.getPath(), url.getQuery(), null);
        HttpGet get = new HttpGet(uri);
        client.execute(get);
    }
}
