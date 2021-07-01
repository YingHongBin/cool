package com.nus.cohanaService.Singleton;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cohana.core.cohort.query.SigmodCohortQuery;
import com.nus.cohana.core.cohort.schema.CubeSchema;
import com.nus.cohana.core.cohort.schema.TableSchema;
import com.nus.cohana.core.iceberg.query.IcebergQuery;
import com.nus.cohana.core.iceberg.result.BaseResult;
import com.nus.cohana.executor.ResultTuple;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class HDFSConnection {

    private static volatile HDFSConnection instance = null;

    private FileSystem fs;

    public static HDFSConnection getInstance() throws URISyntaxException, IOException {
        if (instance == null) {
            synchronized (HDFSConnection.class) {
                if (instance == null) {
                    instance = new HDFSConnection();
                }
            }
        }
        return instance;
    }

    private HDFSConnection() throws URISyntaxException, IOException {
        this.connect();
    }

    public void connect() throws URISyntaxException, IOException {
        System.out.println("connect to HDFS");
        String HDFS_HOST = "";
        try (InputStream input = new FileInputStream("conf/app.properties")) {
            Properties prop = new Properties();
            prop.load(input);
            String ip = prop.getProperty("hdfs.host");
            HDFS_HOST = "hdfs://" + ip + ":9000";
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        fs = FileSystem.get(new URI(HDFS_HOST), new Configuration());
    }

    public List<String> getParameters(String source, String queryId, String queryType) throws IOException {
        List<String> parameters = new ArrayList<>();
        FileStatus[] versions = fs.listStatus(new Path("/cube/" + source));
        for (FileStatus version : versions) {
            if (version.isDirectory()) {
                String path = version.getPath().toString();
                FileStatus[] files = fs.listStatus(version.getPath());
                for (FileStatus file : files) {
                    String[] tmp = file.getPath().toString().split("/");
                    String fileName = tmp[tmp.length - 1];
                    if (fileName.matches(".*[.]dz")) {
                        parameters.add(queryType + "?path=" + path + "/&file=" + fileName + "&queryId=" + queryId);
                    }
                }
            }
        }
        return parameters;
    }

    public void getBaseResult(String queryId) throws JsonMappingException, JsonParseException, IOException {
        FileStatus[] statuses = fs.listStatus(new Path("/tmp/" + queryId + "/results"));
        List<BaseResult> raw = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        for (FileStatus status : statuses) {
            String content = mapper.readValue(fs.open(status.getPath()), String.class);
            JavaType javaType = mapper.getTypeFactory().constructParametricType(ArrayList.class, BaseResult.class);
            List<BaseResult> results = mapper.readValue(content, javaType);
            raw.addAll(results);
        }
        raw = BaseResult.merge(raw);
        String content = new ObjectMapper().writeValueAsString(raw);
        FSDataOutputStream out = fs.create(new Path("/tmp/" + queryId + "/results/merged"));
        new ObjectMapper().writeValue(out, content);
    }

    public void getResultTuple(String queryId) throws JsonMappingException, JsonParseException, IOException {
        FileStatus[] statuses = fs.listStatus(new Path("/tmp/" + queryId + "/results"));
        List<ResultTuple> raw = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        for (FileStatus status : statuses) {
            String content = mapper.readValue(fs.open(status.getPath()), String.class);
            JavaType javaType = mapper.getTypeFactory().constructParametricType(ArrayList.class, ResultTuple.class);
            List<ResultTuple> results = mapper.readValue(content, javaType);
            raw.addAll(results);
        }
        raw = ResultTuple.merge(raw);
        String content = new ObjectMapper().writeValueAsString(raw);
        FSDataOutputStream out = fs.create(new Path("/tmp/" + queryId + "/results/merged"));
        new ObjectMapper().writeValue(out, content);
    }

    public FileStatus[] getResults(String queryId) throws IOException {
        return fs.listStatus(new Path("/tmp/" + queryId + "/results"));
    }

    public void createResult(String queryId, String content) throws IOException {
        String resId = UUID.randomUUID().toString();
        FSDataOutputStream out = fs.create(new Path("/tmp/" + queryId + "/results/" + resId));
        new ObjectMapper().writeValue(out, content);
    }

    public String createQuery(IcebergQuery query) throws IOException {
        String queryId = UUID.randomUUID().toString();
        FSDataOutputStream out = fs.create(new Path("/tmp/" + queryId + "/query.json"));
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(out, query);
        return queryId;
    }

    public IcebergQuery readIcebergQuery(String queryId) throws IOException {
        return IcebergQuery.read(fs.open(new Path("/tmp/" + queryId + "/query.json")));
    }

    public SigmodCohortQuery readCohortQuery(String queryId) throws IOException {
        return SigmodCohortQuery.read(fs.open(new Path("/tmp/" + queryId + "/query.json")));
    }

    public TableSchema readTableSchema(String path) throws IOException {
        InputStream in = fs.open(new Path(path + "table.yaml"));
        return TableSchema.read(in);
    }

    public CubeSchema readCubeSchema(String path) throws IOException {
        InputStream in = fs.open(new Path(path + "cube.yaml"));
        return CubeSchema.read(in);
    }

    public ByteBuffer readCublet(String path, String file) throws IOException {
        // System.out.println("start read cublet");
        return ByteBuffer.wrap(IOUtils.toByteArray(fs.open(new Path(path + file))));
    }

    public FSDataOutputStream appendCublet(String path, String file) throws IOException {
        FSDataOutputStream out = fs.append(new Path(path + file));
        return out;
    }
}
