package com.nus.cool.executor;

import java.io.*;

import java.util.*;

// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
// import org.apache.hadoop.io.IOUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.nus.cool.core.cohort.query.SigmodCohortQuery;

import com.nus.cool.core.cohort.query.CohortAggregation;
import com.nus.cool.core.cohort.query.CohortKey;
import com.nus.cool.core.cohort.query.CohortSelection;
import com.nus.cool.core.cohort.converter.DayIntConverter;
import com.nus.cool.core.cohort.converter.NumericConverter;
import com.nus.cool.core.cohort.schema.TableSchema;
import com.nus.cool.core.io.compression.SimpleBitSetCompressor;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.CubeRS;
import com.nus.cool.core.io.readstore.CubletRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.lang.Integers;

public class LocalCohortLoader {
    // private static final Log LOG = LogFactory.getLog(LocalCohortLoader.class);
    private static CohortModel cohortModel;

    public static List<ResultTuple> executeQuery(CubeRS cube, SigmodCohortQuery query, Map<String, DataOutputStream> map) throws JsonProcessingException, IOException {
        List<CubletRS> cublets = cube.getCublets();
        List<File> cubletFiles = cube.getCubletFiles();
        TableSchema tableSchema = cube.getTableSchema();
        // Comment the following line after sigmod
        List<ResultTuple> resultSet = new ArrayList<>();
        // end of comment
        boolean tag = query.getOutSource() != null; // this tag should in cohort query
        List<BitSet> bitSets = new ArrayList<>();
        //for (CubletRS cubletRS : cublets) {
        for (int i = 0; i < cublets.size(); i++) {
            CubletRS cubletRS = cublets.get(i);
            MetaChunkRS metaChunk = cubletRS.getMetaChunk();
            CohortSelection sigma = new CohortSelection();
            CohortAggregation gamma = new CohortAggregation(sigma);
            gamma.init(tableSchema, query);
            gamma.process(metaChunk);
            if (sigma.isUserActiveCublet()) {
                List<ChunkRS> dataChunks = cubletRS.getDataChunks();
                for (ChunkRS dataChunk : dataChunks) {
                    gamma.process(dataChunk);
                    bitSets.add(gamma.getBs());
                }
            }
            if (tag) {
                int end = cubletRS.getLimit();
                DataOutputStream out = map.get(cubletRS.getFile());
                for(BitSet bs : bitSets) {
                    SimpleBitSetCompressor.compress(bs, out);
                }
                out.writeInt(Integers.toNativeByteOrder(end));
                out.writeInt(Integers.toNativeByteOrder(bitSets.size()));
                out.writeInt(Integers.toNativeByteOrder(0));    // mark
            }

            // IOUtils.cleanup(LOG, gamma);

            // Transfer query result in natural language
            String cohortField = query.getCohortFields()[0];
            String actionTimeField = tableSchema.getActionTimeFieldName();
            NumericConverter converter = cohortField.equals(actionTimeField) ? new DayIntConverter() : null;
            MetaFieldRS cohortMetaField = metaChunk.getMetaField(cohortField);
            Map<CohortKey, Long> results = gamma.getCubletResults();
            // List<ResultTuple> resultSet = new ArrayList<>();
            for (Map.Entry<CohortKey, Long> entry : results.entrySet()) {
                CohortKey key = entry.getKey();
                int cId = key.getCohort();
                String cohort = converter == null ? cohortMetaField.getString(key.getCohort())
                        : converter.getString(cId);
                int age = key.getAge();
                long measure = entry.getValue();
                resultSet.add(new ResultTuple(cohort, age, measure));
            }
            // Uncomment the following line after sigmod
            // return QueryResult.ok(resultSet);
            // end of uncomment
        }
        return ResultTuple.merge(resultSet);
    }

    public static void main(String[] args) throws IOException {
        // Load/Reload the cube
        cohortModel = new CohortModel(args[0]);
        cohortModel.reload(args[1]);

        ObjectMapper mapper = new ObjectMapper();
        SigmodCohortQuery query = mapper.readValue(new File("cohort-query.json"), SigmodCohortQuery.class);

        // The following query can print the query*/
        System.out.println(query.toPrettyString());

        Map<String, DataOutputStream> map = null;
        if (query.getOutSource() != null) {
            map = new HashMap<>();
            File root = new File("cube/", query.getOutSource());
            File[] versions = root.listFiles(new FileFilter() {
                @Override
                public boolean accept(File pathname) {
                    if (pathname.isDirectory()) return true;
                    return false;
                }
            });
            for (File version : versions) {
                for (File cubletFile : version.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(".dz");
                    }
                })) {
                    map.put(cubletFile.getName(), new DataOutputStream(new FileOutputStream(cubletFile, true)));
                }
            }
        }


        cohortModel.init(query.getDataSource());

        // Process the query and print the result
        long begin = System.currentTimeMillis();
        QueryResult result = QueryResult.ok(executeQuery(cohortModel.getCube(query.getDataSource()), query, map));
        long end = System.currentTimeMillis();

        System.out.println("query elapsed: " + (end - begin));
        System.out.println(result.toString());
        cohortModel.close();
    }

}
