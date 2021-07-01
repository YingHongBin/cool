package com.nus.cohana.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nus.cohana.core.cohort.schema.TableSchema;
import com.nus.cohana.core.iceberg.query.Aggregation;
import com.nus.cohana.core.iceberg.query.IcebergAggregation;
import com.nus.cohana.core.iceberg.query.IcebergQuery;
import com.nus.cohana.core.iceberg.query.IcebergSelection;
import com.nus.cohana.core.iceberg.result.BaseResult;
import com.nus.cohana.core.io.readstore.ChunkRS;
import com.nus.cohana.core.io.readstore.CubeRS;
import com.nus.cohana.core.io.readstore.CubletRS;
import com.nus.cohana.core.io.readstore.MetaChunkRS;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * @author yhb
 */
public class LocalIcebergLoader {

    private static CohortModel cohortModel;

    public static List<BaseResult> executeQuery(CubeRS cube, IcebergQuery query) throws Exception {

        long beg;
        long end;
        List<CubletRS> cublets = cube.getCublets();
        TableSchema tableSchema = cube.getTableSchema();
        List<BaseResult> results = new ArrayList<>();

        beg = System.currentTimeMillis();
        IcebergSelection selection = new IcebergSelection();
        selection.init(tableSchema, query);
        end = System.currentTimeMillis();
        //System.out.println("selection init elapsed: " + (end - beg));
        for (CubletRS cubletRS : cublets) {
            MetaChunkRS metaChunk = cubletRS.getMetaChunk();
            beg = System.currentTimeMillis();
            selection.process(metaChunk);
            end = System.currentTimeMillis();
            //System.out.println("selection process meta chunk elapsed: " + (end - beg));
            if (selection.isbActivateCublet()) {
                List<ChunkRS> datachunks = cubletRS.getDataChunks();
                List<BitSet> bitSets = cubletRS.getBitSets();
                //for (ChunkRS dataChunk : datachunks) {
                for (int i = 0; i < datachunks.size(); i++) {
                    ChunkRS dataChunk = datachunks.get(i);
                    BitSet bitSet;
                    if (i >= bitSets.size()) {
                    	bitSet = new BitSet();
                    	bitSet.set(0, dataChunk.records());
                    } else {
                    	bitSet = bitSets.get(i);
                    }
                    if (bitSet.cardinality() == 0) continue;
                    beg = System.currentTimeMillis();
                    Map<String, BitSet> map = selection.process(dataChunk, bitSet);
                    end = System.currentTimeMillis();
                    //System.out.println("selection process data chunk elapsed: " + (end - beg));
                    if (map == null) {
                        continue;
                    }
                    for (Map.Entry<String, BitSet> entry : map.entrySet()) {
                        String timeRange = entry.getKey();
                        BitSet bs = entry.getValue();
                        beg = System.currentTimeMillis();
                        IcebergAggregation icebergAggregation = new IcebergAggregation();
                        icebergAggregation.init(bs, query.getGroupFields(), metaChunk, dataChunk, timeRange);
                        end = System.currentTimeMillis();
                        //System.out.println("init aggregation elapsed: " + (end - beg));
                        for (Aggregation aggregation : query.getAggregations()) {
                            beg = System.currentTimeMillis();
                            List<BaseResult> res = icebergAggregation.process(aggregation);
                            end = System.currentTimeMillis();
                            //System.out.println("aggregation process elapsed: " + (end - beg));
                            results.addAll(res);
                        }
                    }
                }
            }
        }
        results = BaseResult.merge(results);
        return results;
    }

    public static QueryResult wrapResult(CubeRS cube, IcebergQuery query) {
        try {
            List<BaseResult> results = executeQuery(cube, query);
            return QueryResult.ok(results);
        } catch (Exception e) {
            e.printStackTrace();
            return QueryResult.error("something wrong");
        }
    }

    public static void main(String[] args) throws IOException, ParseException {

        cohortModel = new CohortModel(args[0]);
        cohortModel.reload(args[1]);

        ObjectMapper mapper = new ObjectMapper();

        IcebergQuery query = mapper.readValue(new File("fake-data-query.json"), IcebergQuery.class);

        cohortModel.init(query.getDataSource());

        //System.out.println(query.toPrettyString());
        long beg = System.currentTimeMillis();
        QueryResult result = wrapResult(cohortModel.getCube(query.getDataSource()), query);
        long end = System.currentTimeMillis();
        System.out.println("query elapsed: " + (end - beg));
        System.out.println(result.toString());

        /*
        beg = System.currentTimeMillis();
        cohortModel.addCube("append-test", "cube/append-test/000000/16a9145eee7.dz");
        end = System.currentTimeMillis();
        System.out.println("add cublet elapsed: " + (end - beg));

        beg = System.currentTimeMillis();
        result = wrapResult(cohortModel.getCube(query.getDataSource()), query);
        end = System.currentTimeMillis();
        System.out.println("query elapsed: " + (end - beg));
        System.out.println(result.toString());

         */
    }
}
