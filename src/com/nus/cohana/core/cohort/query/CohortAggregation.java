/**
 * 
 */
package com.nus.cohana.core.cohort.query;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

import com.nus.cohana.core.cohort.schema.FieldType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.nus.cohana.core.cohort.filter.FieldFilter;
import com.nus.cohana.core.cohort.schema.CubeSchema;
import com.nus.cohana.core.cohort.schema.TableSchema;
import com.nus.cohana.core.io.InputVector;
import com.nus.cohana.core.io.readstore.ChunkRS;
import com.nus.cohana.core.io.readstore.FieldRS;
import com.nus.cohana.core.io.readstore.MetaChunkRS;
import com.nus.cohana.core.io.readstore.MetaFieldRS;
import com.nus.cohana.core.io.storevector.RLEInputVector;

/**
 * @author david
 *
 */
public class CohortAggregation implements Operator {

    private static Log LOG = LogFactory.getLog(CohortAggregation.class);

    private TableSchema tableSchema;

    private CohortSelection sigma;

    private SigmodCohortQuery query;

    private int totalDataChunks;

    private int totalSkippedDataChunks;

    private int totalUsers;

    private int totalSkippedUsers;

    private String[] birthActions;

    private int[] birthActionGlobalIds;

    private int[] birthActionChunkIds;

    private BitSet bs;

    private Map<CohortKey, Long> cubletResults = new LinkedHashMap<>();

    public BitSet getBs() {
        return this.bs;
    }

    public CohortAggregation(CohortSelection sigma) {
        this.sigma = checkNotNull(sigma);
    }

    private FieldRS loadField(ChunkRS dataChunk, String fieldName) {
        // Retention, i.e., UserCount is implemented as a virtual measure field
        if (fieldName.equals("Retention"))
            return null;
        synchronized (dataChunk) {
            return dataChunk.getField(fieldName);
        }
    }

    private FieldRS loadField(ChunkRS dataChunk, int fieldId) {
        synchronized (dataChunk) {
            return dataChunk.getField(fieldId);
        }
    }

    private int seekToBirthTuple(int beg, int end, InputVector actionInput) {
        int pos = beg - 1;
        for (int id : birthActionChunkIds) {
            pos++;
            for (; pos < end; pos++) {
                if (actionInput.next() == id) {
                    break;
                }
            }
        }
        return pos < end ? pos : end;
    }

    private Aggregator newAggregator() {
        String metric = query.getMetric();
        if (metric.equals("Retention")) {
            return new UserCountAggregator();
        } else {
            return new SumAggregator();
        }
    }

    public Map<CohortKey, Long> getCubletResults() {
        return this.cubletResults;
    }

    @Override
    public void close() throws IOException {
        sigma.close();
        LOG.info(String.format("(totalChunks = %d, totalSkippedChunks = %d, totalUsers = %d, totalSkippedUsers = %d)",
                totalDataChunks, totalSkippedDataChunks, totalUsers, totalSkippedUsers));
    }

    @Override
    public void init(CubeSchema cubeSchema, TableSchema tableSchema, CohortQuery query) {
        throw new UnsupportedOperationException();
    }

    public void init(TableSchema tableSchema, SigmodCohortQuery query) {
        LOG.info("Initializing cohort aggregation operator ...");
        this.tableSchema = checkNotNull(tableSchema);
        this.query = checkNotNull(query);
        this.birthActions = query.getBirthActions();
        sigma.init(tableSchema, query);
    }

    @Override
    public void process(MetaChunkRS metaChunk) {
        LOG.info("Processing metaChunk ...");
        sigma.process(metaChunk);

        int actionField = tableSchema.getActionField();
        MetaFieldRS actionMetaField = metaChunk.getMetaField(actionField, FieldType.Action);
        birthActionGlobalIds = new int[birthActions.length];
        for (int i = 0; i < birthActions.length; i++) {
            int id = actionMetaField.find(birthActions[i]);
            if (id < 0) {
                LOG.error("Unknown birth action: " + birthActions[i]);
                throw new RuntimeException("Unknown birth action");
            }
            birthActionGlobalIds[i] = id;
        }

        // TODO: Although cohort fields is an array, system only support the first one of the array
        String cohortField = query.getCohortFields()[0];
        FieldFilter ff = sigma.getBirthFieldFilter(cohortField);
        MetaFieldRS metaField = metaChunk.getMetaField(cohortField);
        int a = (int) (ff == null ? metaField.getMaxValue() : Math.min(ff.getMaxKey(), metaField.getMaxValue()));
        int b = (int) (ff == null ? metaField.getMinValue() : Math.max(ff.getMinKey(), metaField.getMinValue()));

        checkArgument(a >= b);

        // Estimate the total number of cohorts
        int m = a - b + 1;
        int atField = tableSchema.getActionTimeField();
        MetaFieldRS actionTimeMetaField = metaChunk.getMetaField(atField, FieldType.ActionTime);
        String atFieldName = tableSchema.getFieldSchema(atField).getName();
        ff = sigma.getAgeFieldFilter(atFieldName);

        // Additional col for cohort size
        int n = (int) (ff == null ? actionTimeMetaField.getMaxValue() - actionTimeMetaField.getMinValue() + 1 + 1
                : ff.getMaxKey() - ff.getMinKey() + 1 + 1);
        // LOG.info("C = " + m + ", " + "A = " + n);
        System.out.println("C = " + m + ", " + "A = " + n);
        // this.matrix = new long[m][n];
    }

    @Override
    public boolean isCohortsInCublet() {
        return true;
    }

    @Override
    public void process(ChunkRS chunk) {
        totalDataChunks++;

        sigma.process(chunk);
        if (sigma.isUserActiveChunk() == false) {
            totalSkippedDataChunks++;
            return;
        }

        // If the user is selected into the cohort.
        // Load necessary fields, we need at least four fields
        FieldRS appField = loadField(chunk, tableSchema.getAppKeyField());
        FieldRS userField = loadField(chunk, tableSchema.getUserKeyField());
        FieldRS actionField = loadField(chunk, tableSchema.getActionField());
        FieldRS actionTimeField = loadField(chunk, tableSchema.getActionTimeField());
        FieldRS cohortField = loadField(chunk, query.getCohortFields()[0]);
        FieldRS metricField = loadField(chunk, query.getMetric());
        birthActionChunkIds = new int[birthActionGlobalIds.length];
        for (int i = 0; i < birthActionGlobalIds.length; i++) {
            int id = actionField.getKeyVector().find(birthActionGlobalIds[i]);
            if (id < 0) {
                return;
            }
            birthActionChunkIds[i] = id;
        }

        // System.out.println("CohortField: " + cohortField);
        // System.out.println("ActionTimeField: " + actionTimeField);
        // Local cohort aggregations
        int min = cohortField.minKey();
        int cardinality = cohortField.maxKey() - min + 1;
        // Q: Why add another 1 here?
        // A: We use additional 1 slot storing cohort size.
        int cohortSize = actionTimeField.maxKey() - actionTimeField.minKey() + 1 + 1;
        long[][] chunkResults = new long[cardinality][cohortSize];

        InputVector cohortInput = cohortField.getValueVector();
        InputVector actionTimeInput = actionTimeField.getValueVector();
        InputVector metricInput = metricField == null ? null : metricField.getValueVector();
        Aggregator aggregator = newAggregator();
        int minAllowedAge = 0;
        int maxAllowedAge = cohortSize - 1;
        FieldFilter ageFilter = sigma.getAgeFieldFilter();
        if (ageFilter != null) {
            minAllowedAge = (int) ageFilter.getMinKey();
            maxAllowedAge = (int) ageFilter.getMaxKey();
        }
        aggregator.init(metricInput, actionTimeInput, cohortSize, minAllowedAge, maxAllowedAge, query.getAgeInterval());

        RLEInputVector appInput = (RLEInputVector) appField.getValueVector();
        appInput.skipTo(0);     // I don't know why the position in the end
        RLEInputVector.Block appBlock = new RLEInputVector.Block();
        FieldFilter appFilter = sigma.getAppFieldFilter();
        BitSet bv = new BitSet(chunk.records());
        this.bs = new BitSet(chunk.records()); //record active user

        // Skipping non RLE compressed blocks
        int totalCorruptedUsers = 0;
        while (appInput.hasNext()) {
            appInput.nextBlock(appBlock);
            if (appFilter.accept(appBlock.value)) {
                if (userField.getValueVector() instanceof RLEInputVector == false) {
                    totalCorruptedUsers++;
                    continue;
                }
                RLEInputVector userInput = (RLEInputVector) userField.getValueVector();
                userInput.skipTo(0);                // I don't know why the position in the end
                RLEInputVector.Block userBlock = new RLEInputVector.Block();
                while (userInput.hasNext()) {
                    // Next user RLE block
                    userInput.nextBlock(userBlock);
                    // Skip to the current app
                    if (userBlock.off < appBlock.off)
                        continue;
                    // pass the end of current app
                    if (userBlock.off > appBlock.off + appBlock.len)
                        break;

                    // Find a new user
                    totalUsers++;
                    int beg = userBlock.off;
                    int end = userBlock.off + userBlock.len;
                    InputVector actionInput = actionField.getValueVector();
                    actionInput.skipTo(beg);
                    int birthOff = seekToBirthTuple(beg, end, actionInput);
                    if (birthOff == end) {
                        // LOG.error("User did not perform birth action " + birthAction);
                        totalSkippedUsers++;
                        continue;
                    }

                    // Check if the user is qualified
                    if (sigma.selectUser(birthOff)) {
                        bs.set(beg, end);
                        // Increment cohort size
                        cohortInput.skipTo(birthOff);
                        int cohort = cohortInput.next() - min;
                        chunkResults[cohort][0]++;

                        actionTimeInput.skipTo(birthOff);
                        int birthTime = actionTimeInput.next();

                        int ageOff = birthOff + 1;
                        if (ageOff < end) {
                            // Aggregate age activity tuples
                            bv.set(birthOff + 1, end);
                            sigma.selectAgeActivities(birthOff + 1, end, bv);
                            if (bv.isEmpty() == false)
                                aggregator.processUser(bv, birthTime, birthOff + 1, end, chunkResults[cohort]);
                            bv.clear(birthOff + 1, end);
                        }
                    }
                }
            }
        }
        System.out.println(bs.cardinality());

        if (totalCorruptedUsers > 0)
            LOG.info("Total corrupted users: " + totalCorruptedUsers + " " + totalDataChunks);

        InputVector keyVector = null;
        if (cohortField.isSetField())
            keyVector = cohortField.getKeyVector();
        // Update cohort results
        for (int i = 0; i < cardinality; i++)
            if (chunkResults[i][0] > 0) { // Non-empty cohort
                int cohort = keyVector == null ? i + min : keyVector.get(i + min);
                for (int j = 0; j < cohortSize; j++) {
                    CohortKey key = new CohortKey(cohort, j);
                    long value = 0;
                    if (cubletResults.containsKey(key))
                        value = cubletResults.get(key);
                    if (value + chunkResults[i][j] > 0)
                        cubletResults.put(key, value + chunkResults[i][j]);
                }
            }
    }

}
