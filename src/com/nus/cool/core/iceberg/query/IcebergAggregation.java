package com.nus.cool.core.iceberg.query;


import com.nus.cool.core.cohort.schema.FieldType;
import com.nus.cool.core.iceberg.aggregator.Aggregator;
import com.nus.cool.core.iceberg.aggregator.AggregatorFactory;
import com.nus.cool.core.iceberg.aggregator.AggregatorFactory.AggregatorType;
import com.nus.cool.core.iceberg.result.AggregatorResult;
import com.nus.cool.core.iceberg.result.BaseResult;
import com.nus.cool.core.io.InputVector;
import com.nus.cool.core.io.readstore.ChunkRS;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaChunkRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import org.apache.commons.collections.map.HashedMap;

import java.util.*;

/**
 * @author yhb
 */
public class IcebergAggregation {

    public enum GroupType {

        STRING,

        NUMERIC
    }

    private String timeRange;

    private Map<String, BitSet> group = new HashMap<>();

    private List<Map<String, BitSet>> groups = new ArrayList<>();

    private ChunkRS dataChunk;

    private MetaChunkRS metaChunk;

    private AggregatorFactory aggregatorFactory = new AggregatorFactory();

    private String getString(FieldRS field, MetaFieldRS metaField, int value, GroupType type) {
        switch (type) {
            case STRING: {
                InputVector key = field.getKeyVector();
                return metaField.getString(key.get(value));
            }
            case NUMERIC: {
                return String.valueOf(value);
            }
            default:
                throw new UnsupportedOperationException();
        }
    }

    private void group(FieldRS field, BitSet bs, MetaFieldRS metaField, GroupType type) {
        long beg = System.currentTimeMillis();
        Map<String, BitSet> group = new HashMap<>();
        if (field.isPreCal()) {
            InputVector key = field.getKeyVector();
            for (int i = 0; i < key.size(); i++) {
                String value = getString(field, metaField, i, type);
                BitSet bv = (BitSet) bs.clone();
                bv.and(field.getBitSets()[i]);
                if (bv.cardinality() == 0) continue;
                group.put(value, bv);
            }
        } else {
            Map<Integer, BitSet> id2Bs = new HashedMap();
            InputVector value = field.getValueVector();
            int count = 0;
            for (int i = 0; i < value.size(); i++) {
                int nextPos = bs.nextSetBit(i);
                count += 1;
                if (nextPos < 0) {
                    break;
                }
                value.skipTo(nextPos);
                int id = value.next();
                if (id2Bs.get(id) == null) {
                    BitSet groupBs = new BitSet(bs.size());
                    groupBs.set(nextPos);
                    id2Bs.put(id, groupBs);
                } else {
                    BitSet groupBs = id2Bs.get(id);
                    groupBs.set(nextPos);
                }
                i = nextPos;
            }
            for (Map.Entry<Integer, BitSet> entry : id2Bs.entrySet()) {
                group.put(getString(field, metaField, entry.getKey(), type), entry.getValue());
            }
        }
        this.groups.add(group);
        long end = System.currentTimeMillis();
        //System.out.println("group elapsed: " + (end - beg));
    }

    private boolean checkOperatorIllegal(FieldType fieldType, AggregatorType aggregatorType) {
        switch (aggregatorType) {
            case SUM:
            case AVERAGE:
            case MAX:
            case MIN: {
                if (!fieldType.equals(FieldType.Metric)) {
                    return false;
                }
            }
            case COUNT:
                return true;
            case DISTINCTCOUNT:
                return !fieldType.equals(FieldType.Metric);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private void mergeGroups() {
        this.group = this.groups.get(0);
        for(int i = 1; i < this.groups.size(); i++) {
            Map<String, BitSet> next = this.groups.get(i);
            Map<String, BitSet> merged = new HashMap<>();
            for (Map.Entry<String, BitSet> entry : this.group.entrySet()) {
                for(Map.Entry<String, BitSet> nextEntry : next.entrySet()) {
                    String groupName = entry.getKey() + "|" + nextEntry.getKey();
                    BitSet bs = (BitSet) entry.getValue().clone();
                    bs.and(nextEntry.getValue());
                    merged.put(groupName, bs);
                }
            }
            this.group = merged;
        }
    }

    public void init(BitSet bs, List<String> groupbyFields, MetaChunkRS metaChunk,
                     ChunkRS dataChunk, String timeRange) {
        this.timeRange = timeRange;
        this.dataChunk = dataChunk;
        this.metaChunk = metaChunk;
        if (groupbyFields == null) {
            this.group.put("all", bs);
            return;
        }
        for(String groupbyField : groupbyFields) {
            MetaFieldRS metaField = metaChunk.getMetaField(groupbyField);
            FieldRS field = dataChunk.getField(groupbyField);
            switch (field.getFieldType()) {
                case UserKey:
                case ActionTime:
                case Action:
                case Segment:
                    group(field, bs, metaField, GroupType.STRING);
                    break;
                case Metric:
                    group(field, bs, metaField, GroupType.NUMERIC);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupport field type: " + field.getFieldType());
            }
        }
        mergeGroups();
    }

    public List<BaseResult> process(Aggregation aggregation) {
        String fieldName = aggregation.getFieldName();
        FieldType fieldType = this.metaChunk.getMetaField(fieldName).getFieldType();;
        Map<String, AggregatorResult> resultMap = new HashMap<>();
        for (AggregatorType aggregatorType : aggregation.getOperators()) {
            if (!checkOperatorIllegal(fieldType, aggregatorType)) {
                throw new IllegalArgumentException(fieldName + " can not process " + aggregatorType);
            }
            Aggregator aggregator = this.aggregatorFactory.create(aggregatorType);
            FieldRS field = this.dataChunk.getField(fieldName);
            aggregator.process(this.group, field, resultMap, this.metaChunk.getMetaField(fieldName));
        }
        List<BaseResult> results = new ArrayList<>();
        for (Map.Entry<String, AggregatorResult> entry : resultMap.entrySet()) {
            BaseResult result = new BaseResult();
            result.setTimeRange(this.timeRange);
            result.setFieldName(fieldName);
            result.setKey(entry.getKey());
            result.setAggregatorResult(entry.getValue());
            results.add(result);
        }
        return results;
    }
}
