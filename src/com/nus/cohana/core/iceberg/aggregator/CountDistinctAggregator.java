package com.nus.cohana.core.iceberg.aggregator;

import com.nus.cohana.core.cohort.schema.FieldType;
import com.nus.cohana.core.iceberg.result.AggregatorResult;
import com.nus.cohana.core.io.InputVector;
import com.nus.cohana.core.io.readstore.FieldRS;
import com.nus.cohana.core.io.readstore.MetaFieldRS;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author yhb
 */
public class CountDistinctAggregator implements Aggregator{
    @Override
    public void process(Map<String, BitSet> groups, FieldRS field,
                        Map<String, AggregatorResult> resultMap, MetaFieldRS metaFieldRS) {
        for (Map.Entry<String, BitSet> entry : groups.entrySet()) {
            if (resultMap.get(entry.getKey()) == null) {
                String groupNmae = entry.getKey();
                AggregatorResult aggregatorResult = new AggregatorResult();
                resultMap.put(groupNmae, aggregatorResult);
            }
            AggregatorResult result = resultMap.get(entry.getKey());
            if (result.getCountDistinct() == null) {
                if (field.getFieldType() == FieldType.Metric) {
                    throw new UnsupportedOperationException();
                }
                BitSet bs = entry.getValue();
                InputVector key = field.getKeyVector();
                if (field.isPreCal()) {
                    BitSet[] bitSets = field.getBitSets();
                    Set<Integer> set = new HashSet<>();
                    for (int i = 0; i < bitSets.length; i++) {
                        BitSet bitSet = (BitSet) bitSets[i].clone();
                        bitSet.and(bs);
                        if (bitSet.cardinality() != 0) {
                            set.add(i);
                        }
                    }
                    for (Integer i : set) {
                        result.getDistinctSet().add(metaFieldRS.getString(i));
                    }
                } else {
                    InputVector value = field.getValueVector();
                    Set<Integer> set = new HashSet<>();
                    for (int i = 0; i < bs.size(); i++) {
                        int nextPos = bs.nextSetBit(i);
                        if (nextPos < 0) {
                            break;
                        }
                        set.add(value.get(nextPos));
                        i = nextPos;
                    }
                    for (Integer i : set) {
                        result.getDistinctSet().add(metaFieldRS.getString(key.get(i)));
                    }
                }
                result.setCountDistinct((float) result.getDistinctSet().size());
            }
        }
    }
}
