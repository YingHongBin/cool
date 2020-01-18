package com.nus.cool.core.iceberg.aggregator;

import com.nus.cool.core.cohort.schema.FieldType;
import com.nus.cool.core.iceberg.result.AggregatorResult;
import com.nus.cool.core.io.InputVector;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;

import java.util.BitSet;
import java.util.Map;

/**
 * @author yhb
 */
public class CountAggregator implements Aggregator {

    @Override
    public void process(Map<String, BitSet> groups, FieldRS field,
                        Map<String, AggregatorResult> resultMap, MetaFieldRS metaFieldRS) {
        for(Map.Entry<String, BitSet> entry : groups.entrySet()) {
            if(resultMap.get(entry.getKey()) == null) {
                String groupName = entry.getKey();
                AggregatorResult aggregatorResult = new AggregatorResult();
                resultMap.put(groupName, aggregatorResult);
            }
            if(resultMap.get(entry.getKey()).getCount() == null) {
                BitSet bs = entry.getValue();
                int count = 0;
                if(field.getFieldType() == FieldType.Metric) {
                    count = bs.cardinality();
                } else {
                    InputVector key = field.getKeyVector();
                    InputVector value = field.getValueVector();
                    int nullId = metaFieldRS.find("null");
                    for (int i = 0; i < bs.size(); i++) {
                        int nextPos = bs.nextSetBit(i);
                        if (nextPos < 0) {
                            break;
                        }
                        if (key.get(value.get(nextPos)) != nullId) {
                            count += 1;
                        }
                        i = nextPos;
                    }
                }
                resultMap.get(entry.getKey()).setCount(count);
            }
        }
    }
}

