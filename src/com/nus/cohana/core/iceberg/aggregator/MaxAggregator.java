package com.nus.cohana.core.iceberg.aggregator;

import com.nus.cohana.core.iceberg.result.AggregatorResult;
import com.nus.cohana.core.io.InputVector;
import com.nus.cohana.core.io.readstore.FieldRS;
import com.nus.cohana.core.io.readstore.MetaFieldRS;
import com.nus.cohana.core.lang.MoreArrays;

import java.util.BitSet;
import java.util.Map;

/**
 * @author yhb
 */
public class MaxAggregator implements Aggregator {

    @Override
    public void process(Map<String, BitSet> groups, FieldRS field,
                        Map<String, AggregatorResult> resultMap, MetaFieldRS metaFieldRS) {
        InputVector value = field.getValueVector();
        for(Map.Entry<String, BitSet> entry : groups.entrySet()) {
            if (resultMap.get(entry.getKey()) == null) {
                String groupName = entry.getKey();
                AggregatorResult aggregatorResult = new AggregatorResult();
                resultMap.put(groupName, aggregatorResult);
            }
            if (resultMap.get(entry.getKey()).getMax() == null) {
                BitSet bs = entry.getValue();
                int max = 0;
                for (int i = 0; i < bs.size(); i++) {
                    int nextPos = bs.nextSetBit(i);
                    if (nextPos < 0) {
                        break;
                    }
                    max = max < value.get(nextPos) ? value.get(nextPos) : max;
                    i = nextPos;
                }
                resultMap.get(entry.getKey()).setMax(max);
            }
        }
    }
}
