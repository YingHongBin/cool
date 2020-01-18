package com.nus.cool.core.iceberg.aggregator;

import com.nus.cool.core.iceberg.result.AggregatorResult;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;

import java.util.BitSet;
import java.util.Map;

/**
 * @author yhb
 */
public interface Aggregator {

    void process(Map<String, BitSet> groups, FieldRS field, Map<String, AggregatorResult> resultMap, MetaFieldRS metaField);
}
