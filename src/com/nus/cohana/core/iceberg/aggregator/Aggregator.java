package com.nus.cohana.core.iceberg.aggregator;

import com.nus.cohana.core.iceberg.result.AggregatorResult;
import com.nus.cohana.core.io.readstore.FieldRS;
import com.nus.cohana.core.io.readstore.MetaFieldRS;

import java.util.BitSet;
import java.util.Map;

/**
 * @author yhb
 */
public interface Aggregator {

    void process(Map<String, BitSet> groups, FieldRS field, Map<String, AggregatorResult> resultMap, MetaFieldRS metaField);
}
