/**
 * 
 */
package com.nus.cool.core.cohort.filter;

import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;

import java.util.List;

/**
 * @author david, qingchao
 *
 */
public interface FieldFilter {
	
	double getMinKey();
	
	double getMaxKey();
	
	void updateValues(Double v);

    ExtendedFieldSet getFieldSet();
	
	boolean accept(MetaFieldRS metaField);
	
	boolean accept(FieldRS chunkField);
	
	boolean accept(Double v);
	
	boolean accept(int v);

    int nextAcceptTuple(int start, int to);

    List<String> getValues();
	
}
