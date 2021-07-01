/**
 * 
 */
package com.nus.cohana.core.cohort.filter;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.List;

import com.nus.cohana.core.io.readstore.FieldRS;
import com.nus.cohana.core.io.readstore.MetaFieldRS;

/**
 * @author david, qingchao
 *
 */
public class AgeFieldFilter implements FieldFilter {
	
	private int minAge;
	
	private int maxAge;
	
	public AgeFieldFilter(List<String> values) {
		checkArgument(values != null && values.isEmpty() == false);
		String[] range = values.get(0).split("\\|");
		this.minAge = Integer.parseInt(range[0]);
		this.maxAge = Integer.parseInt(range[1]);
	}

	@Override
	public double getMinKey() {
		return minAge;
	}

	@Override
	public double getMaxKey() {
		return maxAge;
	}

	@Override
	public boolean accept(MetaFieldRS metaField) {
		return true;
	}

	@Override
	public boolean accept(FieldRS chunkField) {
		return true;
	}

	@Override
	public boolean accept(Double v) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public int nextAcceptTuple(int start, int to) {
		throw new UnsupportedOperationException();
	}


	@Override
	public boolean accept(int v) {
		return (v >= minAge && v <= maxAge);
	}

	@Override
	public void updateValues(Double v) {
		throw new UnsupportedOperationException();		
	}

	@Override
	public ExtendedFieldSet getFieldSet() {
		throw new UnsupportedOperationException();	
	}

	@Override
	public List<String> getValues() {
		throw new UnsupportedOperationException();
	}

}
