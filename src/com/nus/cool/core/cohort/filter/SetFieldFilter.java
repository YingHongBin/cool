/**
 * 
 */
package com.nus.cool.core.cohort.filter;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

import java.util.BitSet;
import java.util.List;

import com.nus.cool.core.cohort.converter.NumericConverter;
import com.nus.cool.core.io.InputVector;
import com.nus.cool.core.io.readstore.FieldRS;
import com.nus.cool.core.io.readstore.MetaFieldRS;
import com.nus.cool.core.lang.MoreArrays;

/**
 * @author david, qingchao
 *
 */
public class SetFieldFilter implements FieldFilter {
	
	private List<String> values;
	
	private int[] cubeIDs;
	
	private BitSet filter;
	
	private boolean isAll = false;
	
	private InputVector chunkValues;
	
	private ExtendedFieldSet fieldSet;
	
	public SetFieldFilter(ExtendedFieldSet set, List<String> values, NumericConverter conv) {
        this.values = checkNotNull(values);
        this.isAll = this.values.contains("ALL");
        this.cubeIDs = isAll ? new int[2] : new int[values.size()];		
		this.fieldSet = set;
	}

	@Override
	public double getMinKey() {
		return MoreArrays.min(cubeIDs);
	}

	@Override
	public double getMaxKey() {
		return MoreArrays.max(cubeIDs);
	}

	@Override
	public boolean accept(MetaFieldRS metaField) {
		if(isAll) {
			cubeIDs[1] = metaField.count() - 1;
			return true;
		}
		
		boolean bHit = false;
		int i = 0;
		for(String v : values) {
			int tmp = metaField.find(v);
			cubeIDs[i++] = tmp;
			bHit |= (tmp >= 0);
		}
		return bHit || (values.isEmpty());
	}

	@Override
	public boolean accept(FieldRS chunkField) {
		if(isAll)
			return true;

		InputVector keyVec = chunkField.getKeyVector();
		this.filter = new BitSet(keyVec.size());
		this.chunkValues = chunkField.getValueVector();

		boolean bHit = false;
		for(int i = 0; i < cubeIDs.length; i++) {
			int cId = cubeIDs[i];
			if(cId >= 0) {
				int tmp = keyVec.find(cId);
				bHit |= (tmp >= 0);
				if(tmp >= 0)
					filter.set(tmp);
			}
		}
		return bHit || (values.isEmpty());
	}

	@Override
	public boolean accept(int v) {
		if(isAll)
			return true;
		return filter.get(v);
	}
	
	@Override
	public int nextAcceptTuple(int start, int to) {
		chunkValues.skipTo(start);		
		while(start < to && !accept(chunkValues.next())) ++start;
		return start;
	}

	@Override
	public boolean accept(Double v) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void updateValues(Double v) {
		this.filter.clear();
		this.filter.set(v.intValue());
	}

	@Override
	public ExtendedFieldSet getFieldSet() {
		return this.fieldSet;
	}

	@Override
	public List<String> getValues() {
		return this.values;
	}
}
