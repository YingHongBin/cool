package com.nus.cohana.core.cohort.filter;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.nus.cohana.core.io.InputVector;
import com.nus.cohana.core.io.readstore.FieldRS;
import com.nus.cohana.core.io.readstore.MetaFieldRS;

public class BirthFilter implements FieldFilter {
	
	private InputVector values;
	private int curValue;
	
	public BirthFilter(){
	}

	@Override
	public double getMinKey() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public double getMaxKey() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean accept(MetaFieldRS metaField) {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean accept(FieldRS chunkField) {
		this.values = chunkField.getValueVector();	
		return true;
	}
	
	public void setCurUserBirthOff(int offset) {
		this.values.skipTo(offset);
		this.curValue = values.next();
	}

	@Override
	public boolean accept(Double v) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public boolean accept(int v) {
		return v == curValue;
	}
	
	@Override
	public int nextAcceptTuple(int start, int to) {		
		values.skipTo(start);		
		while(start < to && !accept(values.next())) ++start;
		return start;
	}

	@Override
	public void updateValues(Double v) {
		throw new UnsupportedOperationException();		
	}

	@Override
	public ExtendedFieldSet getFieldSet() {
		// TODO Auto-generated method stub
		throw new UnsupportedOperationException();		
	}

	@Override
	public List<String> getValues() {
		throw new UnsupportedOperationException();
	}

}
