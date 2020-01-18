/**
 * 
 */
package com.nus.cool.core.io.readstore;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.planetj.math.rabinhash.RabinHashFunction32;

import com.nus.cool.core.cohort.schema.FieldType;
import com.nus.cool.core.io.InputVector;
import com.nus.cool.core.io.InputVectorFactory;
import com.nus.cool.core.io.storevector.LZ4InputVector;

/**
 * @author david
 *
 */
public class CohortMetaFieldRS implements MetaFieldRS {
	
	private static final RabinHashFunction32 rhash = RabinHashFunction32.DEFAULT_HASH_FUNCTION;
	
	private FieldType fieldType;
	
	private InputVectorFactory factory;

	private InputVector fingerVec;
	
	private InputVector valueVec;
	
	private Charset charset;
	
	public CohortMetaFieldRS(InputVectorFactory factory, Charset charset) {
		this.factory = checkNotNull(factory);
		this.charset = checkNotNull(charset);
	}
	
	public CohortMetaFieldRS(Charset charset) {
		this(InputVectorFactory.get(), charset);
	}

	@Override
	public FieldType getFieldType() {
		return fieldType;
	}

	@Override
	public int find(String v) {
		return fingerVec.find(rhash.hash(v));
	}

	@Override
	public int count() {
		return fingerVec.size();
	}

	@Override
	public String getString(int i) {
		return ((LZ4InputVector)valueVec).getString(i, charset);
	}
	
	@Override
	public int getMaxValue() {
		//throw new UnsupportedOperationException();
		// Suppose it is a set field
		return count() - 1;
	}
	
	@Override
	public int getMinValue() {
		//throw new UnsupportedOperationException();
		return 0;
	}

	@Override
	public void readFrom(ByteBuffer buf) {
		this.fieldType = FieldType.fromInteger(buf.get());
		this.fingerVec = factory.readFrom(buf);
		if(fieldType == FieldType.Action || fieldType == FieldType.Segment || fieldType == FieldType.UserKey) {
			this.valueVec = factory.readFrom(buf);
		}
	}

	public void readFromWithFieldType(ByteBuffer buf, FieldType fieldType) {
		//this.fieldType = FieldType.fromInteger(buf.get());
		this.fieldType = fieldType;
		this.fingerVec = factory.readFrom(buf);
		if(fieldType == FieldType.Action || fieldType == FieldType.Segment || fieldType == FieldType.UserKey) {
			this.valueVec = factory.readFrom(buf);
		}
	}

	@Override
	public String toString() {
		if(fieldType == FieldType.AppKey)
			return fingerVec.toString() + " []";
		
		int c = count();
		StringBuilder builder = new StringBuilder();
		builder.append('[');
		for(int i = 0; i < c - 1; i++)
			builder.append(getString(i)).append(", ");
		builder.append(getString(c - 1)).append(']');
		return fingerVec.toString() + " " + builder.toString();
	}	

}
