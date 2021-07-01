/**
 * 
 */
package com.nus.cohana.core.io.readstore;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.util.BitSet;

import com.nus.cohana.core.cohort.schema.FieldType;
import com.nus.cohana.core.io.Codec;
import com.nus.cohana.core.io.InputVector;
import com.nus.cohana.core.io.InputVectorFactory;
import com.nus.cohana.core.io.compression.SimpleBitSetCompressor;

/**
 * @author david
 *
 */
public class CohortFieldRS implements FieldRS {
	
	private FieldType fieldType;
	
	private int minKey;
	
	private int maxKey;
	
	private InputVector keyVec;
	
	private InputVector valueVec;
	
	private InputVectorFactory factory;
	
	private boolean bSetField;
	
	private boolean bRangeField;

	private boolean preCal;

	private BitSet[] bitSets;

	public CohortFieldRS() {
		this(InputVectorFactory.get());
	}
	
	public CohortFieldRS(InputVectorFactory factory) {
		this.factory = checkNotNull(factory);
	}

	@Override
	public FieldType getFieldType() {
		return fieldType;
	}

	@Override
	public InputVector getKeyVector() {
		if(keyVec == null)
			throw new IllegalStateException("Not hash indexed!");
		return keyVec;
	}

	@Override
	public InputVector getValueVector() {
		return valueVec;
	}

	@Override
	public int minKey() {
		return minKey;
	}

	@Override
	public int maxKey() {
		return maxKey;
	}

	@Override
	public void readFrom(ByteBuffer buf) {
		this.fieldType = FieldType.fromInteger(buf.get());
		Codec codec = Codec.fromInteger(buf.get());
		if(codec == Codec.Range) {
			this.minKey = buf.getInt();
			this.maxKey = buf.getInt();
			bRangeField = true;
		} else {
			buf.position(buf.position() - 1);
			this.keyVec = factory.readFrom(buf);
			this.minKey = 0;
			this.maxKey = keyVec.size() - 1;
			bSetField = true;
		}

		codec = Codec.fromInteger(buf.get());
		this.preCal = codec == Codec.PreCal;

		if (this.preCal) {
			int valNumber = buf.get();
			this.bitSets = new BitSet[valNumber];
			//int valueSize = buf.getInt();
			for (int i = 0; i < valNumber; i++) {
				this.bitSets[i] = SimpleBitSetCompressor.read(buf);
			}
		} else {
			buf.position(buf.position() - 1);
			this.valueVec = factory.readFrom(buf);
		}
	}

	public void readFromWithFieldType(ByteBuffer buf, FieldType fieldType) {
		//this.fieldType = FieldType.fromInteger(buf.get());
		this.fieldType = fieldType;
		Codec codec = Codec.fromInteger(buf.get());
		if(codec == Codec.Range) {
			this.minKey = buf.getInt();
			this.maxKey = buf.getInt();
			bRangeField = true;
		} else {
			buf.position(buf.position() - 1);
			this.keyVec = factory.readFrom(buf);
			this.minKey = 0;
			this.maxKey = keyVec.size() - 1;
			bSetField = true;
		}

		codec = Codec.fromInteger(buf.get());
		this.preCal = codec == Codec.PreCal;

		if (this.preCal) {
			int valNumber = buf.get();
			this.bitSets = new BitSet[valNumber];
			//int valueSize = buf.getInt();
			for (int i = 0; i < valNumber; i++) {
				this.bitSets[i] = SimpleBitSetCompressor.read(buf);
			}
		} else {
			buf.position(buf.position() - 1);
			this.valueVec = factory.readFrom(buf);
		}
	}

	@Override
	public boolean isRangeField() {
		return bRangeField;
	}

	@Override
	public boolean isSetField() {
		return bSetField;
	}

	@Override
	public String toString() {
		return String.format("(%s, min = %d, max = %d)", fieldType, minKey, maxKey);
	}

	@Override
	public boolean isPreCal() {
		return this.preCal;
	}
	
	@Override
	public BitSet[] getBitSets() {
		return this.bitSets;
	}

}
