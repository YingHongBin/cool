package com.nus.cohana.core.io.readstore;

import java.nio.ByteBuffer;

import com.nus.cohana.core.cohort.schema.FieldType;

/**
 * @file RangeMetaFieldRS.java
 * @brief 
 * @author qingchao
 */
public class RangeMetaFieldRS implements MetaFieldRS {

    private FieldType fieldType;
    private int min, max;

    public RangeMetaFieldRS() {}

    @Override
    public int getMaxValue() {
        return this.max;
    }

    @Override
    public int getMinValue() {
        return this.min;
    }

	@Override
	public void readFrom(ByteBuffer buf) {
        this.fieldType = FieldType.fromInteger(buf.get());
        this.min = buf.getInt();
        this.max = buf.getInt();
	}

    public void readFromWithFieldType(ByteBuffer buf, FieldType fieldType) {
        //this.fieldType = FieldType.fromInteger(buf.get());
        this.fieldType = fieldType;
        this.min = buf.getInt();
        this.max = buf.getInt();
    }

	@Override
	public FieldType getFieldType() {
        return this.fieldType;
	}

	@Override
	public int find(String v) {
        throw new UnsupportedOperationException();
		//return Integer.parseInt(v);
	}

	@Override
	public int count() {
        throw new UnsupportedOperationException();
	}

	@Override
	public String getString(int i) {
		return (new Integer(i).toString());
        //throw new UnsupportedOperationException();
	}

}
