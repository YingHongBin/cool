/**
 * 
 */
package com.nus.cohana.core.io.writestore;

import java.io.DataOutput;
import java.io.IOException;

import com.nus.cohana.core.cohort.schema.FieldType;

/**
 * This class is designed as a "placeholder" for fields
 * which have not meta field part.
 * 
 * @author david, xiezl
 *
 */
public class NullMetaFieldWS implements MetaFieldWS {
	
	private final FieldType fieldType;
	
	public NullMetaFieldWS(FieldType fieldType) {
		this.fieldType = fieldType;
	}

	@Override
	public int writeTo(DataOutput out) throws IOException {
		return 0;
	}

	@Override
	public void put(String v) {
	}

	@Override
	public int find(String v) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int count() {
		return 0;
	}

	@Override
	public FieldType getFieldType() {
		return fieldType;
	}

	@Override
	public void complete() {
	}

}
