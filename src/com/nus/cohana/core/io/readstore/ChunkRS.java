/**
 * 
 */
package com.nus.cohana.core.io.readstore;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;

import com.nus.cohana.core.cohort.schema.TableSchema;
import com.nus.cohana.core.io.ChunkType;
import com.nus.cohana.core.io.Input;

/**
 * @author david, xiezl
 *
 */
public class ChunkRS implements Input {
	
	private TableSchema schema;
	
	private int records;
	
	private int[] fieldOffsets;
	
	private ByteBuffer buf;

	private FieldRS[] fields;
	
	public ChunkRS(TableSchema schema) {
		this.schema = checkNotNull(schema);
	}
	
	public int records() {
		return this.records;
	}
	
	public FieldRS getField(String fieldName) {
		return getField(schema.getFieldID(fieldName));
	}

	/*
	public synchronized FieldRS getField(int i) {
        if (i < 0 || i >= fieldOffsets.length) return null;
		int fieldOffset = fieldOffsets[i];
		buf.position(fieldOffset);
		FieldRS field = new CohortFieldRS();
		field.readFrom(buf);
		return field;
	}
	 */

	public FieldRS getField(int i) {
		return this.fields[i];
	}

	@Override
	public void readFrom(ByteBuffer buf) {
		this.buf = checkNotNull(buf);
		// Get ChunkType
		ChunkType chunkType = ChunkType.fromInteger(buf.get());
		if(chunkType != ChunkType.DATA)
			throw new IllegalStateException("Expect DATA chunk, but reads " + chunkType);
		
		// Get #records
		this.records = buf.getInt();
		// Get #fields
		int fields = buf.getInt();
		// Get field offsets
		this.fieldOffsets = new int[fields];
		for(int i = 0; i < fields; i++)
			fieldOffsets[i] = buf.getInt();

		this.fields = new FieldRS[fields];
		for(int i = 0; i < fields; i++) {
			buf.position(fieldOffsets[i]);
			FieldRS field = new CohortFieldRS();
			field.readFromWithFieldType(buf, this.schema.getFieldSchema(i).getFieldType());
			this.fields[i] = field;
		}
	}

}
