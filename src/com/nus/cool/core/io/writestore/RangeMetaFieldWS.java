package com.nus.cool.core.io.writestore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.primitives.Ints;

import java.io.DataOutput;
import java.io.IOException;

import com.nus.cool.core.cohort.converter.DayIntConverter;
import com.nus.cool.core.cohort.converter.NumericConverter;
import com.nus.cool.core.cohort.converter.WeekIntConverter;
import com.nus.cool.core.cohort.schema.FieldType;
import com.nus.cool.core.lang.Integers;

/**
 * This class does not use any compression.
 * 
 * Layout:
 * -------------------------
 * | fieldType | min | max |
 * -------------------------
 * 
 * @author caiqc, xiezl
 */
public class RangeMetaFieldWS implements MetaFieldWS {

	private int min, max;
	private FieldType fieldType;

	private final NumericConverter intConverter;

	public RangeMetaFieldWS(FieldType type) {
		switch (type) {
		case Day:
		case ActionTime:
			intConverter = new DayIntConverter();
			break;
		case Week:
			intConverter = new WeekIntConverter();
			break;
		default:
			intConverter = null;
			break;
		}

		this.fieldType = type;
		this.min = Integer.MAX_VALUE;
		this.max = Integer.MIN_VALUE;
	}

	@Override
	public int writeTo(DataOutput out) throws IOException {
		int bytesWritten = 0;
		//out.writeByte(fieldType.ordinal());
		//bytesWritten++;

		out.writeInt(Integers.toNativeByteOrder(min));  // write metaChunk -> metaChunkData -> fieldN -> min
		out.writeInt(Integers.toNativeByteOrder(max));  // write metaChunk -> metaChunkData -> fieldN -> max
		bytesWritten += 2 * Ints.BYTES;
		return bytesWritten;
	}

	@Override
	public void put(String v) {
		v = checkNotNull(v);
		String[] range = v.split("\\|");
		checkArgument(range.length == 2);

		switch (this.fieldType) {
		case Metric:
			min = Integer.parseInt(range[0]);
			max = Integer.parseInt(range[1]);
			break;
		case ActionTime:
		case Day:
		case Week:
			min = intConverter.toInt(range[0]);
			max = intConverter.toInt(range[1]);
			break;
		default:
			throw new IllegalArgumentException("Unable to index " + fieldType);
		}

		checkArgument(min <= max);
	}

	@Override
	public int find(String v) {
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public int count() {
		throw new java.lang.UnsupportedOperationException();
	}

	@Override
	public FieldType getFieldType() {
		return this.fieldType;
	}

	@Override
	public void complete() {
	}

}
