/**
 * 
 */
package com.nus.cool.core.io.writestore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataOutput;
import java.io.IOException;

import com.google.common.primitives.Ints;
import com.nus.cool.core.cohort.converter.DayIntConverter;
import com.nus.cool.core.cohort.schema.FieldType;
import com.nus.cool.core.io.Codec;
import com.nus.cool.core.io.DataInputBuffer;
import com.nus.cool.core.io.DataOutputBuffer;
import com.nus.cool.core.io.OutputCompressor;
import com.nus.cool.core.io.compression.CompressType;
import com.nus.cool.core.io.compression.Histogram;
import com.nus.cool.core.lang.Integers;
import com.nus.cool.core.lang.MoreArrays;

/**
 * Range Index, used to store chunk data for four fieldTypes,
 * including ActionTime, Metric.
 * 
 * Data layout
 * 
 * ------------------------------------------
 * | fieldType | codec | min | max | values |
 * ------------------------------------------
 * 
 * min = min of the values
 * max = max of the values
 * values = data in the column (compressed)
 * 
 * @author david, xiezl
 *
 */
public class RangeFieldWS implements FieldWS {
	
	private int i;
	
	private FieldType fieldType;
	
	private DataOutputBuffer buf = new DataOutputBuffer();
	
	private OutputCompressor compressor;
	
	private final DayIntConverter converter = new DayIntConverter(); 
	
	public RangeFieldWS(FieldType fieldType, int i, OutputCompressor compressor) {
		checkArgument(i >= 0);
		this.i = i;
		this.fieldType = fieldType;
		this.compressor = checkNotNull(compressor);
	}

	@Override
	public FieldType getFieldType() {
		return fieldType;
	}

	@Override
	public void put(String[] tuple) throws IOException {
		if (fieldType == FieldType.ActionTime)
			buf.writeInt(converter.toInt(tuple[i]));
		else
			buf.writeInt(Integer.parseInt(tuple[i]));
	}
	
	@Override
	public int writeTo(DataOutput out) throws IOException {
		int bytesWritten = 0;
		
		// Write fieldType
		//out.write(fieldType.ordinal());
		//bytesWritten++;
		
		int[] key = new int[2];
		int[] value = new int[buf.size() / Ints.BYTES];
		
		// Read column data
		try(DataInputBuffer input = new DataInputBuffer()) {
			input.reset(buf);
			for(int i = 0; i < value.length; i++)
				value[i] = input.readInt();
		}
		
		key[0] = MoreArrays.min(value);
		key[1] = MoreArrays.max(value);

		// Write Codec (range)
		out.writeByte(Codec.Range.ordinal());
		bytesWritten++;
		// Write min value
		out.writeInt(Integers.toNativeByteOrder(key[0]));
		bytesWritten += Ints.BYTES;
		// Write max value
		out.writeInt(Integers.toNativeByteOrder(key[1]));
		bytesWritten += Ints.BYTES;
		
		// Write values, i.e., the data within the column
		int min = MoreArrays.min(value);
		int max = MoreArrays.max(value);
		int count = value.length;
		int rawSize = count * Ints.BYTES;
		Histogram hist = Histogram.builder()
				.min(min)
				.max(max)
				.count(count)
				.rawSize(rawSize)
				.compressType(CompressType.ValueFast)
				.build();
		compressor.reset(hist, value, 0, value.length);
		bytesWritten += compressor.writeTo(out);
		return bytesWritten;
	}
	
}
