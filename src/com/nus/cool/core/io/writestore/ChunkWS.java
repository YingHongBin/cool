/**
 * 
 */
package com.nus.cool.core.io.writestore;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.google.common.primitives.Ints;

import com.nus.cool.core.cohort.schema.FieldSchema;
import com.nus.cool.core.cohort.schema.FieldType;
import com.nus.cool.core.cohort.schema.TableSchema;
import com.nus.cool.core.io.ChunkType;
import com.nus.cool.core.io.Output;
import com.nus.cool.core.io.OutputCompressor;
import com.nus.cool.core.io.compression.DefaultCompressorAdviser;
import com.nus.cool.core.io.compression.DefaultCompressorFactory;
import com.nus.cool.core.lang.Integers;

/**
 * Data layout
 * 
 * ---------------------------------------------
 * | chunk data | chunk header | header offset | 
 * --------------------------------------------- 
 * 
 * Chunk data layout: 
 * -------------------------------------
 * | field 1 | field 2 | ... | field N | 
 * -------------------------------------
 * 
 * Chunk header layout: 
 * ---------------------------------------------- 
 * | ChunkType | count | fields | field offsets | 
 * ----------------------------------------------
 * where
 * ChunkType == ChunkType.DATA 
 * count == number of records
 * fields = number of fields
 * 
 * @author david, xiezl
 *
 */
public class ChunkWS implements Output {

	public static ChunkWS newCohortChunk(TableSchema schema,
			MetaFieldWS[] metaFields, int offset) {
		OutputCompressor compressor = new OutputCompressor(
				new DefaultCompressorAdviser(), new DefaultCompressorFactory());
		List<FieldSchema> fieldSchemaList = schema.getFields();
		FieldWS[] fields = new FieldWS[fieldSchemaList.size()];
		int i = 0;
		for (FieldSchema fieldSchema : fieldSchemaList) {
			FieldType fieldType = fieldSchema.getFieldType();
			switch (fieldType) {
			case AppKey:
			case Segment:
			case Action:
			case UserKey:
				fields[i] = new HashFieldWS(fieldType, i, metaFields[i], compressor, fieldSchema.getPreCal());
				break;
			case ActionTime:
			case Metric:
				fields[i] = new RangeFieldWS(fieldType, i, compressor);
				break;
//			case Day:
//			case Week:
//			case Month:
//				fields[i] = new SinceEventFieldWS(i, schema.getAppKeyField(), 
//						schema.getUserKeyField(), schema.getActionField(), fieldSchema.getBirthEvent());
//				break;
			default:
				throw new IllegalArgumentException("FieldType: " + fieldType);
			}
			i++;
		}
		return new ChunkWS(offset, fields);
	}

	/**
	 * Chunk Offset
	 */
	private int off;

	private int count;

	private FieldWS[] fields;

	public ChunkWS(int off, FieldWS[] fields) {
		checkArgument(off >= 0 && fields != null && fields.length > 0);
		this.off = off;
		this.fields = fields;
	}

	/**
	 * Put a tuple into the chunk
	 * 
	 * @param tuple
	 * @throws IOException
	 */
	public void put(String[] tuple) throws IOException {
		count++;
		for (int i = 0; i < tuple.length; i++)
			fields[i].put(tuple);
	}

	/**
	 * The number of records written so far.
	 * 
	 * @return
	 */
	public int count() {
		return count;
	}

	@Override
	public int writeTo(DataOutput out) throws IOException {
		int bytesWritten = 0;
		
		int[] offsets = new int[fields.length];
		
		// Calculate the offset of each field
		// Write chunk data
		for (int i = 0; i < fields.length; i++) {
			offsets[i] = off + bytesWritten;
			bytesWritten += fields[i].writeTo(out);
		}

		// Write header, Calculate the offset of header
		int chunkHeadOff = off + bytesWritten;
		// Write chunkType (DATA)
		out.writeByte(ChunkType.DATA.ordinal());
		bytesWritten++;
		// Write #records (count)
		out.writeInt(Integers.toNativeByteOrder(count));
		bytesWritten += Ints.BYTES;
		// Write #fields (fields)
		out.writeInt(Integers.toNativeByteOrder(fields.length));
		bytesWritten += Ints.BYTES;
		// Write field offsets
		for (int offset : offsets) {
			out.writeInt(Integers.toNativeByteOrder(offset));
			bytesWritten += Ints.BYTES;
		}

		// Write header offset
		out.writeInt(Integers.toNativeByteOrder(chunkHeadOff));
		bytesWritten += Ints.BYTES;
		return bytesWritten;
	}

}
