/**
 * 
 */
package com.nus.cohana.core.io.writestore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;

import com.google.common.primitives.Ints;

import com.nus.cohana.core.cohort.schema.FieldSchema;
import com.nus.cohana.core.cohort.schema.FieldType;
import com.nus.cohana.core.cohort.schema.TableSchema;
import com.nus.cohana.core.io.ChunkType;
import com.nus.cohana.core.io.Output;
import com.nus.cohana.core.io.OutputCompressor;
import com.nus.cohana.core.io.compression.DefaultCompressorAdviser;
import com.nus.cohana.core.io.compression.DefaultCompressorFactory;
import com.nus.cohana.core.lang.Integers;

/**
 * WS = WriteStore
 * MetaChunk Data layout
 * 
 * ------------------------------------------
 * | MetaChunkData | header | header offset |
 * ------------------------------------------
 * 
 * MetaChunkData layout: 
 * ------------------------------------- 
 * | field 1 | field 2 | ... | field N | 
 * -------------------------------------
 * 
 * header layout: 
 * -------------------------------------- 
 * | ChunkType | fields | field offsets | 
 * --------------------------------------
 * where
 * ChunkType = ChunkType.META 
 * fields = number of fields
 *  
 * @author david, xiezl
 */
public class MetaChunkWS implements Output {

	private final int offset;
	
	private final MetaFieldWS[] metaFields;
	
	private final TableSchema schema;
	
	public static MetaChunkWS newCohortMetaChunkWS(TableSchema schema, int offset) {
		OutputCompressor compressor = new OutputCompressor(
				new DefaultCompressorAdviser(), new DefaultCompressorFactory());
		Charset charset = Charset.forName(schema.getCharset());
		int n = schema.fields();
		MetaFieldWS[] metaFields = new MetaFieldWS[n];
		for (int i = 0; i < metaFields.length; i++) {
			FieldSchema fieldSchema = schema.getFieldSchema(i);
			FieldType fieldType = fieldSchema.getFieldType();
			switch (fieldType) {
			case AppKey:
			case Action:
			case Segment:
			case UserKey:
				metaFields[i] = new CohortMetaFieldWS(fieldType, charset, compressor);
				break;
            case ActionTime:
            case Day:
            case Week:
            case Month:
            case Metric:
                metaFields[i] = new RangeMetaFieldWS(fieldType);
                break;
			default:
				metaFields[i] = new NullMetaFieldWS(fieldType);
				break;
			}
		}
		return new MetaChunkWS(schema, offset, metaFields);
	}
	
	public MetaChunkWS(TableSchema schema, int offset, MetaFieldWS[] metaFields) {
		checkArgument(offset >= 0 && metaFields != null && metaFields.length > 0);
		this.offset = offset;
		this.metaFields = metaFields;
		this.schema = checkNotNull(schema);
	}
	
	/**
	 * Put a tuple into the meta chunk
	 * 
	 * @param tuple
	 * @throws IOException
	 */
	public void put(String[] tuple) throws IOException {
		checkArgument(tuple != null);
		try {
			checkArgument(tuple.length == 2);
		} catch (IllegalArgumentException e) {
			for (String i : tuple){
				System.out.println(i);
			}
			throw new IllegalArgumentException();
		}
		int i = schema.getFieldID(tuple[0]);
		metaFields[i].put(tuple[1]);

	}
	
	public void complete() {
		for(MetaFieldWS meta : metaFields)
			meta.complete();
	}
	
	public MetaFieldWS[] getMetaFields() {
		return metaFields;
	}
	
	@Override
	public int writeTo(DataOutput out) throws IOException {
		int bytesWritten = 0;

        // metaChunk -> header -> fieldoffsets
		int[] offsets = new int[metaFields.length];
		// metaChunk -> metaChunkData
		for(int i = 0; i < metaFields.length; i++) {
			offsets[i] = offset + bytesWritten;
			bytesWritten += metaFields[i].writeTo(out); //write metaChunk -> metaChunkData -> fieldN
		}
		
		// metaChunk -> header offset
		int headOffset = offset + bytesWritten;
		// Write ChunkType
		out.writeByte(ChunkType.META.ordinal());    // write metaChunk -> header -> chunkType
		bytesWritten++;
		
		// Write #fields
		out.writeInt(Integers.toNativeByteOrder(metaFields.length));    // write metaChunk -> header -> # fields
		bytesWritten += Ints.BYTES;
		
		// Write metaChunk -> header -> field offsets
		for(int off : offsets) {
			out.writeInt(Integers.toNativeByteOrder(off));
			bytesWritten += Ints.BYTES;
		}
		
		// Write metaChunk -> header offset
		out.writeInt(Integers.toNativeByteOrder(headOffset));
		bytesWritten += Ints.BYTES;
		return bytesWritten;
	}
}
