/**
 * 
 */
package com.nus.cool.core.io.readstore;

import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

import com.google.common.collect.Maps;

import com.nus.cool.core.cohort.schema.FieldType;
import com.nus.cool.core.cohort.schema.TableSchema;
import com.nus.cool.core.io.ChunkType;
import com.nus.cool.core.io.Input;

/**
 * @author david, xiezl
 *
 */
public class MetaChunkRS implements Input {

	private TableSchema schema;
	
	private int[] fieldOffsets;
	
	private ByteBuffer buf;

	public Charset getCharset() {
		return charset;
	}

	private Charset charset;

    private Map<Integer, MetaFieldRS> fields;
	
	public MetaChunkRS(TableSchema schema) {
		this.schema = checkNotNull(schema);
		this.charset = Charset.forName(schema.getCharset());
        this.fields = Maps.newHashMap();
	}
	
	public synchronized MetaFieldRS getMetaField(int i, FieldType ft) {
        if(fields.containsKey(i))
            return fields.get(i);

		int fieldOffset = fieldOffsets[i];
		buf.position(fieldOffset);
        //FieldType ft = FieldType.fromInteger(buf.get(fieldOffset));
		MetaFieldRS metaField = null;
        switch(ft) {
            case AppKey:
            case Action:
            case Segment:
            case UserKey:
                metaField = new CohortMetaFieldRS(charset);
                break;
            case ActionTime:
            case Day:
            case Week:
            case Month:
            case Metric:
                metaField = new RangeMetaFieldRS();
                break;      
            default:
            	throw new IllegalArgumentException("Unexpected FieldType: " + ft);
                	
        }
		long begin = System.currentTimeMillis();
		metaField.readFromWithFieldType(buf, ft);
        fields.put(i, metaField);
		return metaField;
	}
	
	public MetaFieldRS getMetaField(String fieldName) {
		int id = schema.getFieldID(fieldName);
		FieldType ft = schema.getFieldType(fieldName);
		return (id < 0 || id >= fieldOffsets.length) ? null : getMetaField(id, ft);
	}

	@Override
	public void readFrom(ByteBuffer buf) {
		this.buf = checkNotNull(buf);
		ChunkType chunkType = ChunkType.fromInteger(buf.get());
		if(chunkType != ChunkType.META)
			throw new IllegalStateException("Expect MetaChunk, but reads " + chunkType);
		
		int fields = buf.getInt();
		this.fieldOffsets = new int[fields];
		for(int i = 0; i < fields; i++)
			fieldOffsets[i] = buf.getInt();
	}

}
