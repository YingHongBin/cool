/**
 * 
 */
package com.nus.cohana.core.io.writestore;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import com.nus.cohana.core.io.compression.SimpleBitSetCompressor;
import com.nus.cohana.core.lang.Integers;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

import com.nus.cohana.core.cohort.schema.FieldType;
import com.nus.cohana.core.io.Codec;
import com.nus.cohana.core.io.DataInputBuffer;
import com.nus.cohana.core.io.DataOutputBuffer;
import com.nus.cohana.core.io.OutputCompressor;
import com.nus.cohana.core.io.compression.CompressType;
import com.nus.cohana.core.io.compression.CompressorAdviser;
import com.nus.cohana.core.io.compression.Histogram;
import com.nus.cohana.core.lang.MoreArrays;

/**
 * Hash-like indexed field, used to store chunk data for four fieldTypes,
 * including AppKey, Segment, Action, UserKey.
 * 
 * Data Layout:
 * 
 * -----------------------------
 * | fieldType | keys | values |
 * -----------------------------
 * 
 * keys = globalIDs (compressed)
 * values = column data (compressed)
 * 
 * @author david, xiezl
 *
 */
public class HashFieldWS implements FieldWS {
	
	private static Log LOG = LogFactory.getLog(HashFieldWS.class);

	private final int i;

	private final MetaFieldWS metaField;

	private final OutputCompressor compressor;

	private final FieldType fieldType;

	/**
	 * Convert globalID to localID
	 * Key: globalID
	 * Value: localID
	 */
	private Map<Integer, Integer> idMap = Maps.newTreeMap();

	private DataOutputBuffer buf = new DataOutputBuffer();

	private List<BitSet> bitSetList = new ArrayList<>();

	private Boolean preCal;

	public HashFieldWS(FieldType fieldType, int i, MetaFieldWS metaField,
			OutputCompressor compressor, boolean preCal) {
		checkArgument(i >= 0);
		this.fieldType = fieldType;
		this.i = i;
		this.metaField = checkNotNull(metaField);
		this.compressor = checkNotNull(compressor);
		this.preCal = preCal;
	}

	@Override
	public FieldType getFieldType() {
		return fieldType;
	}

	@Override
	public void put(String[] tuple) throws IOException {
		int gId = metaField.find(tuple[i]);
		// TODO: may need to shutdown the loading since
		// the data may be corrupted
		if (gId == -1) {
			System.out.println(String.format("Invalid value %s in field %s", tuple[i], i));
			LOG.info(String.format("Invalid value %s in field %s", tuple[i], i));
		}
		buf.writeInt(gId);
		// Set localId as 0 for now.
		// It will be changed in writeTo
		idMap.put(gId, 0);
	}

	@Override
	public int writeTo(DataOutput out) throws IOException {
		int bytesWritten = 0;
		
		// Write field type
		//out.write(fieldType.ordinal());
		//bytesWritten++;

		int size = buf.size() / Ints.BYTES;

		// Step 1: produce chunk index & assign chunk id
		// Store globalID in order
		int[] key = new int[idMap.size()];
		int i = 0;
		for (Map.Entry<Integer, Integer> en : idMap.entrySet()) {
		    // Store localId into the map
			key[i] = en.getKey();
			en.setValue(i);
			if (this.preCal) {
				BitSet bs = new BitSet(size);
				bitSetList.add(bs);
			}
			i++;
		}

		// Step 2: produce chunk id list, i.e., the data in the chunk
		//int size = buf.size() / Ints.BYTES;
		int[] value = new int[size];
		try (DataInputBuffer input = new DataInputBuffer()) {
			input.reset(buf);
			for (i = 0; i < size; i++) {
				int id = input.readInt();
				value[i] = idMap.get(id);
				if (this.preCal) {
					bitSetList.get(idMap.get(id)).set(i);
				}
			}
		}

		int min, max, count, rawSize;
		Histogram hist;

		// Step 3: write compressed key， i.e., globalID
		min = MoreArrays.min(key);
		max = MoreArrays.max(key);
		count = key.length;
		rawSize = count * Ints.BYTES;
		hist = Histogram.builder().min(min).max(max).count(count).rawSize(rawSize)
				.compressType(CompressType.KeyHash).build();
		compressor.reset(hist, key, 0, key.length);
		bytesWritten += compressor.writeTo(out);

		if (this.preCal) {
			out.write(11);
			bytesWritten++;
			// only support value number < 128
			out.write(this.bitSetList.size());
			bytesWritten ++;
			//out.writeInt(Integers.toNativeByteOrder(size));
			//bytesWritten += Ints.BYTES;
			for (BitSet bs : bitSetList) {
				bytesWritten += SimpleBitSetCompressor.compress(bs, out);
			}
		} else {
			// Step 4: write compressed value, i.e., the data within the column
			min = MoreArrays.min(value);
			max = MoreArrays.max(value);
			count = value.length;
			rawSize = count * Ints.BYTES;
			hist = Histogram.builder().sorted(fieldType == FieldType.AppKey || fieldType == FieldType.UserKey)
					.min(min).max(max).count(count).rawSize(rawSize)
					.compressType(CompressType.Value).build();
			CompressorAdviser adviser = compressor.getCompressorAdviser();
			if (fieldType == FieldType.UserKey) {
				Codec codec = adviser.advise(hist);
				if (codec != Codec.RLE)
					System.out.println("UserKey is compressed by " + adviser.advise(hist));
			}
			compressor.reset(hist, value, 0, value.length);
			bytesWritten += compressor.writeTo(out);
		}

		return bytesWritten;
	}

}
