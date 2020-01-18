/**
 * 
 */
package com.nus.cool.core.io.compression;

import com.nus.cool.core.io.Codec;
import com.nus.cool.core.lang.Integers;

/**
 * Default compressor choosing scheme:
 * 
 * KeyString -> LZ4
 * 
 * KeyFinger -> INT32
 * 
 * ValueFast -> Delta
 * 
 * KeyHash -> BitVector, ZInt
 * 
 * KeyRange -> Range
 * 
 * Value -> ZInt, ZBitInt, RLE
 * 
 * @author david
 *
 */
public class DefaultCompressorAdviser implements CompressorAdviser {

	private Codec adviseForKeyHash(Histogram hist) {	
		int max = (int) hist.max();		
		int bitmapLength = BitSetCompressor.numOfBits(max);
		int bitmapBytes = ((bitmapLength - 1) >>> 3) + 1;		
		
		int bytes = Integers.minBytes(max);
		int byteAlignLength = bytes * hist.count();
		
		if (max < 256) {
			if (bitmapBytes <= byteAlignLength)
				return Codec.BitVector;
			else
				return Codec.INT8;
		}
		else if (max < 65536)
			return Codec.INT16;
		else
			return Codec.INT32;
	}

	private Codec adviseForValue(Histogram hist) {	
		
		if (hist.isSorted())
			return Codec.RLE;
		
		int max = (int) hist.max();
		
		int bitLength = Integers.minBits(max);
		int byteLength = Integers.minBytes(max);	
		
		byteLength = (byteLength == 3 ? 4 : byteLength);
		if(bitLength / (byteLength * 8.0) >= 0.7) {
			switch(byteLength) {
			case 1:
				return Codec.INT8;
			case 2:
				return Codec.INT16;
			case 4:
				return Codec.INT32;
			default:
				throw new IllegalArgumentException("Unsupport ByteLength: " + byteLength);
			}
		} else 
			return Codec.INTBIT;
	}

	@Override
	public Codec advise(Histogram hist) {
		CompressType type = hist.compressType();
		switch (type) {
		case KeyFinger:
			return Codec.INT32;
		case KeyString:
			return Codec.LZ4;
		case KeyHash:
			return adviseForKeyHash(hist);
		case ValueFast:
			return Codec.Delta;
		case Value:
			return adviseForValue(hist);
		default:
			throw new IllegalArgumentException("Unsupport " + type);
		}
	}

}
