package com.nus.cohana.core.io.compression;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.nus.cohana.core.io.Codec;

import com.google.common.primitives.Ints;

/**
 * Delta compression scheme. The data layout is as follows
 * 
 * ---------------------------------------------------
 * | min | max | zInt Codec | zInt compressed values |
 * ---------------------------------------------------
 * 
 * @author qingchao
 * @David add codec for ZIntCompressor type
 */
public class FoRCompressor implements Compressor {

	public static Codec CODEC = Codec.Delta;
	
	// Head account
	public static final int HEADACC = 8 + 1;

	private int numOfVal;
	
	private int minValue;
	
	private int maxValue;

	public FoRCompressor(Histogram hist) {
		this.numOfVal = hist.count();
	}

	@Override
	public int maxCompressedLength() {
		return (this.numOfVal * Ints.BYTES) + ZIntCompressor.HEADACC + HEADACC;
	}

	private void getMetaData(int[] src, int srcOff, int srcLen) {
		numOfVal = srcLen;
		minValue = src[0];
		maxValue = src[0];

		for (int i = 1; i < srcLen; i++) {
			int cur = src[i + srcOff];
			if (minValue > cur)
				minValue = cur;
			if (maxValue < cur)
				maxValue = cur;
		}
	}

	@Override
	public int compress(byte[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int compress(int[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {

		getMetaData(src, srcOff, srcLen);

		// prepare data for compression
		int[] valuesToCompress = new int[srcLen];
		for (int i = 0; i < srcLen; i++) {
			valuesToCompress[i] = src[srcOff + i] - minValue;
		}

		ByteBuffer buffer = ByteBuffer.wrap(dest, destOff, maxDestLen);
		buffer.order(ByteOrder.nativeOrder());
		buffer.putInt(minValue);
		buffer.putInt(maxValue);

		int maxDelta = maxValue - minValue;

		Histogram hist = Histogram.builder()
				.max(maxDelta)
				.count(numOfVal)
				.build();
		buffer.put((byte) ZIntCompressor.getCodec(hist).ordinal());
		ZIntCompressor compressor = new ZIntCompressor(hist);
		int offset = HEADACC;
		return HEADACC + compressor.compress(valuesToCompress, 0, srcLen, 
				dest, destOff + offset, maxDestLen - offset);
	}

	@Override
	public int compress(long[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int compress(float[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return String.format("(numOfVal = %d)", numOfVal);
	}
}
