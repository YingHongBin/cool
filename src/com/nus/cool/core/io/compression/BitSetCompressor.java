/**
 * 
 */
package com.nus.cool.core.io.compression;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

/**
 * Compress a list of integers with BitVector encoding. The final
 * BitSet is encoded in native byte order format.
 * 
 * Data layout:
 * 
 * ---------------
 * | len | words |
 * ---------------
 * 
 * where len is the number of words. Each word is a 64bit computer word.
 * 
 * @author David
 *
 */
public class BitSetCompressor implements Compressor {
	
	private BitSet bitSet;
	
	private int maxLength;
	
	private static int numOfLongs(int i) {
        //i = (i == 0) ? 1 : i;
		//return ((i - 1) >>> 6) + 1;
		return ((i) >>> 6) + 1;
	}
	
	public static int numOfBits(int i) {
		return numOfLongs(i) << 6;
	}
	
	public BitSetCompressor(Histogram hist) {
		int bitLength = numOfBits((int) (hist.max()));
		this.bitSet = new BitSet(bitLength);
		this.maxLength = (bitLength >>> 3) + 1;
	}

	@Override
	public int maxCompressedLength() {
		return maxLength;
	}

	@Override
	public int compress(byte[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int compress(int[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		for(int i = srcOff; i < srcOff + srcLen; i++)
			bitSet.set(src[i]);
		long[] words = bitSet.toLongArray();
		ByteBuffer buf = ByteBuffer.wrap(dest, destOff, maxDestLen);
		buf.order(ByteOrder.nativeOrder());
		buf.put((byte) words.length);
		for(long w : words)
			buf.putLong(w);
		return buf.position();
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
	
}
