/**
 * 
 */
package com.nus.cohana.core.io.compression;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.nus.cohana.core.io.Codec;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

/**
 * LZ4Compressor for compressing string values.
 * 
 * The compressed data layout
 * 
 * -------------------------------------
 * | zlen | rawLen | compressed values |
 * -------------------------------------
 * 
 * @author David
 *
 */
public class LZ4JavaCompressor implements Compressor {

	public static Codec CODEC = Codec.LZ4;

	public static int HEADACC = 4 + 4;

	private LZ4Compressor lz4;

	private int maxLen;

	public LZ4JavaCompressor(Histogram hist) {
		this(LZ4Factory.fastestInstance().fastCompressor(), hist);
	}

	public LZ4JavaCompressor(LZ4Compressor lz4, Histogram hist) {
		this.lz4 = lz4;
		this.maxLen = lz4.maxCompressedLength(hist.rawSize()) + HEADACC;
	}

	@Override
	public int maxCompressedLength() {
		return maxLen;
	}

	@Override
	public int compress(byte[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		ByteBuffer buf = ByteBuffer.wrap(dest, destOff, maxDestLen).order(ByteOrder.nativeOrder());
		int zLen = lz4.compress(src, srcOff, srcLen, dest, destOff + HEADACC, maxDestLen - HEADACC);
		int rawLen = srcLen;
		// save zLen and rawLen for uncompressing
		buf.putInt(zLen);
		buf.putInt(rawLen);
		return HEADACC + zLen;
	}

	@Override
	public int compress(int[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		ByteBuffer buf = ByteBuffer.allocate(srcLen << 2);
		for (int i = srcOff; i < srcOff + srcLen; i++) {
			buf.putInt(src[i]);
		}
		return lz4.compress(buf.array(), buf.arrayOffset(), buf.position(), dest,
				destOff, maxDestLen);
	}

	@Override
	public int compress(long[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		ByteBuffer buf = ByteBuffer.allocate(srcLen << 3);
		for (int i = srcOff; i < srcOff + srcLen; i++) {
			buf.putLong(src[i]);
		}
		return lz4.compress(buf.array(), buf.arrayOffset(), buf.position(), dest,
				destOff, maxDestLen);
	}

	@Override
	public int compress(float[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		ByteBuffer buf = ByteBuffer.allocate(srcLen << 2);
		for (int i = srcOff; i < srcOff + srcLen; i++) {
			buf.putFloat(src[i]);
		}
		return lz4.compress(buf.array(), buf.arrayOffset(), buf.position(), dest,
				destOff, maxDestLen);
	}

}
