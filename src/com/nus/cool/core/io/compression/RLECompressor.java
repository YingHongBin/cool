/**
 * 
 */
package com.nus.cool.core.io.compression;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.nus.cool.core.io.Codec;
import com.nus.cool.core.lang.Integers;

/**
 * RLE compressing scheme. The data layout is as follows
 * 
 * ---------------------------------------
 * | zLen | segments | compressed values |
 * ---------------------------------------
 * 
 * segments = # of segment in values
 * @author david
 *
 */
public class RLECompressor implements Compressor {

	public static final int HEADACC = 4 + 4;
	
	public static Codec CODEC = Codec.RLE;

	private int maxCompressedLen;

	public RLECompressor(Histogram hist) {
        int uncompressedSize = 3 * Integer.BYTES * hist.count();
		this.maxCompressedLen = HEADACC + 
            (hist.rawSize() < uncompressedSize ? uncompressedSize : hist.rawSize());
	}

	private void writeInt(ByteBuffer buf, int v, int width) {
		switch (width) {
		case 1:
			buf.put((byte) v);
			break;
		case 2:
			buf.putShort((short) v);
			break;
		case 3:
		case 0:
			buf.putInt(v);
			break;
		default:
			throw new java.lang.IllegalArgumentException("incorrect number of bytes");
		}

	}

	private void write(ByteBuffer buf, int val, int off, int len) {
		byte b = 0;
		b |= ((Integers.minBytes(val) << 4) | Integers.minBytes(off) << 2 | Integers.minBytes(len));
		buf.put(b);

		writeInt(buf, val, ((b >> 4) & 3));
		writeInt(buf, off, ((b >> 2) & 3));
		writeInt(buf, len, ((b >> 0) & 3));
	}

	@Override
	public int maxCompressedLength() {
		return maxCompressedLen;
	}

	@Override
	public int compress(byte[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int compress(int[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		int n = 1;
		ByteBuffer buf = ByteBuffer.wrap(dest, destOff, maxDestLen).order(ByteOrder.nativeOrder());
		buf.position(HEADACC);
		int v = src[srcOff], voff = 0, vlen = 1;
		for (int i = srcOff + 1; i < srcOff + srcLen; i++) {
			if (src[i] != v) {
				write(buf, v, voff, vlen);
				v = src[i];
				voff = i - srcOff;
				vlen = 1;
				n++;
			} else {
				vlen++;
			}
		}
		write(buf, v, voff, vlen);
		int zLen = buf.position() - HEADACC;
		buf.position(0);
		buf.putInt(zLen).putInt(n);
		return zLen + HEADACC;
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
