/**
 * 
 */
package com.nus.cool.core.io.compression;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.nus.cool.core.io.Codec;
import com.nus.cool.core.lang.Integers;

/**
 * Compress integers using the leading zero suppressed scheme. Each compressed
 * integer is formatted to align to byte boundary and stored in native byte order
 *
 * The data layout is as follows
 * 
 * ------------------------------------
 * | count | ZInt compressed integers |
 * ------------------------------------
 * 
 * @author David
 *
 */
public class ZIntCompressor implements Compressor {
	
	public static final int HEADACC = 4;

	private int width;
	
	private int maxCompressedLength;

	public static Codec getCodec(Histogram hist) {
		int width = Integers.minBytes((int)hist.max());
		Codec codec = Codec.Unknown;
		switch(width) {
		case 1:
			codec = Codec.INT8;
			break;
		case 2:
			codec = Codec.INT16;
			break;
		case 3:
		case 4:
			codec = Codec.INT32;
			break;
		}
		return codec;
	}

	public ZIntCompressor(Codec codec, Histogram hist) {
        switch(codec) {
            case INT8:
                width = 1;
                break;
            case INT16:
                width = 2;
                break;
            case INT32:
                width = 4;
                break;
            default:
                throw new IllegalArgumentException("Unsupport codec" + codec);
        }

		this.maxCompressedLength = width * hist.count() + HEADACC;

	}
	
	public ZIntCompressor(Histogram hist) {
		int bytes = Integers.minBytes((int) (hist.max()));
		bytes = bytes == 3 ? 4 : bytes;
		this.width = bytes;

		this.maxCompressedLength = bytes * hist.count() + HEADACC;
	}

	@Override
	public int maxCompressedLength() {
		return maxCompressedLength;
	}

	@Override
	public int compress(byte[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int compress(int[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		ByteBuffer buffer = ByteBuffer.wrap(dest, destOff, maxDestLen);
		buffer.order(ByteOrder.nativeOrder());
		// Write count
		// save srcLen for uncompressing
		buffer.putInt(srcLen);
		// Write compressed data
		for (int i = srcOff; i < srcOff + srcLen; i++) {			
			switch (width) {
			case 1:
				buffer.put((byte) src[i]);
				break;
			case 2:
				buffer.putShort((short) src[i]);
				break;
			case 4:
				buffer.putInt(src[i]);
				break;
			}
		}
		return buffer.position() - destOff;
	}

	@Override
	public int compress(long[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		ByteBuffer buffer = ByteBuffer.wrap(dest, destOff, maxDestLen);
		buffer.order(ByteOrder.nativeOrder());
		buffer.putInt(srcLen);
		for (int i = srcOff; i < srcOff + srcLen; i++) {
			switch (width) {
			case 1:
				buffer.put((byte) src[i]);
				break;
			case 2:
				buffer.putShort((short) src[i]);
				break;
			case 4:
				buffer.putInt((int) src[i]);
				break;
			}
		}
		return buffer.position() - destOff;
	}
	
	@Override
	public int compress(float[] src, int srcOff, int srcLen, byte[] dest,
			int destOff, int maxDestLen) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String toString() {
		return String.format("(numOfBytes = %d)", width);
	}	

}
