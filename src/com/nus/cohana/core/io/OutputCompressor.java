/**
 * 
 */
package com.nus.cohana.core.io;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.DataOutput;
import java.io.IOException;
import com.nus.cohana.core.io.compression.Compressor;
import com.nus.cohana.core.io.compression.CompressorAdviser;
import com.nus.cohana.core.io.compression.CompressorFactory;
import com.nus.cohana.core.io.compression.Histogram;

/**
 * Utility class for writing a compressed integer vector into
 * disk.
 * 
 * @author david
 *
 */
public class OutputCompressor implements Output {
	
	private static enum DataType { INTEGER, STRING }
	
	private DataType dataType;
	
	private int[] vec;
	
	private byte[] strVec;
	
	private int off;
	
	private int len;
	
	private Histogram hist;
	
	private CompressorAdviser adviser;
	
	private CompressorFactory compFactory;
	
	public CompressorAdviser getCompressorAdviser() {
		return adviser;
	}
	
	public OutputCompressor(CompressorAdviser adviser, CompressorFactory compFactory) {
		this.adviser = checkNotNull(adviser);
		this.compFactory = checkNotNull(compFactory);
	}
	
	public void reset(Histogram h, int[] vec, int off, int len) {
		this.hist = h;
		this.vec = vec;
		this.off = off;
		this.len = len;
		this.dataType = DataType.INTEGER;
	}
	
	public void reset(Histogram h, byte[] vec, int off, int len) {
		this.hist = h;
		this.strVec = vec;
		this.off = off;
		this.len = len;
		this.dataType = DataType.STRING;
	}
	
	@Override
	public int writeTo(DataOutput out) throws IOException {
		int bytesWritten = 0;
		Codec codec = adviser.advise(hist);
		Compressor comp = compFactory.newCompressor(codec, hist);
		int maxLen = comp.maxCompressedLength();
		byte[] compressed = new byte[maxLen];

		// TODO: strVec includes value offsets and values, value offsets can be compressed by another algorithm for better performance
		int compressLen = dataType == DataType.INTEGER ? comp.compress(vec, off, len, compressed, 0, maxLen)
				: comp.compress(strVec , off, len, compressed, 0, maxLen);
		out.writeByte(codec.ordinal());
		bytesWritten++;
		// Write the compressed data
		out.write(compressed, 0, compressLen);
		bytesWritten += compressLen;
		return bytesWritten;
	}

}
