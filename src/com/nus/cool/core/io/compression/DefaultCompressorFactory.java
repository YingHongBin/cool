/**
 * 
 */
package com.nus.cool.core.io.compression;

import com.nus.cool.core.io.Codec;
import net.jpountz.lz4.LZ4Factory;

/**
 * @author David
 *
 */
public class DefaultCompressorFactory implements CompressorFactory {

	private LZ4Factory factory = LZ4Factory.fastestInstance();

	@Override
	public Compressor newCompressor(Codec type, Histogram hist) {
		Compressor compressor = null;
		switch (type) {
		case INT8:
		case INT16:
		case INT32:
			compressor = new ZIntCompressor(type, hist);
			break;
		case BitVector:
			compressor = new BitSetCompressor(hist);
			break;
		case INTBIT:
			compressor = new ZIntBitCompressor(hist);
			break;
		case RLE:
			compressor = new RLECompressor(hist);
			break;
		case LZ4:
			compressor = new LZ4JavaCompressor(factory.fastCompressor(), hist);
			break;
		case Delta:
			compressor = new FoRCompressor(hist);
			break;
		case INT24:
		case None:
		case Unknown:
		default:
			throw new IllegalArgumentException("Unsupport Codec: " + type);
		}

		return compressor;
	}

}
