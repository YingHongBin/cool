/**
 * 
 */
package com.nus.cohana.core.io.storevector;

import java.nio.ByteBuffer;

import com.nus.cohana.core.io.Codec;
import com.nus.cohana.core.io.InputVector;
import com.nus.cohana.core.io.compression.FoRCompressor;

/**
 * @author david, xiezl
 *
 */
public class FoRInputVector implements InputVector {

	private int min;

	private int max;

	private InputVector vecIn;

	@Override
	public void readFrom(ByteBuffer buf) {
		min = buf.getInt();
		max = buf.getInt();
		Codec codec = Codec.fromInteger(buf.get());
		switch (codec) {
		case INT8:
			vecIn = (InputVector) ZInt8Store.load(buf, buf.getInt());
			break;
		case INT16:
			vecIn = (InputVector) ZInt16Store.load(buf, buf.getInt());
			break;
		case INT32:
			vecIn = (InputVector) ZInt32Store.load(buf, buf.getInt());
			break;
		default:
			throw new IllegalArgumentException("Codec: " + codec);
		}
	}

	@Override
	public int find(int key) {
		if(key < min || key > max)
			return -1;
		return vecIn.find(key - min);
	}

	@Override
	public int get(int index) {
		return min + vecIn.get(index);
	}

	@Override
	public boolean hasNext() {
		return vecIn.hasNext();
	}

	@Override
	public int next() {
		return min + vecIn.next();
	}

	@Override
	public int size() {
		return vecIn.size();
	}

	@Override
	public int sizeInBytes() {
		return vecIn.sizeInBytes() + FoRCompressor.HEADACC;
	}

	@Override
	public void skipTo(int pos) {
		vecIn.skipTo(pos);
	}

	@Override
	public String toString() {
		return vecIn.toString();
	}	
}
