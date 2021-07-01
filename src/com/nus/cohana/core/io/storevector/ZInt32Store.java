package com.nus.cohana.core.io.storevector;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.Arrays;

import com.google.common.primitives.Ints;
import com.nus.cohana.core.nio.IntBuffers;

import com.nus.cohana.core.io.InputVector;
import com.nus.cohana.core.lang.Integers;

/**
 * Store integers in four bytes
 * 
 * @author david, caiqc, xiezl
 */
public class ZInt32Store implements ZIntStore, InputVector {

	private IntBuffer intBuffer;

	private int count;

	private ZInt32Store(int numOfValues) {
		this.count = numOfValues;
	}

	public ZInt32Store(byte[] compressed, int offset, int unused) {
		ByteBuffer b = ByteBuffer.wrap(compressed, offset, 4).order(ByteOrder.nativeOrder());
		this.count = b.getInt();
		int length = count * Ints.BYTES;
		ByteBuffer buf = ByteBuffer.wrap(compressed, offset + 4, length);
		buf.order(ByteOrder.nativeOrder());
		this.intBuffer = buf.asIntBuffer();
	}

	public static ZIntStore load(ByteBuffer buffer, int n) {	
		ZIntStore store = new ZInt32Store(n);
		store.readFrom(buffer);
		return store;
	}

	@Override
	public int get(int pos) {
		return intBuffer.get(pos);
	}

	@Override
	public void put(int[] val, int offset, int length) {
		// intBuffer.rewind();
		for (int i = 0; i < length; i++) {
			intBuffer.put(i, val[i + offset]);
		}
	}

	@Override
	public void put(int pos, int val) {
		intBuffer.put(pos, val);
	}

	@Override
	public boolean hasNext() {
		return intBuffer.hasRemaining();
	}

	@Override
	public void readFrom(ByteBuffer buffer) {
		int length = count * Ints.BYTES;
		int oldLimit = buffer.limit();
		int newLimit = buffer.position() + length;
		buffer.limit(newLimit);
		this.intBuffer = buffer.asIntBuffer();
		buffer.position(newLimit);
		buffer.limit(oldLimit);
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {
		out.writeInt(Integers.toNativeByteOrder(this.count));
		for (int i = 0; i < this.count; i++) {
			int s = intBuffer.get(i);
			out.writeInt(Integers.toNativeByteOrder(s));
		}	
	}

	@Override
	public int next() {
		return intBuffer.get();
	}

	@Override
	public int binarySearch(int key) {
		return IntBuffers.binarySearch(intBuffer, 0, intBuffer.limit(), key);
	}

	@Override
	public int size() {
		return this.count;
	}

	@Override
	public int sizeInByte() {
		return (this.count << 2);
	}

	@Override
	public void rewind() {
		this.intBuffer.rewind();
	}

	@Override
	public int find(int key) {
		return IntBuffers.binarySearch(intBuffer, 0, intBuffer.limit(), key);
	}

	@Override
	public int sizeInBytes() {
		return intBuffer.limit();
	}

	@Override
	public void skipTo(int pos) {
		intBuffer.position(pos);
	}

	@Override
	public String toString() {
		int[] data = new int[count];
		for (int i = 0; i < count; i++)
			data[i] = this.intBuffer.get(i);

		return Arrays.toString(data);
	}
}
