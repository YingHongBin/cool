package com.nus.cohana.core.io.storevector;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.nus.cohana.core.io.InputVector;
import com.nus.cohana.core.lang.Integers;
import com.nus.cohana.core.nio.ByteBuffers;

/**
 * Store integers in one byte
 * 
 * @author david, caiqc, xiezl
 */
public class ZInt8Store implements ZIntStore, InputVector {

	private ByteBuffer byteBuffer;

	private int count;
	
	private ZInt8Store(int c) {
		count = c;
	}

	public ZInt8Store(byte[] compressed, int offset, int unused) {
		ByteBuffer b = ByteBuffer.wrap(compressed, offset, 4).order(ByteOrder.nativeOrder());
		this.count = b.getInt();
		
		int length = count;

		this.byteBuffer = ByteBuffer.wrap(compressed, offset+4, length).slice();
	}

	public static ZIntStore load(ByteBuffer buffer, int n) {
		ZIntStore store = new ZInt8Store(n);
		store.readFrom(buffer);
		return store;
	}

	@Override
	public int next() {
		return (byteBuffer.get() & 0xFF);
	}

	@Override
	public int get(int i) {
		return (byteBuffer.get(i) & 0xFF);
	}

	@Override
	public void put(int[] val, int offset, int length) {
		// byteBuffer.rewind();
		for (int i = 0; i < length; i++) {
			byteBuffer.put(i, (byte) val[i + offset]);
		}
	}

	@Override
	public void put(int pos, int val) {
		byteBuffer.put(pos, (byte) val);
	}

	@Override
	public boolean hasNext() {
		return byteBuffer.hasRemaining();
	}

	@Override
	public int binarySearch(int key) {
		// Trick: Search compressed key instead of first uncompressing data then
		// search the key
		return ByteBuffers.binarySearchUnsigned(byteBuffer, 0, byteBuffer.limit(),
				(byte) key);
	}

	@Override
	public void readFrom(ByteBuffer buffer) {
		int limit = buffer.limit();
		int newlimit = buffer.position() + count;
		buffer.limit(newlimit);
		this.byteBuffer = buffer.slice();
		buffer.position(newlimit);
		buffer.limit(limit);
	}

	@Override
	public void writeTo(DataOutput out) throws IOException {	
		out.writeInt(Integers.toNativeByteOrder(this.count));
		for (int i = 0; i < this.count; i++) {
			out.writeByte(byteBuffer.get(i));
		}
	}

	@Override
	public int size() {
		return this.count;
	}

	@Override
	public int sizeInByte() {
		return this.count;
	}

	@Override
	public void rewind() {
		this.byteBuffer.rewind();
	}

	@Override
	public int find(int key) {
		if (key > Byte.MAX_VALUE || key < 0)
			return -1;

		return ByteBuffers.binarySearchUnsigned(byteBuffer, 0, byteBuffer.limit(),
				(byte) key);
	}

	@Override
	public int sizeInBytes() {
		return this.count;
	}

	@Override
	public void skipTo(int pos) {
		this.byteBuffer.position(pos);
	}

}
