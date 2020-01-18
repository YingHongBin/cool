/**
 * 
 */
package com.nus.cool.core.io;

/**
 * An ordered collection (sequence) of integers. Implementation of this interface should at least
 * implements sequential access method (i.e., hasNext() and next()).
 * 
 * If random access method (i.e., find() and get()) is implemented. The find() should be completed
 * at O(log(n)) and the get() should be completed at O(1).
 * 
 * @author david
 *
 */
public interface InputVector extends Input {

  int find(int key);

  int get(int index);

  boolean hasNext();

  int next();

  int size();

  int sizeInBytes();

  void skipTo(int pos);

}
