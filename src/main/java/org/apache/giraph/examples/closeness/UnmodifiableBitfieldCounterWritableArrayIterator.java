package org.apache.giraph.examples.closeness;

import com.google.common.collect.UnmodifiableIterator;

public class UnmodifiableBitfieldCounterWritableArrayIterator extends
    UnmodifiableIterator<BitfieldCounterWritable> {
  /** Array to iterate over */
  private final BitfieldCounterWritable[] counterArray;
  /** Offset to array */
  private int offset;

  public UnmodifiableBitfieldCounterWritableArrayIterator(BitfieldCounterWritable[] intArray) {
    this.counterArray = intArray;
    offset = 0;
  }

  @Override
  public boolean hasNext() {
    return offset < counterArray.length;
  }

  @Override
  public BitfieldCounterWritable next() {
    return counterArray[offset++];
  }
}
