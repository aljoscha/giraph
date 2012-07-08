package org.apache.giraph.examples.closeness;

import com.google.common.collect.UnmodifiableIterator;

public class UnmodifiableFMSketchWritableArrayIterator extends
    UnmodifiableIterator<FMSketchWritable> {
  /** Array to iterate over */
  private final FMSketchWritable[] counterArray;
  /** Offset to array */
  private int offset;

  public UnmodifiableFMSketchWritableArrayIterator(FMSketchWritable[] intArray) {
    this.counterArray = intArray;
    offset = 0;
  }

  @Override
  public boolean hasNext() {
    return offset < counterArray.length;
  }

  @Override
  public FMSketchWritable next() {
    return counterArray[offset++];
  }
}
