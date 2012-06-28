package org.apache.giraph.examples.closeness;

import org.apache.hadoop.io.Writable;

public abstract class DistinctSeenCounterWritable implements Writable{

  public DistinctSeenCounterWritable() {
    super();
  }

  public abstract void merge(DistinctSeenCounterWritable other);

  public abstract int getCount();

  public abstract void addNode(long n);
}