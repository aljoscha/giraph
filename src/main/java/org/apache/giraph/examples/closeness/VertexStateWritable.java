package org.apache.giraph.examples.closeness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class VertexStateWritable implements Writable {
  private BitfieldCounterWritable counter;
  private OpenLongIntHashMapWritable shortestPaths;
  
  public VertexStateWritable() {
    this.counter = new BitfieldCounterWritable();
    this.shortestPaths = new OpenLongIntHashMapWritable();
  }
  public VertexStateWritable(int numBits) {
    counter = new BitfieldCounterWritable(numBits);
    shortestPaths = new OpenLongIntHashMapWritable();
  }
  
  public BitfieldCounterWritable getCounter() {
    return counter;
  }

  public OpenLongIntHashMapWritable getShortestPaths() {
    return shortestPaths;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    counter.write(out);
    shortestPaths.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    counter.readFields(in);
    shortestPaths.readFields(in);
  }
  
  public int getNumBits() {
    return counter.getNumBits();
  }
}
