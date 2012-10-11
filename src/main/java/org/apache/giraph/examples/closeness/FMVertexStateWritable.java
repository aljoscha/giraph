package org.apache.giraph.examples.closeness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FMVertexStateWritable implements ClosenessVertexStateWritable {
  private FMCounterWritable counter;
  private OpenLongIntHashMapWritable shortestPaths;
  
  public FMVertexStateWritable() {
    this.counter = new FMCounterWritable();
    this.shortestPaths = new OpenLongIntHashMapWritable();
  }
  
  public FMVertexStateWritable(int numBuckets) {
    this.counter = new FMCounterWritable(numBuckets);
    this.shortestPaths = new OpenLongIntHashMapWritable();
  }
  
  public FMCounterWritable getCounter() {
    return counter;
  }

  public OpenLongIntHashMapWritable getShortestPaths() {
    return shortestPaths;
  }
  
  public int getNumBuckets() {
    return counter.getNumBuckets();
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
}
