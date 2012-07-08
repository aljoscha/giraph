package org.apache.giraph.examples.closeness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class FMVertexStateWritable implements Writable {
  private FMSketchWritable counter;
  private OpenIntIntHashMapWritable shortestPaths;
  
  public FMVertexStateWritable() {
    this.counter = new FMSketchWritable();
    this.shortestPaths = new OpenIntIntHashMapWritable();
  }
  public FMVertexStateWritable(Configuration conf) {
    counter = new FMSketchWritable(conf);
    shortestPaths = new OpenIntIntHashMapWritable();
  }
  
  public FMSketchWritable getCounter() {
    return counter;
  }

  public OpenIntIntHashMapWritable getShortestPaths() {
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
}
