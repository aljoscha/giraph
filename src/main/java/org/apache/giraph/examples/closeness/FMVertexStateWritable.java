package org.apache.giraph.examples.closeness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class FMVertexStateWritable implements Writable {
  private FMSketchWritable counter;
  private OpenLongIntHashMapWritable shortestPaths;
  
  public FMVertexStateWritable() {
    this.counter = new FMSketchWritable();
    this.shortestPaths = new OpenLongIntHashMapWritable();
  }
  public FMVertexStateWritable(Configuration conf) {
    counter = new FMSketchWritable(conf);
    shortestPaths = new OpenLongIntHashMapWritable();
  }
  
  public FMSketchWritable getCounter() {
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
}
