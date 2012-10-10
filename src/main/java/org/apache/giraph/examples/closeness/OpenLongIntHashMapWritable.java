package org.apache.giraph.examples.closeness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.map.OpenLongIntHashMap;

@SuppressWarnings("serial")
public class OpenLongIntHashMapWritable extends OpenLongIntHashMap
    implements Writable {

  @Override
  public void write(DataOutput out) throws IOException {
    // first write the capacity
    out.writeInt(table.length);
    for (int i = 0; i < table.length; ++i) {
      out.writeLong(table[i]);
      out.writeInt(values[i]);
      out.writeByte(state[i]);
    }
    out.writeInt(freeEntries);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int capacity = in.readInt();
    table = new long[capacity];
    values = new int[capacity];
    state = new byte[capacity];
    
    for (int i = 0; i < table.length; ++i) {
      table[i] = in.readInt();
      values[i] = in.readInt();
      state[i] = in.readByte();
    }
    freeEntries = in.readInt();
  }
}
