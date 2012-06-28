package org.apache.giraph.examples.closeness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.map.OpenLongLongHashMap;

@SuppressWarnings("serial")
public class OpenLongLongHashMapWritable extends OpenLongLongHashMap
    implements Writable {

  @Override
  public void write(DataOutput out) throws IOException {
    // first write the capacity
    out.writeInt(table.length);
    for (int i = 0; i < table.length; ++i) {
      out.writeLong(table[i]);
      out.writeLong(values[i]);
      out.writeByte(state[i]);
    }
    out.writeInt(freeEntries);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int capacity = in.readInt();
    table = new long[capacity];
    values = new long[capacity];
    state = new byte[capacity];
    
    for (int i = 0; i < table.length; ++i) {
      table[i] = in.readLong();
      values[i] = in.readLong();
      state[i] = in.readByte();
    }
    freeEntries = in.readInt();
  }
}
