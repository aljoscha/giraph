/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.giraph.utils.UnmodifiableLongNullEdgeArrayIterable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Simple implementation of {@link Vertex} using an long as id,
 * and having vertex value and message type as generic arguments.
 * Edges are immutable and unweighted. This class aims to be as
 * memory efficient as possible. This class is based on
 * {@link IntIntNullIntVertex} and {@link HashMapVertex}.
 */
public abstract class LongXNullXVertex<V extends Writable, M extends Writable>
    extends
    Vertex<LongWritable, V, NullWritable, M> {

  /** Long array of neighbor vertex ids */
  private long[] neighbors;
  /** List of incoming messages from the previous superstep */
  private List<M> messageList = Lists.newArrayList();

  @Override
  public void initialize(LongWritable vertexId, V vertexValue,
      Map<LongWritable, NullWritable> edges,
      Iterable<M> messages) {
    initialize(vertexId, vertexValue);
    this.neighbors = new long[(edges != null) ? edges.size() : 0];
    int n = 0;
    if (edges != null) {
      for (LongWritable neighbor : edges.keySet()) {
        this.neighbors[n++] = neighbor.get();
      }
    }
    if (messages != null) {
      Iterables.<M>addAll(messageList, messages);
    }
  }

  @Override
  public Iterable<Edge<LongWritable, NullWritable>> getEdges() {
    return new UnmodifiableLongNullEdgeArrayIterable(neighbors);
  }

  @Override
  public NullWritable getEdgeValue(LongWritable targetVertexId) {
    return NullWritable.get();
  }

  @Override
  public boolean hasEdge(LongWritable targetVertexId) {
    for (long neighbor : neighbors) {
      if (neighbor == targetVertexId.get()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int getNumEdges() {
    return neighbors.length;
  }

  @Override
  public void sendMessageToAllEdges(final M message) {
    for (long neighbor : neighbors) {
      sendMessage(new LongWritable(neighbor), message);
    }
  }

  @Override
  public Iterable<M> getMessages() {
    return Iterables.unmodifiableIterable(messageList);
  }

  @Override
  void putMessages(Iterable<M> messages) {
    messageList.clear();
    Iterables.addAll(messageList, messages);
  }

  @Override
  void releaseResources() {
    messageList.clear();
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    getId().write(out);
    getValue().write(out);
    
    out.writeInt(neighbors.length);
    for (int n = 0; n < neighbors.length; n++) {
      out.writeLong(neighbors[n]);
    }
    
    out.writeInt(messageList.size());
    for (M message : messageList) {
      message.write(out);
    }
    
    out.writeBoolean(isHalted());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    LongWritable vertexId = getConf().createVertexId();
    vertexId.readFields(in);
    V vertexValue = getConf().createVertexValue();
    vertexValue.readFields(in);
    super.initialize(vertexId, vertexValue);
    
    int numEdges = in.readInt();
    neighbors = new long[numEdges];
    for (int n = 0; n < numEdges; n++) {
      neighbors[n] = in.readLong();
    }
    
    
    int numMessages = in.readInt();
    messageList = Lists.newArrayListWithCapacity(numMessages);
    for (int i = 0; i < numMessages; ++i) {
      M message = getConf().createMessageValue();
      message.readFields(in);
      messageList.add(message);
    }

    boolean halt = in.readBoolean();
    if (halt) {
      voteToHalt();
    } else {
      wakeUp();
    }
  }
}
