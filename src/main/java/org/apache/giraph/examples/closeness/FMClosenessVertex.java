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

package org.apache.giraph.examples.closeness;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.utils.UnmodifiableIntArrayIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

public class FMClosenessVertex
    extends
    BasicVertex<IntWritable, FMVertexStateWritable, NullWritable, FMSketchWritable>
    implements Tool {

  /** Int represented vertex id */
  private int id;
  /** Int represented vertex value */
  private FMVertexStateWritable value;
  /** Int array of neighbor vertex ids */
  private int[] neighbors;
  /** Int array of messages */
  private FMSketchWritable[] messages;
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(FMClosenessVertex.class);
  /** Configuration */
  private Configuration conf;

  public FMClosenessVertex() {
    id = -1;
    value = new FMVertexStateWritable();
  }

  @Override
  public void initialize(IntWritable vertexId,
      FMVertexStateWritable vertexValue, Map<IntWritable, NullWritable> edges,
      Iterable<FMSketchWritable> messages) {
    id = vertexId.get();
    value = vertexValue;
    value.getCounter().addNode(id);
    this.neighbors = new int[(edges != null) ? edges.size() : 0];
    int n = 0;
    if (edges != null) {
      for (IntWritable neighbor : edges.keySet()) {
        this.neighbors[n++] = neighbor.get();
      }
    }
    this.messages = new FMSketchWritable[(messages != null) ? Iterables
        .size(messages) : 0];
    if (messages != null) {
      n = 0;
      for (FMSketchWritable message : messages) {
        this.messages[n++] = message;
      }
    }
  }

  @Override
  public void setVertexId(IntWritable id) {
    this.id = id.get();
  }

  @Override
  public IntWritable getVertexId() {
    return new IntWritable(id);
  }

  @Override
  public FMVertexStateWritable getVertexValue() {
    return value;
  }

  @Override
  public void setVertexValue(FMVertexStateWritable vertexValue) {
    value = vertexValue;
  }

  @Override
  public Iterator<IntWritable> getOutEdgesIterator() {
    return new UnmodifiableIntArrayIterator(neighbors);
  }

  @Override
  public NullWritable getEdgeValue(IntWritable targetVertexId) {
    return NullWritable.get();
  }

  @Override
  public boolean hasEdge(IntWritable targetVertexId) {
    for (int neighbor : neighbors) {
      if (neighbor == targetVertexId.get()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int getNumOutEdges() {
    return neighbors.length;
  }

  @Override
  public void sendMsgToAllEdges(final FMSketchWritable message) {
    for (int neighbor : neighbors) {
      sendMsg(new IntWritable(neighbor), message);
    }
  }

  @Override
  public Iterable<FMSketchWritable> getMessages() {
    return new Iterable<FMSketchWritable>() {
      @Override
      public Iterator<FMSketchWritable> iterator() {
        return new UnmodifiableFMSketchWritableArrayIterator(messages) {
        };
      }
    };
  }

  @Override
  public void putMessages(Iterable<FMSketchWritable> newMessages) {
    messages = new FMSketchWritable[Iterables.size(newMessages)];
    int n = 0;
    for (FMSketchWritable message : newMessages) {
      messages[n++] = message;
    }
  }

  @Override
  public void releaseResources() {
    messages = new FMSketchWritable[0];
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeInt(id);
    value.write(out);
    out.writeInt(neighbors.length);
    for (int n = 0; n < neighbors.length; n++) {
      out.writeInt(neighbors[n]);
    }
    out.writeInt(messages.length);
    for (int n = 0; n < messages.length; n++) {
      messages[n].write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readInt();
    value = new FMVertexStateWritable(getConf());
    value.readFields(in);
    int numEdges = in.readInt();
    neighbors = new int[numEdges];
    for (int n = 0; n < numEdges; n++) {
      neighbors[n] = in.readInt();
    }
    int numMessages = in.readInt();
    messages = new FMSketchWritable[numMessages];
    for (int n = 0; n < numMessages; n++) {
      messages[n].readFields(in);
    }
  }

  @Override
  public void compute(Iterator<FMSketchWritable> msgIterator) {
    int seenCountBefore = getVertexValue().getCounter().getCount();

    while (msgIterator.hasNext()) {
      FMSketchWritable inCounter = msgIterator.next();
      getVertexValue().getCounter().merge(inCounter);
    }

    int seenCountAfter = getVertexValue().getCounter().getCount();

    if ((seenCountBefore != seenCountAfter) || (getSuperstep() == 0)) {
      sendMsgToAllEdges(getVertexValue().getCounter().copy());
    }

    // determine last iteration for which we set a value,
    // we need to copy this to all iterations up to this one
    // because the number of reachable vertices stays the same
    // when the compute method is not invoked
    if (getSuperstep() > 0) {
      int i = (int) getSuperstep() - 1;
      while (i > 0) {
        if (getVertexValue().getShortestPaths().containsKey(i)) {
          break;
        }
        --i;
      }
      int numReachable = getVertexValue().getShortestPaths().get(i);
      for (; i < getSuperstep(); ++i) {
        getVertexValue().getShortestPaths().put(i, numReachable);
      }
    }
    // subtract 1 because our own bit is counted as well
    getVertexValue().getShortestPaths().put((int) getSuperstep(),
        getVertexValue().getCounter().getCount() - 1);

    voteToHalt();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * run with: hadoop jar target/giraph-0.2-SNAPSHOT-jar-with-dependencies.jar
   * org.apache.giraph.examples.closeness.ClosenessVertex closenessInputGraph
   * closenessOutputGraph 32 3
   */
  @Override
  public int run(String[] argArray) throws Exception {
    Preconditions.checkArgument(argArray.length == 4,
        "run: Must have 4 arguments <input path> <output path> "
            + "<num buckets> <# of workers>");

    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    job.setVertexClass(getClass());
    job.setVertexInputFormatClass(FMClosenessVertexInputFormat.class);
    job.setVertexOutputFormatClass(FMClosenessVertexOutputFormat.class);
    FileInputFormat.addInputPath(job.getInternalJob(), new Path(argArray[0]));
    FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(argArray[1]));
    job.getConfiguration().setInt(FMSketchWritable.NUM_BUCKETS,
        Integer.parseInt(argArray[2]));
    job.setWorkerConfiguration(Integer.parseInt(argArray[3]),
        Integer.parseInt(argArray[3]), 100.0f);

    return job.run(true) ? 0 : -1;
  }

  /**
   * Can be used for command line execution.
   * 
   * @param args
   *          Command line arguments.
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new FMClosenessVertex(), args));
  }
}
