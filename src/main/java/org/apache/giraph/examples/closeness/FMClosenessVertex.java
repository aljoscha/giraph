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

import java.util.Map;

import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.LongXNullXVertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Preconditions;

/**
 * Graph based implementation of the centrality algorithm detailed in
 * "Centralities in Large Networks: Algorithms and Observations"
 * (http://www.cs.cmu.edu/~ukang/papers/CentralitySDM2011.pdf).
 * 
 * The authors used MapReduce but the algorithm is iterative and perfectly
 * suited for a graph based implementation.
 * 
 * This Vertex uses a Flajolet-Martin Sketch to count the number of neighbours
 * as detailed in the aforementioned paper.
 * 
 * The state held at each vertex is a bit more complex, therefore we need a
 * custom Writable, {@link FMVertexStateWritable} that holds the FM-Sketch and a
 * hash map holding the shortest paths as detailed in the paper.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class FMClosenessVertex extends
    LongXNullXVertex<FMVertexStateWritable, FMCounterWritable> implements Tool {

  /** Configuration */
  private Configuration conf;

  @Override
  public void initialize(LongWritable vertexId,
      FMVertexStateWritable vertexValue, Map<LongWritable, NullWritable> edges,
      Iterable<FMCounterWritable> messages) {
    super.initialize(vertexId, vertexValue, edges, messages);
    if (vertexValue == null || vertexValue.getNumBuckets() <= 0) {
      // the getConfiguration() calls seems not to work so we hardcode it here
      int numBuckets = 32;
      // getContext().getConfiguration().getInt(FMSketchWritable.NUM_BUCKETS,
      // 32);
      vertexValue = new FMVertexStateWritable(numBuckets);
      vertexValue.getCounter().addNode((int) getId().get());
      setValue(vertexValue);
    }
  }

  // Needed for Tool interface
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void compute(Iterable<FMCounterWritable> msgIterator) {
    int seenCountBefore = getValue().getCounter().getCount();

    for (FMCounterWritable inCounter : msgIterator) {
      getValue().getCounter().merge(inCounter);
    }

    int seenCountAfter = getValue().getCounter().getCount();

    if ((seenCountBefore != seenCountAfter) || (getSuperstep() == 0)) {
      sendMessageToAllEdges(getValue().getCounter().copy());
    }

    // determine last iteration for which we set a value,
    // we need to copy this to all iterations up to this one
    // because the number of reachable vertices stays the same
    // when the compute method is not invoked
    if (getSuperstep() > 0) {
      int i = (int) getSuperstep() - 1;
      while (i > 0) {
        if (getValue().getShortestPaths().containsKey(i)) {
          break;
        }
        --i;
      }
      int numReachable = getValue().getShortestPaths().get(i);
      for (; i < getSuperstep(); ++i) {
        getValue().getShortestPaths().put(i, numReachable);
      }
    }
    // subtract 1 because our own bit is counted as well
    getValue().getShortestPaths().put((int) getSuperstep(),
        getValue().getCounter().getCount() - 1);

    voteToHalt();
  }

  /**
   * run with: hadoop jar target/giraph-0.2-SNAPSHOT-jar-with-dependencies.jar
   * org.apache.giraph.examples.closeness.FMClosenessVertex closenessInputGraph
   * closenessOutputGraph 32 3
   */
  @Override
  public int run(String[] argArray) throws Exception {
    Preconditions.checkArgument(argArray.length == 4,
        "run: Must have 4 arguments <input path> <output path> "
            + "<num bits> <# of workers>");

    GiraphJob job = new GiraphJob(new Configuration(), getClass().getName());
    job.getConfiguration().setVertexClass(getClass());
    job.getConfiguration().setVertexInputFormatClass(
        ClosenessVertexInputFormat.class);
    job.getConfiguration().setVertexOutputFormatClass(
        ClosenessVertexOutputFormat.class);
    FileInputFormat.addInputPath(job.getInternalJob(), new Path(argArray[0]));
    FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(argArray[1]));
    job.getConfiguration().setInt(BitfieldCounterWritable.NUM_BITS,
        Integer.parseInt(argArray[2]));
    job.getConfiguration().setWorkerConfiguration(
        Integer.parseInt(argArray[3]), Integer.parseInt(argArray[3]), 100.0f);
    job.getConfiguration().setBoolean(GiraphConfiguration.USE_NETTY, true);

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
