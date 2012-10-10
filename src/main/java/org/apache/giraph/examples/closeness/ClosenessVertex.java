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
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

public class ClosenessVertex
    extends
    LongXNullXVertex<VertexStateWritable, BitfieldCounterWritable>
    implements Tool {

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(ClosenessVertex.class);
  /** Configuration */
  private Configuration conf;


  @Override
  public void initialize(LongWritable vertexId, VertexStateWritable vertexValue,
      Map<LongWritable, NullWritable> edges,
      Iterable<BitfieldCounterWritable> messages) {
    super.initialize(vertexId, vertexValue, edges, messages);
    getValue().getCounter().addNode(getId().get());
  }
  
  // Needed for Tool interface
  @Override
  public void setConf(Configuration conf) {
    this.setConf(conf);
  }

  @Override
  public void compute(Iterable<BitfieldCounterWritable> msgIterator) {
    int seenCountBefore = getValue().getCounter().getCount();

    for (BitfieldCounterWritable inCounter : msgIterator) {
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
   * org.apache.giraph.examples.closeness.ClosenessVertex closenessInputGraph
   * closenessOutputGraph 32 3
   */
  @Override
  public int run(String[] argArray) throws Exception {
    Preconditions.checkArgument(argArray.length == 4,
        "run: Must have 4 arguments <input path> <output path> "
            + "<num bits> <# of workers>");

    GiraphJob job = new GiraphJob(getConf(), getClass().getName());
    job.getConfiguration().setVertexClass(getClass());
    job.getConfiguration().setVertexInputFormatClass(ClosenessVertexInputFormat.class);
    job.getConfiguration().setVertexOutputFormatClass(ClosenessVertexOutputFormat.class);
    FileInputFormat.addInputPath(job.getInternalJob(), new Path(argArray[0]));
    FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(argArray[1]));
    job.getConfiguration().setInt(BitfieldCounterWritable.NUM_BITS,
        Integer.parseInt(argArray[2]));
    job.getConfiguration().setWorkerConfiguration(Integer.parseInt(argArray[3]),
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
    System.exit(ToolRunner.run(new ClosenessVertex(), args));
  }
}
