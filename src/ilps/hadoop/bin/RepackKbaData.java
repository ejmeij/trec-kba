/*******************************************************************************
 * Copyright 2012 Edgar Meij
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package ilps.hadoop.bin;

import ilps.hadoop.StreamItemWritable;
import ilps.hadoop.ThriftFileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Repacks the documents to allow more convenient processing.     
 *  
 * @author emeij
 *
 */
public class RepackKbaData extends Configured implements Tool {

  protected enum Counter {
    documents
  };

  private static final Logger LOG = Logger.getLogger(RepackKbaData.class);

  /**
   * Emits date, StreamItemWritable pairs. 
   * 
   * @author emeij
   *
   */
  public static class MyMapper extends
      Mapper<Text, StreamItemWritable, Text, StreamItemWritable> {

    private static final Text date = new Text();

    @Override
    public void map(Text key, StreamItemWritable value, Context context)
        throws IOException, InterruptedException {

      context.getCounter(Counter.documents).increment(1);

      String filename = key.toString();
      String dirdate = filename.substring(
          filename.lastIndexOf('/', filename.lastIndexOf('/') - 1) + 1,
          filename.lastIndexOf('/'));
      date.set(dirdate);

      context.write(date, value);

    }

  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RepackKbaData(), args);
    System.exit(res);
  }

  static int printUsage() {
    System.out.println("Usage: " + RepackKbaData.class.getName()
        + " -i input -o output [-r num_reducers] \n\n"
        + "  -f will overwrite the output folder\n\n"
        + "Example usage: hadoop jar trec-kba.jar "
        + RepackKbaData.class.getName() + " "
        + "-i kba/tiny-kba-stream-corpus/*/* "
        + "-o kba/tiny-kba-stream-corpus-repacked \n\n");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  @Override
  public int run(String[] args) throws Exception {

    String in = null;
    String out = null;
    Integer reducers = null;

    boolean force = false;

    List<String> other_args = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-i".equals(args[i])) {
          in = args[++i];
        } else if ("-o".equals(args[i])) {
          out = args[++i];
        } else if ("-r".equals(args[i])) {
          reducers = Integer.parseInt(args[++i]);
        } else if ("-f".equals(args[i])) {
          force = true;
        } else if ("-h".equals(args[i]) || "--help".equals(args[i])) {
          return printUsage();
        } else {
          other_args.add(args[i]);
        }
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from "
            + args[i - 1]);
        return printUsage();
      }
    }

    if (other_args.size() > 0 || in == null || out == null)
      return printUsage();

    LOG.info("Tool: " + this.getClass().getName());
    LOG.info(" - input path: " + in);
    LOG.info(" - output path: " + out);

    Configuration conf = getConf();

    Job job = new Job(conf, "Repack KBA");
    job.setJarByClass(RepackKbaData.class);

    // some weird issues with Thrift classes in the Hadoop distro.
    job.setUserClassesTakesPrecedence(true);

    job.setInputFormatClass(ThriftFileInputFormat.class);
    ThriftFileInputFormat.addInputPath(job, new Path(in));

    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(StreamItemWritable.class);

    // delete the output dir
    if (force)
      FileSystem.get(conf).delete(new Path(out), true);

    job.setReducerClass(Reducer.class);
    SequenceFileOutputFormat.setOutputCompressionType(job,
        SequenceFile.CompressionType.BLOCK);
    SequenceFileOutputFormat.setOutputPath(job, new Path(out));
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(StreamItemWritable.class);

    if (reducers != null)
      job.setNumReduceTasks(reducers);

    // Let's go
    return job.waitForCompletion(true) ? 0 : 1;

  }
}
