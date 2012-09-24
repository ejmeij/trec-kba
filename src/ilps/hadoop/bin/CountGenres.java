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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Counts the different genres (web, social, news) in the KBA data. 
 *  
 * @author emeij
 *
 */
public class CountGenres extends Configured implements Tool {

  protected enum Counter {
    records
  };

  private static final Logger LOG = Logger.getLogger(CountGenres.class);

  public static class MyMapper extends
      Mapper<Text, StreamItemWritable, Text, IntWritable> {

    private final Text out = new Text();
    private final IntWritable one = new IntWritable(1);

    @Override
    public void map(Text key, StreamItemWritable value, Context context)
        throws IOException, InterruptedException {

      context.getCounter(Counter.records).increment(1);
      String genre = value.getSource();
      out.set(genre);
      context.write(out, one);

    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CountGenres(), args);
    System.exit(res);
  }

  static int printUsage() {
    System.out
        .println(CountGenres.class.getName()
            + " -i input -o output \n"
            + "Example usage: hadoop jar trec-kba.jar ilps.hadoop.CountGenres"
            + " -i kba/kba-stream-corpus-2012-cleansed-only-out-repacked"
            + " -o kba/kba-stream-corpus-2012-cleansed-only-out-repacked-genrecounts\n\n");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  @Override
  public int run(String[] args) throws Exception {

    String in = null;
    String out = null;

    List<String> other_args = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-i".equals(args[i])) {
          in = args[++i];
        } else if ("-o".equals(args[i])) {
          out = args[++i];
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from "
            + args[i - 1]);
        return printUsage();
      }
    }

    if (other_args.size() > 0) {
      return printUsage();
    }

    if (in == null || out == null)
      return printUsage();

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - input path: " + in);
    LOG.info(" - output path: " + out);

    Configuration conf = getConf();
    Job job = new Job(conf, "Count genres");
    job.setJarByClass(CountGenres.class);

    // some weird issues with Thrift classes in the Hadoop distro.
    job.setUserClassesTakesPrecedence(true);

    // for the raw data:
    // job.setInputFormatClass(ThriftFileInputFormat.class);
    // for the repacked data:
    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setMapperClass(MyMapper.class);
    FileInputFormat.addInputPath(job, new Path(in));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);

    FileSystem.get(conf).delete(new Path(out), true);
    TextOutputFormat.setOutputPath(job, new Path(out));
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;

  }
}
