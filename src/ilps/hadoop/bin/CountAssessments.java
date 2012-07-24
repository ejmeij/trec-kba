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

import ilps.hadoop.StringLongPair;
import ilps.hadoop.StreamItemWritable;
import ilps.hadoop.ThriftFileInputFormat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Counts the number of assessments (rel/non-rel) per hour.  
 *  
 * @author emeij
 *
 */
public class CountAssessments extends Configured implements Tool {

  protected enum Counter {
    records, assessed
  };

  private static final Logger LOG = Logger.getLogger(CountAssessments.class);

  public static class MyMapper extends
      Mapper<Text, StreamItemWritable, Text, StringLongPair> {

    private final Text keyout = new Text();
    private final StringLongPair pair = new StringLongPair();

    private Set<String> relevant = new HashSet<String>();
    private Set<String> neutral = new HashSet<String>();
    private Set<String> nonrelevant = new HashSet<String>();

    private void loadAssessments(Context context) throws IOException {

      FSDataInputStream in = null;

      BufferedReader br = null;
      try {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path path = new Path(context.getConfiguration().get("ASSESSMENTS_HDFS"));
        in = fs.open(path);
        br = new BufferedReader(new InputStreamReader(in));
      } catch (FileNotFoundException e1) {
        throw new IOException("read from distributed cache: file not found!");
      } catch (Exception e) {
        throw new IOException(e);
      }

      try {

        String line;
        while ((line = br.readLine()) != null) {

          if (line.startsWith("#"))
            continue;

          /* 
           * NIST-TREC  annotators  1318243161-f3dc55e7157b77228e4fa5c7b0cfd151 Alexander_McCall_Smith  0 -1  1
          */
          String[] elements = line.split("\\s+");

          int rel = Integer.parseInt(elements[5]);
          if (rel < 0)
            nonrelevant.add(elements[2].toLowerCase());
          else if (rel > 0)
            relevant.add(elements[2].toLowerCase());
          else
            neutral.add(elements[2].toLowerCase());

        }

      } catch (IOException e) {
        LOG.error("Read error: " + e.getMessage());
        e.printStackTrace();
      } finally {
        in.close();
      }

      LOG.info(relevant.size() + " relevant assessments loaded");
      LOG.info(neutral.size() + " neutral assessments loaded");
      LOG.info(nonrelevant.size() + " nonrelevant assessments loaded");

    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      loadAssessments(context);
    }

    /** 
     * Not used
     */
    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      super.cleanup(context);
    }

    @Override
    public void map(Text key, StreamItemWritable value, Context context)
        throws IOException, InterruptedException {

      context.getCounter(Counter.records).increment(1);

      String id = value.getStream_id();
      if (relevant.contains(id)) {
        pair.set("relevant", 1);
        context.getCounter(Counter.assessed).increment(1);
      } else if (neutral.contains(id)) {
        pair.set("neutral", 1);
        context.getCounter(Counter.assessed).increment(1);
      } else if (nonrelevant.contains(id)) {
        pair.set("nonrelevant", 1);
        context.getCounter(Counter.assessed).increment(1);
      } else {
        pair.set("unjudged", 1);
      }

      // protocol:/path/2011-10-07-15/social.e9dd670bef76603b80dacc5a8ee049bf.4957bfe16ba4ab4c46eae4554bd56384
      String hour = key.toString();
      hour = hour.substring(
          hour.lastIndexOf('/', hour.lastIndexOf('/') - 1) + 1,
          hour.lastIndexOf('/'));

      keyout.set(hour);
      context.write(keyout, pair);

      pair.set("total", 1);
      context.write(keyout, pair);

    }
  }

  /**
   * "Counter" reducer. 
   * 
   * @author emeij
   *
   */
  public static class CountReducer extends
      Reducer<Text, StringLongPair, Text, StringLongPair> {

    private static final StringLongPair pair = new StringLongPair();

    @Override
    public void reduce(Text key, Iterable<StringLongPair> values,
        Context context) throws IOException, InterruptedException {

      long relsum = 0l;
      long neutralsum = 0l;
      long nonrelsum = 0l;
      long unjudgedsum = 0l;
      long totalsum = 0l;

      for (StringLongPair pair : values) {

        String rel = pair.getLeftElement();

        if ("relevant".equalsIgnoreCase(rel))
          relsum += pair.getRightElement();
        else if ("neutral".equalsIgnoreCase(rel))
          neutralsum += pair.getRightElement();
        else if ("nonrelevant".equalsIgnoreCase(rel))
          nonrelsum += pair.getRightElement();
        else if ("unjudged".equalsIgnoreCase(rel))
          unjudgedsum += pair.getRightElement();
        else
          totalsum += pair.getRightElement();

      }

      pair.set("relevant", relsum);
      context.write(key, pair);
      pair.set("neutral", neutralsum);
      context.write(key, pair);
      pair.set("nonrelevant", nonrelsum);
      context.write(key, pair);
      pair.set("unjudged", unjudgedsum);
      context.write(key, pair);
      pair.set("total", totalsum);
      context.write(key, pair);

    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CountAssessments(), args);
    System.exit(res);
  }

  static int printUsage() {
    System.out.println(CountAssessments.class.getName()
        + " -i input -o output -q assessmentsfile (on HDFS) \n");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  @Override
  public int run(String[] args) throws Exception {

    String in = null;
    String out = null;
    String assessmentsfile = null;

    List<String> other_args = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-i".equals(args[i])) {
          in = args[++i];
        } else if ("-o".equals(args[i])) {
          out = args[++i];
        } else if ("-q".equals(args[i])) {
          assessmentsfile = args[++i];
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

    if (in == null || out == null || assessmentsfile == null)
      return printUsage();

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - input path: " + in);
    LOG.info(" - output path: " + out);
    LOG.info(" - assessments path: " + assessmentsfile);

    Configuration conf = getConf();
    conf.set("ASSESSMENTS_HDFS", assessmentsfile);
    Job job = new Job(conf, "Count assessments");
    job.setJarByClass(CountAssessments.class);

    // some weird issues with Thrift classes in the Hadoop distro.
    job.setUserClassesTakesPrecedence(true);

    job.setInputFormatClass(ThriftFileInputFormat.class);
    job.setMapperClass(MyMapper.class);
    FileInputFormat.addInputPath(job, new Path(in));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(StringLongPair.class);

    job.setCombinerClass(CountReducer.class);
    job.setReducerClass(CountReducer.class);
    job.setNumReduceTasks(1);

    FileSystem.get(conf).delete(new Path(out), true);
    TextOutputFormat.setOutputPath(job, new Path(out));
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(StringLongPair.class);

    return job.waitForCompletion(true) ? 0 : 1;

  }
}
