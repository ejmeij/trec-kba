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

import ilps.hadoop.ResultObject;
import ilps.hadoop.StreamItemWritable;
import ilps.util.IO;
import ilps.util.Normalization;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Step one of the ILPS KBA system. It performs case-normalized document pre-selection based on lexical matching of all labels of the topics.    
 *  
 * @author emeij
 *
 */
public class IlpsKbaStepOne extends Configured implements Tool {

  public static final String TOPICTITLEFILEPATH_HDFS = "kba.topictitlefilelocation";
  public static final String NUMLABELS = "kba.numlabels";
  public static final String ASSESSMENTS_HDFS = "kba.assessments";

  public static final String AFTER = "kba.after";
  public static final String CUTOFF = "kba.cutoff";

  protected enum Counter {
    documents, matches, notassessed, aftercutoff
  };

  private static final Logger LOG = Logger.getLogger(IlpsKbaStepOne.class);

  /**
   * Emits date, StreamItemWritable pairs. 
   * 
   * @author emeij
   *
   */
  public static class MyMapper extends
      Mapper<Text, StreamItemWritable, Text, ResultObject> {

    private final Map<String, HashSet<String>> _topics = new LinkedHashMap<String, HashSet<String>>();
    private final Map<String, Integer> assessed = new HashMap<String, Integer>();
    private boolean haveAssessments = false;
    private double cutoff;
    private boolean after = false;

    /** 
     * Used to load the queries. 
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      super.setup(context);

      int init_num = Integer
          .parseInt(context.getConfiguration().get(NUMLABELS));
      Path f = new Path(context.getConfiguration().get(TOPICTITLEFILEPATH_HDFS));
      IO.readTopicTitles(context, init_num, f, _topics);

      if (context.getConfiguration().get(ASSESSMENTS_HDFS) != null) {
        Path path = new Path(context.getConfiguration().get(ASSESSMENTS_HDFS));
        IO.loadAssessments(context, path, assessed);
        haveAssessments = true;
      }

      String c = context.getConfiguration().get(CUTOFF);
      if (c == null)
        cutoff = Double.MAX_VALUE;
      else
        cutoff = Double.valueOf(c);

      String a = context.getConfiguration().get(AFTER);
      after = a == null ? false : Boolean.valueOf(a);

    }

    @Override
    public void map(Text date, StreamItemWritable value, Context context)
        throws IOException, InterruptedException {

      context.getCounter(Counter.documents).increment(1);
      String id = value.getStream_id();

      if (haveAssessments && !assessed.containsKey(id)) {
        context.getCounter(Counter.notassessed).increment(1);
        return; // next doc
      }

      // after the cut-off point
      if (after) {
        if (value.getStream_time().epoch_ticks < cutoff) {// 1326334731d) //
                                                          // should be
                                                          // 1325375954
          context.getCounter(Counter.aftercutoff).increment(1);
          return;
        }
      } else {
        if (value.getStream_time().epoch_ticks > cutoff) {// 1326334731d)
          context.getCounter(Counter.aftercutoff).increment(1);
          return;
        }
      }

      String body = Normalization.parseElement(value.getBody().getCleansed());
      String title = Normalization.parseElement(value.getTitle().getCleansed());
      String anchor = Normalization.parseElement(value.getAnchor()
          .getCleansed());
      String url = Normalization.parseElement(value.getOriginal_url());

      TOPIC: for (String topic : _topics.keySet()) {

        for (String t : _topics.get(topic)) {

          context.setStatus(t.toString());

          if (title.contains(t) || url.contains(t) || anchor.contains(t)
              || body.contains(t)) {

            context.getCounter(Counter.matches).increment(1);
            context.write(date,
                new ResultObject(value, 1l, topic, value.getStream_id()));

            continue TOPIC;
          }

        }
      }

    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new IlpsKbaStepOne(), args);
    System.exit(res);
  }

  static int printUsage() {
    System.out
        .println("Usage: "
            + IlpsKbaStepOne.class.getName()
            + " -i input -o output -q query_file (local) [-f] [-r num_reducers] [-c cutoff] [-after] [-a assessmentsfile (HDFS)]\n\n"
            + "  -f will overwrite the output folder\n"
            + "  if -a is defined, it will consider only the assessments\n\n"
            + "Example usage: hadoop jar trec-kba.jar "
            + IlpsKbaStepOne.class.getName() + " "
            + "-i kba/tiny-kba-stream-corpus/*/* "
            + "-o kba/tiny-kba-stream-corpus-out "
            + "-q filter-topics.sample-trec-kba-targets-2012.json \n\n");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  @Override
  public int run(String[] args) throws Exception {

    String in = null;
    String out = null;
    String queryfile = null;
    boolean force = false;
    String assessmentsfile = null;
    String cutoff_str = null;
    Double cutoff = null;
    Boolean after = false;
    Integer reducers = null;

    List<String> other_args = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-i".equals(args[i])) {
          in = args[++i];
        } else if ("-o".equals(args[i])) {
          out = args[++i];
        } else if ("-q".equals(args[i])) {
          queryfile = args[++i];
        } else if ("-f".equals(args[i])) {
          force = true;
        } else if ("-a".equals(args[i])) {
          assessmentsfile = args[++i];
        } else if ("-after".equals(args[i])) {
          after = true;
        } else if ("-c".equals(args[i])) {
          cutoff_str = args[++i];
        } else if ("-r".equals(args[i])) {
          reducers = Integer.parseInt(args[++i]);
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

    if (other_args.size() > 0 || in == null || out == null || queryfile == null)
      return printUsage();

    if (cutoff_str == null)
      cutoff = Double.MAX_VALUE;
    else
      cutoff = Double.valueOf(cutoff_str);

    LOG.info("Tool: " + this.getClass().getName());
    LOG.info(" - input path: " + in);
    LOG.info(" - output path: " + out);
    LOG.info(" - queryfile: " + queryfile);
    LOG.info(" - cutoff: " + cutoff);
    LOG.info(" - consider documents after the cutoff: " + after);

    Path _tmp = new Path(IlpsKbaStepOne.class.getSimpleName() + "_TMP_"
        + new Random().nextInt());

    LOG.info(" - _tmp: " + _tmp);

    Configuration conf = getConf();
    conf.set(TOPICTITLEFILEPATH_HDFS, _tmp.toString());
    conf.set(CUTOFF, cutoff.toString());
    conf.set(AFTER, after.toString());

    if (assessmentsfile != null) {
      LOG.info(" - assessments path: " + assessmentsfile);
      conf.set(ASSESSMENTS_HDFS, assessmentsfile);
    }

    int num = IO.WriteTopicTitles(new File(queryfile), _tmp,
        FileSystem.get(conf));
    LOG.info(num + " labels");
    conf.set(NUMLABELS, Integer.toString(num));

    Job job = new Job(conf, "ILPS KBA system // Step 1");
    job.setJarByClass(IlpsKbaStepOne.class);

    // some weird issues with Thrift classes in the Hadoop distro.
    job.setUserClassesTakesPrecedence(true);

    // make the query file available to each mapper.
    DistributedCache.addCacheFile(
        new URI(_tmp + "#" + TOPICTITLEFILEPATH_HDFS), job.getConfiguration());
    DistributedCache.createSymlink(job.getConfiguration());

    // for the raw data:
    // job.setInputFormatClass(ThriftFileInputFormat.class);
    // for the repacked data:
    job.setInputFormatClass(SequenceFileInputFormat.class);

    FileInputFormat.addInputPath(job, new Path(in));

    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ResultObject.class);

    job.setReducerClass(Reducer.class); // TODO actually, split per dir name?
    if (reducers != null)
      job.setNumReduceTasks(reducers);

    // delete the output dir
    if (force)
      FileSystem.get(conf).delete(new Path(out), true);

    SequenceFileOutputFormat.setOutputPath(job, new Path(out));
    SequenceFileOutputFormat.setCompressOutput(job, false);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ResultObject.class);

    // Let's go
    int status = job.waitForCompletion(true) ? 0 : 1;

    Counters c = job.getCounters();
    long total = c.findCounter(Counter.documents).getValue();
    long matches = c.findCounter(Counter.matches).getValue();

    LOG.info("Done. " + ((double) matches / total)
        + " of the documents matched any topic.");

    // clean up
    FileSystem.get(conf).delete(_tmp, false);
    return status;

  }
}
