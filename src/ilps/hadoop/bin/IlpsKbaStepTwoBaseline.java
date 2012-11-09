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
import ilps.json.run.Filter_run;
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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Baseline ILPS KBA system. It builds on the results of step one and performs case-normalized linking based on lexical matching of all labels of the topics.    
 *  
 * @author emeij
 *
 */
public class IlpsKbaStepTwoBaseline extends Configured implements Tool {

  public static final String RUNTAG = "kba.runtag";
  public static final String TEAMNAME = "kba.teamname";

  public static final String TOPICTITLEFILEPATH_HDFS = "kba.topictitlefilelocation";
  public static final String NUMLABELS = "kba.numlabels";

  protected enum Counter {
    documents, matches
  };

  private static final Logger LOG = Logger
      .getLogger(IlpsKbaStepTwoBaseline.class);

  public static class MyReducer extends Reducer<Text, ResultObject, Text, Text> {

    private static final Text out = new Text();
    private static final Text nil = new Text("");

    private String runtag;
    private String teamname;

    @Override
    public void setup(Context context) throws InterruptedException, IOException {

      super.setup(context);

      teamname = context.getConfiguration().get(TEAMNAME);
      runtag = context.getConfiguration().get(RUNTAG);

    }

    @Override
    public void reduce(Text date, Iterable<ResultObject> values, Context context)
        throws IOException, InterruptedException {

      // remove duplicates for this date
      Set<String> seenItems = new HashSet<String>();

      for (ResultObject result : values) {

        String line = teamname + " " + runtag + " " + result.getStream_id()
            + " " + result.getTopic() + " " + result.getScore().toString();

        out.set(line);

        if (!seenItems.contains(line)) {
          context.write(out, nil);
          seenItems.add(line);
        }
      }
    }
  }

  /**
   * Emits date, PairOfStringLong pairs, where the string contains the docno and the topic and the long contains the score. 
   * 
   * @author emeij
   *
   */
  public static class MyMapper extends
      Mapper<Text, ResultObject, Text, ResultObject> {

    private Map<String, HashSet<String>> topiclabels = new LinkedHashMap<String, HashSet<String>>();

    /** 
     * Used to load the queries. 
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      super.setup(context);

      int init_num = Integer
          .parseInt(context.getConfiguration().get(NUMLABELS));
      Path f = new Path(context.getConfiguration().get(TOPICTITLEFILEPATH_HDFS));
      IO.readTopicTitles(context, init_num, f, topiclabels);

    }

    @Override
    public void map(Text date, ResultObject value, Context context)
        throws IOException, InterruptedException {

      StreamItemWritable sdoc = value.getDocument();

      // build a pseudo-doc
      StringBuilder document_sb = new StringBuilder();
      document_sb
          .append(Normalization.parseElement(sdoc.getOriginal_url()) + '\n');
      document_sb.append(Normalization.parseElement(sdoc.getTitle()
          .getCleansed()) + '\n');
      document_sb.append(Normalization.parseElement(sdoc.getAnchor()
          .getCleansed()) + '\n');
      document_sb.append(Normalization.parseElement(sdoc.getBody()
          .getCleansed()) + '\n');
      String document = document_sb.toString();

      for (String topic : topiclabels.keySet()) {

        // long count = 0;
        double num_hits = 0d;

        // get all labels
        for (String t : topiclabels.get(topic)) {

          context.setStatus(t.toString());

          if (document.contains(t)) {
            context.getCounter(Counter.matches).increment(1);
            num_hits += document.split(t).length - 1; // quick hack to get "TF"
          }

        }

        if (num_hits > 0) {

          // naive
          // score based on this document, topic, and label
          // count = (long) (1000d * num_hits * (1d /
          // document.split("\\s+").length));

          context.write(date, new ResultObject(sdoc, (long) num_hits, topic,
              value.getStream_id()));

        } // end if

      } // end topics
    } // end function
  } // end class

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new IlpsKbaStepTwoBaseline(),
        args);
    System.exit(res);
  }

  static int printUsage() {
    System.out
        .println("Usage: "
            + IlpsKbaStepTwoBaseline.class.getName()
            + " -i input -o output -q query_file (local)[-f] [-c corpus_id] -r runtag -t teamname [-d description] \n\n"
            + "  -f will overwrite the output folder\n\n"
            + "Example usage: hadoop jar trec-kba.jar "
            + IlpsKbaStepTwoBaseline.class.getName() + " "
            + "-i kba/tiny-kba-stream-corpus/*/* "
            + "-o kba/tiny-kba-stream-corpus-out "
            + "-q kba/filter-topics.sample-trec-kba-targets-2012.json \n\n");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  @Override
  public int run(String[] args) throws Exception {

    String in = null;
    String out = null;
    String queryfile = null;
    String systemdescription = null;
    String corpus_id = null;
    String runtag = null;
    String teamname = null;

    boolean force = false;
    HashMap<String, Object> run_info = new HashMap<String, Object>();

    List<String> other_args = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-i".equals(args[i])) {
          in = args[++i];
        } else if ("-o".equals(args[i])) {
          out = args[++i];
        } else if ("-q".equals(args[i])) {
          queryfile = args[++i];
        } else if ("-r".equals(args[i])) {
          runtag = args[++i];
        } else if ("-t".equals(args[i])) {
          teamname = args[++i];
        } else if ("-d".equals(args[i])) {
          systemdescription = args[++i];
        } else if ("-c".equals(args[i])) {
          corpus_id = args[++i];
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

    if (other_args.size() > 0 || in == null || out == null || queryfile == null
        || runtag == null || teamname == null)
      return printUsage();

    if (corpus_id == null)
      corpus_id = "kba-stream-corpus-2012-cleansed-only";

    if (systemdescription == null)
      systemdescription = "Description intentionally left blank.";

    LOG.info("Tool: " + this.getClass().getName());
    LOG.info(" - input path: " + in);
    LOG.info(" - output path: " + out);
    LOG.info(" - runtag: " + runtag);
    LOG.info(" - teamname: " + teamname);
    LOG.info(" - corpus_id: " + corpus_id);
    LOG.info(" - run description: " + systemdescription);

    Filter_run fr = new Filter_run.Factory().create(teamname, runtag,
        systemdescription, corpus_id);

    Path _tmp = new Path(IlpsKbaStepTwoBaseline.class.getSimpleName() + "_TMP_"
        + new Random().nextInt());

    LOG.info(" - _tmp: " + _tmp);

    Configuration conf = getConf();
    conf.set(TOPICTITLEFILEPATH_HDFS, _tmp.toString());
    conf.set(RUNTAG, runtag);
    conf.set(TEAMNAME, teamname);

    FileSystem fs = FileSystem.get(conf);

    int num = IO.WriteTopicTitles(new File(queryfile), _tmp, fs);
    LOG.info(num + " labels");
    conf.set(NUMLABELS, Integer.toString(num));

    // Lookup required data from the topic file
    IO.loadTopicData(queryfile, fr, fs, run_info);

    Job job = new Job(conf, "ILPS KBA system // Step 2 // Baseline");
    job.setJarByClass(IlpsKbaStepTwoBaseline.class);

    // some weird issues with Thrift classes in the Hadoop distro.
    job.setUserClassesTakesPrecedence(true);

    // make the query file available to each mapper.
    DistributedCache.addCacheFile(
        new URI(_tmp + "#" + TOPICTITLEFILEPATH_HDFS), job.getConfiguration());
    DistributedCache.createSymlink(job.getConfiguration());

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(MyMapper.class);
    FileInputFormat.addInputPath(job, new Path(in));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(ResultObject.class);

    // job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    job.setNumReduceTasks(528);

    // delete the output dir
    if (force)
      FileSystem.get(conf).delete(new Path(out), true);

    TextOutputFormat.setOutputPath(job, new Path(out));
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Let's go
    int status = job.waitForCompletion(true) ? 0 : 1;

    // add some more statistics
    Counters c = job.getCounters();
    long cputime = c.findCounter(
        org.apache.hadoop.mapred.Task.Counter.CPU_MILLISECONDS).getValue();
    run_info.put("elapsed_time_secs", ((double) cputime / 1000d));

    long num_filter_results = c.findCounter(
        org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS).getValue();
    run_info.put("num_filter_results", num_filter_results);

    long num_entity_doc_compares = c.findCounter(
        org.apache.hadoop.mapred.Task.Counter.MAP_INPUT_RECORDS).getValue();
    run_info.put("num_entity_doc_compares", num_entity_doc_compares);

    long hours = c.findCounter(
        org.apache.hadoop.mapred.Task.Counter.REDUCE_INPUT_GROUPS).getValue();
    run_info.put("num_stream_hours", hours);

    long total = c.findCounter(Counter.documents).getValue();
    long matches = c.findCounter(Counter.matches).getValue();

    run_info.put("percentage_matches", ((double) matches / total));
    LOG.info("Done. " + ((double) matches / total)
        + " of the documents matched any topic.");

    fr.setAdditionalProperties("run_info", run_info);

    System.out.println("#" + new Filter_run.Factory().toJSON(fr));

    Path path = new Path(out);
    FileStatus fstatus[] = fs.listStatus(path);
    for (FileStatus f : fstatus) {

      System.err.println(f.getPath().getName() + "...");

      if (!f.getPath().getName().startsWith("part-"))
        continue;

      Text line = new Text();
      LineReader reader = new LineReader(fs.open(f.getPath()));
      for (int i = 0; i < num_filter_results; i++) {
        reader.readLine(line);
        String l = line.toString();
        if (l.trim().length() > 0)
          System.out.println(l);
      }

    }

    System.out.println("#"
        + new Filter_run.Factory().toPrettyJSON(fr).replaceAll("\\n", "\n#"));

    // clean up
    FileSystem.get(conf).delete(_tmp, false);
    return status;

  }
}
