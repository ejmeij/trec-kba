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
import ilps.hadoop.StringLongPair;
import ilps.json.run.Filter_run;
import ilps.json.topics.Filter_topics;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
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
 * Toy KBA system similar to the Python version. It identifies entities in documents based on mere lexical matching.    
 *  
 * @author emeij
 *
 */
public class ToyKbaSystem extends Configured implements Tool {

  public static final String QUERYFILEPATH_HDFS = "kba.topicfilelocation";
  public static final String RUNTAG = "kba.runtag";
  public static final String TEAMNAME = "kba.teamname";

  protected enum Counter {
    documents
  };

  private static final Logger LOG = Logger.getLogger(ToyKbaSystem.class);

  public static class MyReducer extends
      Reducer<Text, StringLongPair, Text, Text> {

    private static final Text out = new Text();
    private static final Text none = new Text("");

    private String runtag;
    private String teamname;

    @Override
    public void setup(Context context) throws InterruptedException, IOException {

      super.setup(context);

      teamname = context.getConfiguration().get(TEAMNAME);
      runtag = context.getConfiguration().get(RUNTAG);

    }

    @Override
    public void reduce(Text date, Iterable<StringLongPair> values,
        Context context) throws IOException, InterruptedException {

      for (StringLongPair pair : values) {

        String doc_entity = pair.getLeftElement();
        Long count = pair.getRightElement();

        out.set(teamname + " " + runtag + " " + doc_entity + " "
            + count.toString());
        context.write(out, none);

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
      Mapper<Text, StreamItemWritable, Text, StringLongPair> {

    private static final Text date = new Text();
    private static final StringLongPair scorepair = new StringLongPair();
    private Map<String, Pattern> topicregexes = new LinkedHashMap<String, Pattern>();
    private Map<String, HashSet<String>> partialtopics = new LinkedHashMap<String, HashSet<String>>();
    private Filter_topics ft = null;

    /** 
     * Used to load the queries. 
     */
    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      super.setup(context);
      loadTopics(context);

    }

    /**
     * Loads the queries from the JSON topic file.
     * 
     * @param context
     */
    private void loadTopics(Context context) {

      DataInputStream in = null;
      try {

        in = new DataInputStream(new FileInputStream(QUERYFILEPATH_HDFS));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        ft = new Filter_topics.Factory().loadTopics(br);

        LOG.info(ft.getTopic_set_id());

        for (String t : ft.getTopic_names()) {

          Pattern p;

          // add the full name
          p = Pattern.compile(".*\\b+" + t.replaceAll("_", " ") + "\\b+.*",
              Pattern.CASE_INSENSITIVE);
          topicregexes.put(t, p);

          // add the individual terms
          HashSet<String> pset = new HashSet<String>();
          pset.addAll(new HashSet<String>(Arrays.asList(t.split("_"))));
          pset.add(t.replaceAll("_", " "));
          partialtopics.put(t, pset);

        }

      } catch (IOException e) {
        e.printStackTrace();
        LOG.error("read from distributed cache: read instances");
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error("read from distributed cache: " + e);
      } finally {

        if (in != null) {
          try {
            in.close();
          } catch (IOException e1) {
            e1.printStackTrace();
          }
        }
      }
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

      context.getCounter(Counter.documents).increment(1);

      String body = new String(value.getBody().getCleansed());
      String streamid = value.getStream_id();

      String filename = key.toString();
      String dirdate = filename.substring(
          filename.lastIndexOf('/', filename.lastIndexOf('/') - 1) + 1,
          filename.lastIndexOf('/'));
      date.set(dirdate);

      for (String topic : topicregexes.keySet()) {

        long count = 0;
        String entity = streamid + " " + topic;
        Map<String, Long> counts = new LinkedHashMap<String, Long>();

        for (String t : partialtopics.get(topic)) {

          context.setStatus(t.toString());

          if (body.contains(t))
            counts.put(t, (long) t.length());

          /*
                    Matcher matcher = p.matcher(body);
                    while (matcher.find())
                      count++;

          */
        }

        // if (count > 0) {
        if (counts.size() > 0) {

          // calculate the score as the relative frequency of occurring of the
          // entity in the document.
          // count = 1000 * (count * topic.length()) / body.length();

          count = 1000 * Collections.max(counts.values()) / topic.length();

          scorepair.set(entity, count);
          context.write(date, scorepair);
        }
      }
    }

  }

  /**
   * Loads the JSON topic file.
   * 
   * @param context
   */
  private static void loadTopicData(String queryfile, Filter_run fr,
      FileSystem fs, HashMap<String, Object> run_info) {

    FSDataInputStream in = null;
    try {

      in = fs.open(new Path(queryfile));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      Filter_topics ft = new Filter_topics.Factory().loadTopics(br);

      fr.setTopic_set_id(ft.getTopic_set_id());
      run_info.put("num_entities", ft.getTopic_names().size());

    } catch (IOException e1) {
      e1.printStackTrace();
    } catch (Exception e1) {
      e1.printStackTrace();
    } finally {

      if (in != null) {
        try {
          in.close();
        } catch (IOException e1) {
          e1.printStackTrace();
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ToyKbaSystem(), args);
    System.exit(res);
  }

  static int printUsage() {
    System.out
        .println("Usage: "
            + ToyKbaSystem.class.getName()
            + " -i input -o output -q query_file (hdfs) [-f] [-c corpus_id -r runtag -t teamname -d description] \n\n"
            + "  -f will overwrite the output folder\n\n"
            + "Example usage: hadoop jar trec-kba.jar "
            + ToyKbaSystem.class.getName() + " "
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

    if (other_args.size() > 0 || in == null || out == null || queryfile == null)
      return printUsage();

    if (runtag == null)
      runtag = "toy_1";

    if (teamname == null)
      teamname = "CompInsights";

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

    Filter_run fr = new Filter_run.Factory().create(TEAMNAME, RUNTAG,
        systemdescription, corpus_id);

    Configuration conf = getConf();
    conf.set(QUERYFILEPATH_HDFS, new Path(queryfile).toUri().toString());
    conf.set(RUNTAG, runtag);
    conf.set(TEAMNAME, teamname);

    FileSystem fs = FileSystem.get(conf);
    // Lookup required data from the topic file
    loadTopicData(queryfile, fr, fs, run_info);

    Job job = new Job(conf, "Toy KBA system");
    job.setJarByClass(ToyKbaSystem.class);

    // some weird issues with Thrift classes in the Hadoop distro.
    job.setUserClassesTakesPrecedence(true);

    // make the query file available to each mapper.
    DistributedCache.addCacheFile(new URI(new Path(queryfile) + "#"
        + QUERYFILEPATH_HDFS), job.getConfiguration());
    DistributedCache.createSymlink(job.getConfiguration());

    // for the raw data:
    // job.setInputFormatClass(ThriftFileInputFormat.class);
    // for the repacked data:
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(MyMapper.class);
    FileInputFormat.addInputPath(job, new Path(in));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(StringLongPair.class);

    // job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    job.setNumReduceTasks(1);

    // delete the output dir
    if (force)
      FileSystem.get(conf).delete(new Path(out), true);
    TextOutputFormat.setOutputPath(job, new Path(out));
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Let's go
    int status = job.waitForCompletion(true) ? 0 : 1;

    /*
    for (String g : job.getCounters().getGroupNames()) {

      Iterator<org.apache.hadoop.mapreduce.Counter> it = job.getCounters()
          .getGroup(g).iterator();

      LOG.info(g + "\t" + job.getCounters().getGroup(g).getDisplayName());

      while (it.hasNext()) {
        org.apache.hadoop.mapreduce.Counter c = it.next();
        LOG.info("\t" + c.getDisplayName() + "\t" + c.getValue());
      }
    }
    */

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

    fr.setAdditionalProperties("run_info", run_info);

    System.out.println("#" + new Filter_run.Factory().toJSON(fr));

    Text line = new Text();
    LineReader reader = new LineReader(fs.open(new Path(out + "/part-r-00000")));
    for (int i = 0; i < num_filter_results; i++) {
      reader.readLine(line);
      System.out.println(line.toString());
    }

    System.out.println("#"
        + new Filter_run.Factory().toPrettyJSON(fr).replaceAll("\\n", "\n#"));

    return status;

  }
}
