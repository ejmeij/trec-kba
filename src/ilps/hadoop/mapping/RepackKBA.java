/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package ilps.hadoop.mapping;

import ilps.hadoop.StreamItemWritable;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Tool for repacking Wikipedia XML dumps into <code>SequenceFiles</code>.
 *
 * @author Jimmy Lin
 */
public class RepackKBA extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RepackKBA.class);

  private static enum Records {
    TOTAL
  };

  private static class MyMapper extends MapReduceBase implements
      Mapper<Text, StreamItemWritable, IntWritable, StreamItemWritable> {

    private static final IntWritable docno = new IntWritable();
    private static final KBADocnoMapping docnoMapping = new KBADocnoMapping();

    public void configure(JobConf job) {
      try {
        Path p = new Path(job.get(DOCNO_MAPPING_FIELD));
        LOG.info("Loading docno mapping: " + p);

        FileSystem fs = FileSystem.get(job);
        if (!fs.exists(p)) {
          throw new RuntimeException(p + " does not exist!");
        }

        docnoMapping.loadMapping(p, fs);
      } catch (Exception e) {
        throw new RuntimeException("Error loading docno mapping data file!");
      }

    }

    public void map(Text key, StreamItemWritable doc,
        OutputCollector<IntWritable, StreamItemWritable> output,
        Reporter reporter) throws IOException {
      reporter.incrCounter(Records.TOTAL, 1);
      String id = doc.getStream_id();

      if (id != null) {
        // We're going to discard pages that aren't in the docno mapping.
        int n = docnoMapping.getDocno(id);
        if (n >= 0) {
          docno.set(n);
          output.collect(docno, doc);
        }
      }
    }
  }

  private static final String DOCNO_MAPPING_FIELD = "DocnoMappingDataFile";

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String MAPPING_FILE_OPTION = "mapping_file";

  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output location").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("mapping file").create(MAPPING_FILE_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)
        || !cmdline.hasOption(MAPPING_FILE_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);
    String mappingFile = cmdline.getOptionValue(MAPPING_FILE_OPTION);

    JobConf conf = new JobConf(getConf(), RepackKBA.class);
    conf.setJobName(String.format("RepackKBA[%s: %s, %s: %s]", INPUT_OPTION,
        inputPath, OUTPUT_OPTION, outputPath));

    conf.set(DOCNO_MAPPING_FIELD, mappingFile);

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - XML dump file: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - docno mapping data file: " + mappingFile);

    int mapTasks = 10;

    conf.setNumMapTasks(mapTasks);
    conf.setNumReduceTasks(0);

    SequenceFileInputFormat.addInputPath(conf, new Path(inputPath));
    SequenceFileOutputFormat.setOutputPath(conf, new Path(outputPath));

    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressionType(conf,
        SequenceFile.CompressionType.BLOCK);

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(StreamItemWritable.class);

    conf.setMapperClass(MyMapper.class);

    // some weird issues with Thrift classes in the Hadoop distro.
    conf.setUserClassesTakesPrecedence(true);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    JobClient.runJob(conf);

    return 0;
  }

  public RepackKBA() {
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RepackKBA(), args);
  }
}
