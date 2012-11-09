package ilps.util;

import ilps.json.run.Filter_run;
import ilps.json.topics.Filter_topics;
import ilps.util.WittenWikipediaWebservice.Item;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.log4j.Logger;

@SuppressWarnings("rawtypes")
public class IO {

  private static final Logger LOG = Logger.getLogger(IO.class);

  protected enum Assessments {
    conflict
  };

  /**
   * Loads the JSON topic file.
   * 
   * @param context
   */
  public static void loadTopicData(String queryfile, Filter_run fr,
      FileSystem fs, HashMap<String, Object> run_info) {

    InputStream in = null;
    try {

      in = new FileInputStream(new File(queryfile));
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

  /**
   * Loads the queries from the JSON topic file and writes them to an output file on HDFS. Also performs normalization (case folding and diacritics removal).
   * 
   * @param context
   */
  public static int WriteTopicTitles(File queryfile, Path topicpath,
      FileSystem fs) {

    DataOutputStream out = null;
    DataInputStream in = null;
    int total = 0;

    try {

      if (fs.exists(topicpath))
        fs.delete(topicpath, false);

      out = fs.create(topicpath);

      in = new DataInputStream(new FileInputStream(queryfile));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      Filter_topics ft = new Filter_topics.Factory().loadTopics(br);

      LOG.info(ft.getTopic_set_id());

      for (String t : ft.getTopic_names()) {

        String topic = t;
        // System.out.println(t);

        HashSet<String> pset = new HashSet<String>();

        t = t.replaceAll("_", " ");
        pset.add(t.toLowerCase());

        for (Item o : WittenWikipediaWebservice.getExpansions(t)) {

          String label = Normalization.normalize(o.text);

          if (!pset.contains(label)) {

            pset.add(label);

          }
        }

        // write them
        total += pset.size() + 1;
        out.writeInt(pset.size() + 1);
        out.writeUTF(topic);
        for (String r : pset) {
          out.writeUTF(r);
          // System.out.println("\t" + r);
        }

      }

    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {

      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return total;
  }

  /**
   * Loads the topics and associated labels.
   * 
   * @param context
   */
  public static void readTopicTitles(Context context, int init_num, Path f,
      Map<String, HashSet<String>> _topics) throws IOException {

    int total = 0;
    DataInputStream in = null;

    try {

      FileSystem fs = FileSystem.get(context.getConfiguration());
      in = fs.open(f);
      // new DataInputStream(new FileInputStream(TOPICTITLEFILEPATH_HDFS));

      while (true) {

        HashSet<String> pset = new HashSet<String>();

        int size = in.readInt();
        total += size;
        String topic = in.readUTF();

        for (int i = 1; i < size; i++) {
          pset.add(in.readUTF());
        }

        _topics.put(topic, pset);

      }
    } catch (EOFException e) {

    } finally {

      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

    }

    /*for (String t : _topics.keySet()) {
      System.out.println(t);
      for (String r : _topics.get(t)) {
        System.out.println("\t" + r);
      }
    }
    */

    LOG.info(total + " labels");
    if (total != init_num)
      throw new IOException("Incorrect number of labels loaded!");
  }

  public static void loadAssessments(Context context, Path path,
      Map<String, Integer> assessed) throws IOException {
    loadAssessments(context, path, assessed, null);
  }

  /**
   * 
   * @param context
   * @param path
   * @param assessed
   * @param removeConflicts when false, does not remove the conflicting assessments (default true). 
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static void loadAssessments(Context context, Path path,
      Map<String, Integer> assessed, Boolean removeConflicts)
      throws IOException {

    // default true
    removeConflicts = removeConflicts == null ? true : removeConflicts;

    FSDataInputStream in = null;
    BufferedReader br = null;
    assessed.clear();
    Set<String> conflicts = new HashSet<String>();

    try {
      FileSystem fs = FileSystem.get(context.getConfiguration());
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
        Integer rel = Integer.parseInt(elements[5]);

        if (assessed.containsKey(elements[2].toLowerCase())) {
          // dupes: added Sep 26 2012
          int oldrel = assessed.get(elements[2].toLowerCase());
          if (oldrel != rel) { // conflict
            context.getCounter(Assessments.conflict).increment(1);
            conflicts.add(elements[2].toLowerCase());
          }
        } else {
          assessed.put(elements[2].toLowerCase(), rel);
        }

      }

    } catch (IOException e) {
      LOG.error("Read error: " + e.getMessage());
      e.printStackTrace();
    } finally {
      in.close();
    }

    for (String c : conflicts) {
      assessed.remove(c);
    }

    LOG.info(assessed.size() + " assessments loaded");

  }
}
