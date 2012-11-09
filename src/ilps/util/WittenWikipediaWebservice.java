package ilps.util;

import ilps.json.topics.Filter_topics;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class WittenWikipediaWebservice {

  private static final Logger LOG = Logger
      .getLogger(WittenWikipediaWebservice.class);

  public static class Item {

    public String text;
    public double proportion;

    public Item() {

    }

    public Item(String text, double proportion) {
      this.text = text;
      this.proportion = proportion;
    }

  }

  public static class Title extends Item {
    public Title(String text, double proportion) {
      super(text, proportion);
    }
  }

  public static class Anchor extends Item {
    public Anchor(String text, double proportion) {
      super(text, proportion);
    }
  }

  public static class Redirect extends Item {
    public Redirect(String text, double proportion) {
      super(text, proportion);
    }
  }

  private static String mapToString(Map<String, String> map) {
    StringBuilder stringBuilder = new StringBuilder();

    for (String key : map.keySet()) {
      if (stringBuilder.length() > 0) {
        stringBuilder.append("&");
      }
      String value = map.get(key);
      try {
        stringBuilder.append((key != null ? URLEncoder.encode(key, "UTF-8")
            : ""));
        stringBuilder.append("=");
        stringBuilder.append(value != null ? URLEncoder.encode(value, "UTF-8")
            : "");
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(
            "This method requires UTF-8 encoding support", e);
      }
    }

    return stringBuilder.toString();
  }

  public static Set<Item> getExpansions(String q)
      throws ParserConfigurationException, SAXException, IOException {

    HashSet<Item> topics = new HashSet<Item>();
    Map<String, String> map = new HashMap<String, String>();
    map.put("title", q);
    map.put("labels", "true");

    Document doc;
    URL url;
    try {
      url = new URL(
          "http://wikipedia-miner.cms.waikato.ac.nz/services/exploreArticle?"
              + mapToString(map));
      doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
          .parse(url.openStream());
    } catch (IOException e) {
      e.printStackTrace();
      return topics;
    }

    doc.getDocumentElement().normalize();

    NodeList listOfLabels = doc.getElementsByTagName("label");
    for (int l = 0; l < listOfLabels.getLength(); l++) {

      Element label = (Element) listOfLabels.item(l);
      double p = Double.parseDouble(label.getAttribute("proportion"));

      // stupid bug
      if (label.getAttribute("fromRedirect").equalsIgnoreCase("true"))
        topics.add(new Title(label.getAttribute("text"), p));
      else if (label.getAttribute("fromTitle").equalsIgnoreCase("true"))
        topics.add(new Redirect(label.getAttribute("text"), p));
      else
        topics.add(new Anchor(label.getAttribute("text"), p));

    }

    if (topics.size() == 0) {
      System.err.println(url);
      throw new IOException();
    }

    return topics;
  }

  public static String getParagraph(String q)
      throws ParserConfigurationException, SAXException, IOException {
    Map<String, String> map = new HashMap<String, String>();
    map.put("definitionLength", "long");
    return getArticle(q, map);
  }

  public static String getSentence(String q)
      throws ParserConfigurationException, SAXException, IOException {
    Map<String, String> map = new HashMap<String, String>();
    map.put("definitionLength", "short");
    return getArticle(q, map);
  }

  private static String getArticle(String q, Map<String, String> map)
      throws ParserConfigurationException, SAXException, IOException {

    map.put("title", q);
    map.put("definition", "true");
    map.put("linkFormat", "plain");
    map.put("emphasisFormat", "WIKI");

    StringBuilder out = new StringBuilder();
    URL url = new URL(
        "http://wikipedia-miner.cms.waikato.ac.nz/services/exploreArticle?"
            + mapToString(map));

    Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder()
        .parse(url.openStream());
    doc.getDocumentElement().normalize();

    NodeList listOfDefinitions = doc.getElementsByTagName("definition");
    for (int i = 0; i < listOfDefinitions.getLength(); i++) {
      Element def = (Element) listOfDefinitions.item(i);
      out.append(def.getTextContent());
    }

    if (out.length() == 0) {
      System.err.println(url);
      LOG.warn("WARNING! Result is empty for " + q);
      // throw new IOException();
      return "";

    }

    return out.toString();
  }

  public static void main(String args[]) {

    DataInputStream in = null;

    try {

      String queryfile = args[0];
      in = new DataInputStream(new FileInputStream(queryfile));
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      Filter_topics ft = new Filter_topics.Factory().loadTopics(br);

      LOG.info(ft.getTopic_set_id());

      for (String t : ft.getTopic_names()) {

        HashSet<String> pset = new HashSet<String>();
        String topic = t;
        t = t.replaceAll("_", " ");
        pset.add(t.toLowerCase());

        for (Item o : WittenWikipediaWebservice.getExpansions(t)) {
          String label = Normalization.normalize(o.text);
          if (!pset.contains(label)) {
            pset.add(label);
          }
        }

        System.out.println(topic + "\t" + pset.size());

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
    }
  }
}
