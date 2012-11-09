package ilps.util;

import java.io.IOException;
import java.io.StringReader;
import java.text.Normalizer;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

public class Normalization {

  // private static final Logger LOG = Logger.getLogger(IO.class);
  private final static Pattern pattern = Pattern
      .compile("\\p{InCombiningDiacriticalMarks}+");

  /**
   * lowercases and removes diacritics, see http://stackoverflow.com/questions/1008802/converting-symbols-accent-letters-to-english-alphabet
   * @param str
   * @return
   */
  public static String normalize(final String str) {
    String norm = Normalizer.normalize(str.toLowerCase(), Normalizer.Form.NFD);
    return pattern.matcher(norm).replaceAll("");
  }

  public static String parseElement(byte[] in) {

    if (in == null)
      return "";

    return normalize(new String(in));
  }

  /**
   * 
   * @param in
   * @param analyzer 
   * @return
   * @throws IOException
   */
  @SuppressWarnings("deprecation")
  public static String tokenize(String in, Analyzer analyzer)
      throws IOException {

    StringBuilder out = new StringBuilder();
    TokenStream tokenStream = analyzer.tokenStream("content", new StringReader(
        normalize(in)));
    Token t = null;

    while ((t = tokenStream.next()) != null) {
      out.append(" ");
      out.append(t.term());
    }

    tokenStream.close();
    return out.substring(1, out.length());

  }

}
