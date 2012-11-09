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

package ilps.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

/** 
 * Class to serialize the result of step one of the ILPS KBA system <b>as values</b>. It encapsulates the topic, "score," and the document.    
 * 
 * @author emeij
 *
 */
public class ResultObject implements Writable {

  private static final TSerializer serializer = new TSerializer(
      new TBinaryProtocol.Factory());
  private static final TDeserializer deserializer = new TDeserializer(
      new TBinaryProtocol.Factory());

  public StreamItemWritable document = new StreamItemWritable();
  public Long score = null;
  public String topic = null;
  public String stream_id = null;// for convenience

  public ResultObject() {

  }

  public ResultObject(StreamItemWritable document, Long score, String topic,
      String stream_id) {
    super();
    this.document = document;
    this.score = score;
    this.topic = topic;
    this.stream_id = stream_id;
  }

  public StreamItemWritable getDocument() {
    return document;
  }

  public void setDocument(StreamItemWritable document) {
    this.document = document;
  }

  public Long getScore() {
    return score;
  }

  public void setScore(Long score) {
    this.score = score;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getStream_id() {
    return stream_id;
  }

  public void setStream_id(String stream_id) {
    this.stream_id = stream_id;
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    try {

      score = in.readLong();
      topic = in.readUTF();
      stream_id = in.readUTF();

      int length = WritableUtils.readVInt(in);
      byte[] bytes = new byte[length];
      in.readFully(bytes, 0, length);

      deserializer.deserialize(document, bytes);

    } catch (TException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {

    try {

      out.writeLong(score);
      out.writeUTF(topic);
      out.writeUTF(stream_id);

      byte[] bytes = serializer.serialize(document);
      WritableUtils.writeVInt(out, bytes.length);
      out.write(bytes, 0, bytes.length);

    } catch (TException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  @Override
  public String toString() {
    return Long.toString(score) + ", " + topic + ", " + document.toString();
  }

}
