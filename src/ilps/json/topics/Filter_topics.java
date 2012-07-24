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
package ilps.json.topics;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Generated;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Describes the topic entities for a KBA filtering task.
 * 
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({ "$schema", "topic_set_id", "topic_names", "kb" })
public class Filter_topics {

  /**
   * Factory
   */
  public static class Factory {

    private static final ObjectMapper mapper = new ObjectMapper();

    public Factory() {

    }

    public Filter_topics loadTopics(String inputfilelocation) {
      Filter_topics ft = null;
      try {
        ft = mapper.readValue(new File(inputfilelocation), Filter_topics.class);
      } catch (JsonParseException e) {
        e.printStackTrace();
      } catch (JsonMappingException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

      return ft;
    }

    public Filter_topics loadTopics(BufferedReader br) {
      Filter_topics ft = null;
      try {
        ft = mapper.readValue(br, Filter_topics.class);
      } catch (JsonParseException e) {
        e.printStackTrace();
      } catch (JsonMappingException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

      return ft;
    }

    public Filter_topics loadTopics(InputStream is) {
      Filter_topics ft = null;
      try {
        ft = mapper.readValue(is, Filter_topics.class);
      } catch (JsonParseException e) {
        e.printStackTrace();
      } catch (JsonMappingException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

      return ft;
    }
  }

  /**
   * Unique string that identifies this set of topic entities
   * (Required)
   * 
   */
  @JsonProperty("topic_set_id")
  private String topic_set_id;
  /**
   * Array of strings that uniquely identify the topics within 'kb'.
   * (Required)
   * 
   */
  @JsonProperty("topic_names")
  private List<String> topic_names = new ArrayList<String>();
  /**
   * Object describing the knowledge base containing the topic entities
   * (Required)
   * 
   */
  @JsonProperty("kb")
  private Kb kb;
  private Map<String, Object> additionalProperties = new HashMap<String, Object>();

  /**
   * Unique string that identifies this set of topic entities
   * (Required)
   * 
   */
  @JsonProperty("topic_set_id")
  public String getTopic_set_id() {
    return topic_set_id;
  }

  /**
   * Unique string that identifies this set of topic entities
   * (Required)
   * 
   */
  @JsonProperty("topic_set_id")
  public void setTopic_set_id(String topic_set_id) {
    this.topic_set_id = topic_set_id;
  }

  /**
   * Array of strings that uniquely identify the topics within 'kb'.
   * (Required)
   * 
   */
  @JsonProperty("topic_names")
  public List<String> getTopic_names() {
    return topic_names;
  }

  /**
   * Array of strings that uniquely identify the topics within 'kb'.
   * (Required)
   * 
   */
  @JsonProperty("topic_names")
  public void setTopic_names(List<String> topic_names) {
    this.topic_names = topic_names;
  }

  /**
   * Object describing the knowledge base containing the topic entities
   * (Required)
   * 
   */
  @JsonProperty("kb")
  public Kb getKb() {
    return kb;
  }

  /**
   * Object describing the knowledge base containing the topic entities
   * (Required)
   * 
   */
  @JsonProperty("kb")
  public void setKb(Kb kb) {
    this.kb = kb;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public boolean equals(Object other) {
    return EqualsBuilder.reflectionEquals(this, other);
  }

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }

  @JsonAnySetter
  public void setAdditionalProperties(String name, Object value) {
    this.additionalProperties.put(name, value);
  }

}
