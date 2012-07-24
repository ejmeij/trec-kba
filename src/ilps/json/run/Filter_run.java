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
package ilps.json.run;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Generated;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Describes a run of a KBA system and is first line of a text file containing one result per line.  For example, see http://trec-kba.org/tasks/ccr-2012.html
 * 
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({ "$schema", "task_id", "topic_set_id", "corpus_id",
    "team_id", "system_id", "run_type", "system_description" })
@SuppressWarnings("static-access")
public class Filter_run {

  /**
   * Factory
   */
  public static class Factory {

    private static final ObjectMapper mapper = new ObjectMapper();

    public Factory() {

    }

    /**
     * Creates a new Filter_run object, with most default values already initialized.
     * 
     * @return
     */
    public Filter_run create() {

      Filter_run fr = new Filter_run();
      fr.set_schema(Filter_run._schema);
      fr.setTask_id("kba-ccr-2012");
      fr.setRun_type(Run_type.AUTOMATIC);

      return fr;
    }

    /**
     * Creates a new Filter_run object, with most default values already initialized.
     * 
     * @param team The team name
     * @param system The run ID
     * @param desc A description
     * @return
     */
    public Filter_run create(String team, String system, String desc,
        String corpus_id) {

      Filter_run fr = create();

      fr.setTeam_id(team);
      fr.setSystem_id(system);
      fr.setSystem_description(desc);
      fr.setCorpus_id(corpus_id);

      return fr;
    }

    public void serializeRun(Filter_run fr, File f) {
      try {
        mapper.writeValue(f, fr);
      } catch (JsonGenerationException e) {
        e.printStackTrace();
      } catch (JsonMappingException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public String toPrettyJSON(Filter_run fr) {
      try {
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(fr);
      } catch (JsonGenerationException e) {
        e.printStackTrace();
      } catch (JsonMappingException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    public String toJSON(Filter_run fr) {
      try {
        return mapper.writeValueAsString(fr);
      } catch (JsonGenerationException e) {
        e.printStackTrace();
      } catch (JsonMappingException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    public Filter_run loadRun(String inputfilelocation) {
      Filter_run fr = null;
      try {
        fr = mapper.readValue(new File(inputfilelocation), Filter_run.class);
      } catch (JsonParseException e) {
        e.printStackTrace();
      } catch (JsonMappingException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

      return fr;
    }
  }

  /**
   * URI of this JSON schema document.
   * 
   */
  @JsonProperty("$schema")
  private static Filter_run._schema _schema = Filter_run._schema
      .fromValue("http://trec-kba.org/schemas/v1.0/filter-run.json");
  /**
   * Unique string that identifies the task your system is performing
   * (Required)
   * 
   */
  @JsonProperty("task_id")
  private String task_id;
  /**
   * Unique string that identifies the topics for which your system is filtering
   * (Required)
   * 
   */
  @JsonProperty("topic_set_id")
  private String topic_set_id;
  /**
   * Unique string that identifies the corpus on which your system operated
   * (Required)
   * 
   */
  @JsonProperty("corpus_id")
  private String corpus_id;
  /**
   * Unique string that your team selected for identifying itself to TREC
   * (Required)
   * 
   */
  @JsonProperty("team_id")
  private String team_id;
  /**
   * Unique string that you select for distinguishing this system-&-configuration from other runs you submit
   * (Required)
   * 
   */
  @JsonProperty("system_id")
  private String system_id;
  /**
   * Flags for categorizing runs.
   * (Required)
   * 
   */
  @JsonProperty("run_type")
  private Filter_run.Run_type run_type;
  /**
   * Human readable description of this system.  Please be verbose.
   * (Required)
   * 
   */
  @JsonProperty("system_description")
  private String system_description;
  private Map<String, Object> additionalProperties = new HashMap<String, Object>();

  /**
   * URI of this JSON schema document.
   * 
   */
  @JsonProperty("$schema")
  public Filter_run._schema get_schema() {
    return _schema;
  }

  /**
   * URI of this JSON schema document.
   * 
   */
  @JsonProperty("$schema")
  public void set_schema(Filter_run._schema _schema) {
    this._schema = _schema;
  }

  /**
   * Unique string that identifies the task your system is performing
   * (Required)
   * 
   */
  @JsonProperty("task_id")
  public String getTask_id() {
    return task_id;
  }

  /**
   * Unique string that identifies the task your system is performing
   * (Required)
   * 
   */
  @JsonProperty("task_id")
  public void setTask_id(String task_id) {
    this.task_id = task_id;
  }

  /**
   * Unique string that identifies the topics for which your system is filtering
   * (Required)
   * 
   */
  @JsonProperty("topic_set_id")
  public String getTopic_set_id() {
    return topic_set_id;
  }

  /**
   * Unique string that identifies the topics for which your system is filtering
   * (Required)
   * 
   */
  @JsonProperty("topic_set_id")
  public void setTopic_set_id(String topic_set_id) {
    this.topic_set_id = topic_set_id;
  }

  /**
   * Unique string that identifies the corpus on which your system operated
   * (Required)
   * 
   */
  @JsonProperty("corpus_id")
  public String getCorpus_id() {
    return corpus_id;
  }

  /**
   * Unique string that identifies the corpus on which your system operated
   * (Required)
   * 
   */
  @JsonProperty("corpus_id")
  public void setCorpus_id(String corpus_id) {
    this.corpus_id = corpus_id;
  }

  /**
   * Unique string that your team selected for identifying itself to TREC
   * (Required)
   * 
   */
  @JsonProperty("team_id")
  public String getTeam_id() {
    return team_id;
  }

  /**
   * Unique string that your team selected for identifying itself to TREC
   * (Required)
   * 
   */
  @JsonProperty("team_id")
  public void setTeam_id(String team_id) {
    this.team_id = team_id;
  }

  /**
   * Unique string that you select for distinguishing this system-&-configuration from other runs you submit
   * (Required)
   * 
   */
  @JsonProperty("system_id")
  public String getSystem_id() {
    return system_id;
  }

  /**
   * Unique string that you select for distinguishing this system-&-configuration from other runs you submit
   * (Required)
   * 
   */
  @JsonProperty("system_id")
  public void setSystem_id(String system_id) {
    this.system_id = system_id;
  }

  /**
   * Flags for categorizing runs.
   * (Required)
   * 
   */
  @JsonProperty("run_type")
  public Filter_run.Run_type getRun_type() {
    return run_type;
  }

  /**
   * Flags for categorizing runs.
   * (Required)
   * 
   */
  @JsonProperty("run_type")
  public void setRun_type(Filter_run.Run_type run_type) {
    this.run_type = run_type;
  }

  /**
   * Human readable description of this system.  Please be verbose.
   * (Required)
   * 
   */
  @JsonProperty("system_description")
  public String getSystem_description() {
    return system_description;
  }

  /**
   * Human readable description of this system.  Please be verbose.
   * (Required)
   * 
   */
  @JsonProperty("system_description")
  public void setSystem_description(String system_description) {
    this.system_description = system_description;
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

  @Generated("com.googlecode.jsonschema2pojo")
  public static enum Run_type {

    MANUAL("manual"), AUTOMATIC("automatic"), OTHER("other");
    private final String value;

    private Run_type(String value) {
      this.value = value;
    }

    @JsonValue
    @Override
    public String toString() {
      return this.value;
    }

    @JsonCreator
    public static Filter_run.Run_type fromValue(String value) {
      for (Filter_run.Run_type c : Filter_run.Run_type.values()) {
        if (c.value.equals(value)) {
          return c;
        }
      }
      throw new IllegalArgumentException(value);
    }

  }

  @Generated("com.googlecode.jsonschema2pojo")
  public static enum _schema {

    HTTP_TREC_KBA_ORG_SCHEMAS_V_1_0_FILTER_RUN_JSON(
        "http://trec-kba.org/schemas/v1.0/filter-run.json");
    private final String value;

    private _schema(String value) {
      this.value = value;
    }

    @JsonValue
    @Override
    public String toString() {
      return this.value;
    }

    @JsonCreator
    public static Filter_run._schema fromValue(String value) {
      for (Filter_run._schema c : Filter_run._schema.values()) {
        if (c.value.equals(value)) {
          return c;
        }
      }
      throw new IllegalArgumentException(value);
    }

  }

}
