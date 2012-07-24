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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;


/**
 * Object describing the knowledge base containing the topic entities
 * 
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "name",
    "URL",
    "snapshot_time",
    "description"
})
public class Kb {

    /**
     * Unique name of the KB
     * (Required)
     * 
     */
    @JsonProperty("name")
    private String name;
    /**
     * URL to front door of KB
     * 
     */
    @JsonProperty("URL")
    private String URL;
    /**
     * Date of a snapshot of the KB
     * 
     */
    @JsonProperty("snapshot_time")
    private Snapshot_time snapshot_time;
    /**
     * Human readable description of the KB, possibly including instructions for querying kb with strings from topic_names
     * (Required)
     * 
     */
    @JsonProperty("description")
    private String description;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * Unique name of the KB
     * (Required)
     * 
     */
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    /**
     * Unique name of the KB
     * (Required)
     * 
     */
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    /**
     * URL to front door of KB
     * 
     */
    @JsonProperty("URL")
    public String getURL() {
        return URL;
    }

    /**
     * URL to front door of KB
     * 
     */
    @JsonProperty("URL")
    public void setURL(String URL) {
        this.URL = URL;
    }

    /**
     * Date of a snapshot of the KB
     * 
     */
    @JsonProperty("snapshot_time")
    public Snapshot_time getSnapshot_time() {
        return snapshot_time;
    }

    /**
     * Date of a snapshot of the KB
     * 
     */
    @JsonProperty("snapshot_time")
    public void setSnapshot_time(Snapshot_time snapshot_time) {
        this.snapshot_time = snapshot_time;
    }

    /**
     * Human readable description of the KB, possibly including instructions for querying kb with strings from topic_names
     * (Required)
     * 
     */
    @JsonProperty("description")
    public String getDescription() {
        return description;
    }

    /**
     * Human readable description of the KB, possibly including instructions for querying kb with strings from topic_names
     * (Required)
     * 
     */
    @JsonProperty("description")
    public void setDescription(String description) {
        this.description = description;
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
