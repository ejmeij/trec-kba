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
 * Date of a snapshot of the KB
 * 
 */
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Generated("com.googlecode.jsonschema2pojo")
@JsonPropertyOrder({
    "epoch_ticks",
    "zulu_timestamp"
})
public class Snapshot_time {

    /**
     * Seconds (and milliseconds) since the epoch January 1, 1970.
     * 
     */
    @JsonProperty("epoch_ticks")
    private Object epoch_ticks;
    /**
     * Calculated from epoch_ticks in format %Y-%m-%dT%H:%M:%S.%fZ
     * 
     */
    @JsonProperty("zulu_timestamp")
    private String zulu_timestamp;
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * Seconds (and milliseconds) since the epoch January 1, 1970.
     * 
     */
    @JsonProperty("epoch_ticks")
    public Object getEpoch_ticks() {
        return epoch_ticks;
    }

    /**
     * Seconds (and milliseconds) since the epoch January 1, 1970.
     * 
     */
    @JsonProperty("epoch_ticks")
    public void setEpoch_ticks(Object epoch_ticks) {
        this.epoch_ticks = epoch_ticks;
    }

    /**
     * Calculated from epoch_ticks in format %Y-%m-%dT%H:%M:%S.%fZ
     * 
     */
    @JsonProperty("zulu_timestamp")
    public String getZulu_timestamp() {
        return zulu_timestamp;
    }

    /**
     * Calculated from epoch_ticks in format %Y-%m-%dT%H:%M:%S.%fZ
     * 
     */
    @JsonProperty("zulu_timestamp")
    public void setZulu_timestamp(String zulu_timestamp) {
        this.zulu_timestamp = zulu_timestamp;
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
