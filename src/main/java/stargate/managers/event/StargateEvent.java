/*
   Copyright 2018 The Trustees of University of Arizona

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package stargate.managers.event;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class StargateEvent {
    
    private StargateEventType eventType;
    private Object value;
    
    public static StargateEvent createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (StargateEvent) JsonSerializer.fromJsonFile(file, StargateEvent.class);
    }
    
    public static StargateEvent createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (StargateEvent) JsonSerializer.fromJson(json, StargateEvent.class);
    }
    
    StargateEvent() {
    }
    
    public StargateEvent(StargateEventType eventType, Object value) {
        if(eventType == null) {
            throw new IllegalArgumentException("eventType is null");
        }
        
        if(value == null) {
            throw new IllegalArgumentException("value is null");
        }
        
        this.eventType = eventType;
        this.value = value;
    }
    
    @JsonProperty("event_type")
    public StargateEventType getEventType() {
        return this.eventType;
    }
    
    @JsonProperty("event_type")
    public void setEventType(StargateEventType eventType) {
        if(eventType == null) {
            throw new IllegalArgumentException("eventType is null");
        }
        
        this.eventType = eventType;
    }
    
    @JsonProperty("value")
    public Object getValue() {
        return this.value;
    }
    
    @JsonProperty("value")
    public void setValue(Object value) {
        if(value == null) {
            throw new IllegalArgumentException("value is null");
        }
        
        this.value = value;
    }
    
    @JsonIgnore
    public String toJson() throws IOException {
        return JsonSerializer.toJson(this);
    }
    
    @JsonIgnore
    public void saveTo(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer.toJsonFile(file, this);
    }
}
