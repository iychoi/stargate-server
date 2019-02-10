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
package stargate.managers.transport;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class TransferAssignment {
    private TransferEventType eventType;
    private TransferEvent event;
    private long queuedTime;
    private long order;
    
    public static TransferAssignment createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (TransferAssignment) JsonSerializer.fromJsonFile(file, TransferAssignment.class);
    }
    
    public static TransferAssignment createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (TransferAssignment) JsonSerializer.fromJson(json, TransferAssignment.class);
    }
    
    TransferAssignment() {
    }
    
    public TransferAssignment(TransferEventType eventType, TransferEvent event, long queuedTime, long order) {
        if(eventType == null) {
            throw new IllegalArgumentException("eventType is null");
        }
        
        if(event == null) {
            throw new IllegalArgumentException("event is null");
        }
        
        if(queuedTime < 0) {
            throw new IllegalArgumentException("queuedTime is negative");
        }
        
        if(order < 0) {
            throw new IllegalArgumentException("order is negative");
        }
        
        this.eventType = eventType;
        this.event = event;
        this.queuedTime = queuedTime;
        this.order = order;
    }
    
    @JsonProperty("event_type")
    public TransferEventType getEventType() {
        return this.eventType;
    }
    
    @JsonProperty("event_type")
    public void setEventType(TransferEventType eventType) {
        this.eventType = eventType;
    }
    
    @JsonProperty("event")
    public TransferEvent getEvent() {
        return this.event;
    }
    
    @JsonProperty("event")
    public void setEvent(TransferEvent event) {
        this.event = event;
    }
    
    @Override
    @JsonIgnore
    public int hashCode() {
        int hash = 5;
        hash = 17 * hash + Objects.hashCode(this.event);
        hash = 17 * hash + Objects.hashCode(this.queuedTime);
        hash = 17 * hash + Objects.hashCode(this.order);
        return hash;
    }

    @Override
    @JsonIgnore
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TransferAssignment other = (TransferAssignment) obj;
        if (!Objects.equals(this.event, other.event)) {
            return false;
        }
        if (this.queuedTime != other.queuedTime) {
            return false;
        }
        if (this.order != other.order) {
            return false;
        }
        return true;
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        return "TransferAssignment{" + "eventType=" + eventType + ", event=" + event + ", queuedTime=" + queuedTime + ", order=" + order + '}';
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
