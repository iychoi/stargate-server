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
package stargate.managers.transport.layout;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class TransferWorkloadEvent {
    private TransferWorkloadEventType eventType;
    private ClusterWorkload clusterWorkload;
    
    public static TransferWorkloadEvent createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (TransferWorkloadEvent) JsonSerializer.fromJsonFile(file, TransferWorkloadEvent.class);
    }
    
    public static TransferWorkloadEvent createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (TransferWorkloadEvent) JsonSerializer.fromJson(json, TransferWorkloadEvent.class);
    }
    
    TransferWorkloadEvent() {
    }
    
    public TransferWorkloadEvent(TransferWorkloadEventType eventType, ClusterWorkload workload) {
        if(eventType == null) {
            throw new IllegalArgumentException("eventType is null");
        }
        
        if(workload == null) {
            throw new IllegalArgumentException("workload is null");
        }
        
        this.eventType = eventType;
        this.clusterWorkload = workload;
    }
    
    @JsonProperty("event_type")
    public TransferWorkloadEventType getEventType() {
        return this.eventType;
    }
    
    @JsonProperty("event_type")
    public void setEventType(TransferWorkloadEventType eventType) {
        if(eventType == null) {
            throw new IllegalArgumentException("eventType is null");
        }
        
        this.eventType = eventType;
    }
    
    @JsonProperty("workload")
    public ClusterWorkload getWorkload() {
        return this.clusterWorkload;
    }
    
    @JsonProperty("workload")
    public void setWorkload(ClusterWorkload workload) {
        if(workload == null) {
            throw new IllegalArgumentException("workload is null");
        }
        
        this.clusterWorkload = workload;
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
