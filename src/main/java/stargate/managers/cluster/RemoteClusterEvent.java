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
package stargate.managers.cluster;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.cluster.Cluster;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class RemoteClusterEvent {
    private RemoteClusterEventType eventType;
    private Cluster cluster;
    
    public static RemoteClusterEvent createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (RemoteClusterEvent) JsonSerializer.fromJsonFile(file, RemoteClusterEvent.class);
    }
    
    public static RemoteClusterEvent createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (RemoteClusterEvent) JsonSerializer.fromJson(json, RemoteClusterEvent.class);
    }
    
    RemoteClusterEvent() {
    }
    
    public RemoteClusterEvent(RemoteClusterEventType eventType, Cluster cluster) {
        if(eventType == null) {
            throw new IllegalArgumentException("eventType is null");
        }
        
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        this.eventType = eventType;
        this.cluster = cluster;
    }
    
    @JsonProperty("event_type")
    public RemoteClusterEventType getEventType() {
        return this.eventType;
    }
    
    @JsonProperty("event_type")
    public void setEventType(RemoteClusterEventType eventType) {
        if(eventType == null) {
            throw new IllegalArgumentException("eventType is null");
        }
        
        this.eventType = eventType;
    }
    
    @JsonProperty("cluster")
    public Cluster getCluster() {
        return this.cluster;
    }
    
    @JsonProperty("cluster")
    public void setCluster(Cluster cluster) {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        this.cluster = cluster;
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
