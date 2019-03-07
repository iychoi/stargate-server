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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class ClusterWorkload {
    private String clusterName;
    private Map<String, NodeWorkload> nodeWorkloads = new HashMap<String, NodeWorkload>();
    private long lastModifiedTime;
    
    public static ClusterWorkload createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        return (ClusterWorkload) JsonSerializer.fromJsonFile(file, ClusterWorkload.class);
    }
    
    public static ClusterWorkload createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (ClusterWorkload) JsonSerializer.fromJson(json, ClusterWorkload.class);
    }
    
    ClusterWorkload() {
    }
    
    public ClusterWorkload(String clusterName) {
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }
        
        this.clusterName = clusterName;
    }
    
    @JsonProperty("cluster_name")
    public String getClusterName() {
        return this.clusterName;
    }
    
    @JsonProperty("cluster_name")
    public void setClusterName(String clusterName) {
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }
        
        this.clusterName = clusterName;
    }
    
    @JsonProperty("node_workload")
    public Map<String, NodeWorkload> getNodeWorkloads() {
        return Collections.unmodifiableMap(this.nodeWorkloads);
    }
    
    @JsonProperty("node_workload")
    public void setNodeWorkloads(Map<String, NodeWorkload> nodeWorkloads) {
        if(nodeWorkloads == null) {
            throw new IllegalArgumentException("nodeWorkloads is null");
        }
        
        this.nodeWorkloads.putAll(nodeWorkloads);
        
        this.lastModifiedTime = DateTimeUtils.getTimestamp();
    }
    
    @JsonIgnore
    public Collection<String> getNodeNamesWithValue() {
        return this.nodeWorkloads.keySet();
    }
    
    @JsonIgnore
    public NodeWorkload getNodeWorkloadWithDefault(String nodeName) {
        if(nodeName == null || nodeName.isEmpty()) {
            throw new IllegalArgumentException("nodeName is null or empty");
        }
        
        NodeWorkload nodeWorkload = this.nodeWorkloads.get(nodeName);
        if(nodeWorkload == null) {
            nodeWorkload = new NodeWorkload(nodeName);
            this.nodeWorkloads.put(nodeName, nodeWorkload);
        }
        return nodeWorkload;
    }
    
    @JsonIgnore
    public NodeWorkload getNodeWorkload(String nodeName) {
        if(nodeName == null || nodeName.isEmpty()) {
            throw new IllegalArgumentException("nodeName is null or empty");
        }
        
        return this.nodeWorkloads.get(nodeName);
    }
    
    @JsonIgnore
    public void setNodeWorkload(NodeWorkload nodeWorkload) {
        if(nodeWorkload == null) {
            throw new IllegalArgumentException("nodeWorkload is null");
        }
        
        this.nodeWorkloads.put(nodeWorkload.getNodeName(), nodeWorkload);
        
        this.lastModifiedTime = DateTimeUtils.getTimestamp();
    }
    
    @JsonIgnore
    public void increaseNodeWorkload(NodeWorkload nodeWorkload) {
        if(nodeWorkload == null) {
            throw new IllegalArgumentException("nodeWorkload is null");
        }
        
        NodeWorkload existingNodeWorkload = this.nodeWorkloads.get(nodeWorkload.getNodeName());
        if(existingNodeWorkload == null) {
            this.nodeWorkloads.put(nodeWorkload.getNodeName(), nodeWorkload);
        } else {
            existingNodeWorkload.increaseWorkload(nodeWorkload.getWorkload());
        }
        
        this.lastModifiedTime = DateTimeUtils.getTimestamp();
    }
    
    @JsonIgnore
    public void increaseClusterWorkload(ClusterWorkload workload) {
        if(workload == null) {
            throw new IllegalArgumentException("workload is null");
        }
        
        Collection<NodeWorkload> values = workload.nodeWorkloads.values();
        for(NodeWorkload value : values) {
            NodeWorkload nodeWorkload = this.nodeWorkloads.get(value.getNodeName());
            if(nodeWorkload != null) {
                nodeWorkload.increaseWorkload(value.getWorkload());
            } else {
                this.nodeWorkloads.put(value.getNodeName(), value);
            }
        }
        
        if(this.lastModifiedTime < workload.lastModifiedTime) {
            this.lastModifiedTime = workload.lastModifiedTime;
        }
    }
    
    @JsonIgnore
    public void clearNodeWorkload(String nodeName) {
        if(nodeName == null || nodeName.isEmpty()) {
            throw new IllegalArgumentException("nodeName is null or empty");
        }
        
        this.nodeWorkloads.remove(nodeName);
        
        this.lastModifiedTime = DateTimeUtils.getTimestamp();
    }
    
    @JsonProperty("last_modified_time")
    public long getLastModifiedTime() {
        return this.lastModifiedTime;
    }
    
    @JsonProperty("last_modified_time")
    public void setLastModifiedTime(long lastModifiedTime) {
        if(lastModifiedTime < 0) {
            throw new IllegalArgumentException("lastModifiedTime is negative");
        }
        
        this.lastModifiedTime = lastModifiedTime;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        Collection<NodeWorkload> values = this.nodeWorkloads.values();
        for(NodeWorkload workload : values) {
            if(sb.length() != 0) {
                sb.append(", ");
            }
            sb.append(String.format("%s:%f", workload.getNodeName(), workload.getWorkload()));
        }
        
        return String.format("%s : [%s]", this.clusterName, sb.toString());
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
