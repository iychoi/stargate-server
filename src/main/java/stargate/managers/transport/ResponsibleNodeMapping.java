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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class ResponsibleNodeMapping {
    
    private static final Log LOG = LogFactory.getLog(ResponsibleNodeMapping.class);
    
    private String clusterName;
    private Map<String, NodeMapping> forwardMappings = new HashMap<String, NodeMapping>(); // forward, local to remote
    
    public static ResponsibleNodeMapping createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (ResponsibleNodeMapping) JsonSerializer.fromJsonFile(file, ResponsibleNodeMapping.class);
    }
    
    public static ResponsibleNodeMapping createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (ResponsibleNodeMapping) JsonSerializer.fromJson(json, ResponsibleNodeMapping.class);
    }
    
    ResponsibleNodeMapping() {
    }
    
    public ResponsibleNodeMapping(String clusterName) {
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
        this.clusterName = clusterName;
    }
    
    @JsonProperty("node_mappings")
    public Collection<NodeMapping> getNodeMappings() {
        return Collections.unmodifiableCollection(this.forwardMappings.values());
    }
    
    @JsonIgnore
    public NodeMapping getNodeMapping(String localNodeName) {
        return this.forwardMappings.get(localNodeName);
    }
    
    @JsonProperty("node_mappings")
    public void addNodeMappings(Collection<NodeMapping> mappings) {
        for(NodeMapping mapping : mappings) {
            addNodeMapping(mapping);
        }
    }
    
    @JsonIgnore
    public void addNodeMapping(NodeMapping mapping) {
        NodeMapping existingForwardMapping = this.forwardMappings.get(mapping.getSourceNodeName());
        if(existingForwardMapping != null) {
            // add
            existingForwardMapping.addTargetNodeNames(mapping.getTargetNodeNames());
        } else {
            // create a new
            this.forwardMappings.put(mapping.getSourceNodeName(), mapping);
        }
    }
    
    @JsonIgnore
    public void addNodeMapping(String sourcecNodeName, String targetNodeName) {
        NodeMapping existingForwardMapping = this.forwardMappings.get(sourcecNodeName);
        if(existingForwardMapping != null) {
            // add
            existingForwardMapping.addTargetNodeName(targetNodeName);
        } else {
            // create a new
            NodeMapping mapping = new NodeMapping(sourcecNodeName, targetNodeName);
            this.forwardMappings.put(sourcecNodeName, mapping);
        }
    }
    
    @JsonIgnore
    public boolean removeNodeMapping(String sourceNameName) {
        NodeMapping mapping = this.forwardMappings.remove(sourceNameName);
        if(mapping != null) {
            return true;
        }
        return false;
    }
    
    @JsonIgnore
    public void clearNodeMappings() {
        this.forwardMappings.clear();
    }
    
    @Override
    public String toString() {
        return this.clusterName;
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
