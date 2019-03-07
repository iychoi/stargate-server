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
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class ResponsibleNodeMapping {
    
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
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }
        
        this.clusterName = clusterName;
    }

    @JsonProperty("node_mappings")
    public Collection<NodeMapping> getNodeMappings() {
        return Collections.unmodifiableCollection(this.forwardMappings.values());
    }
    
    @JsonIgnore
    public NodeMapping getNodeMapping(String localNodeName) {
        if(localNodeName == null || localNodeName.isEmpty()) {
            throw new IllegalArgumentException("localNodeName is null or empty");
        }
        
        return this.forwardMappings.get(localNodeName);
    }
    
    @JsonProperty("node_mappings")
    public void addNodeMappings(Collection<NodeMapping> mappings) {
        if(mappings == null) {
            throw new IllegalArgumentException("mappings is null");
        }
        
        for(NodeMapping mapping : mappings) {
            addNodeMapping(mapping);
        }
    }
    
    @JsonIgnore
    public void addNodeMapping(NodeMapping mapping) {
        if(mapping == null) {
            throw new IllegalArgumentException("mapping is null");
        }
        
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
        if(sourcecNodeName == null || sourcecNodeName.isEmpty()) {
            throw new IllegalArgumentException("sourcecNodeName is null or empty");
        }
        
        if(targetNodeName == null || targetNodeName.isEmpty()) {
            throw new IllegalArgumentException("targetNodeName is null or empty");
        }
        
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
    public boolean removeNodeMapping(String sourcecNodeName) {
        if(sourcecNodeName == null || sourcecNodeName.isEmpty()) {
            throw new IllegalArgumentException("sourcecNodeName is null or empty");
        }
        
        NodeMapping mapping = this.forwardMappings.remove(sourcecNodeName);
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
    @JsonIgnore
    public int hashCode() {
        int hash = 7;
        hash = 29 * hash + Objects.hashCode(this.clusterName);
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
        final ResponsibleNodeMapping other = (ResponsibleNodeMapping) obj;
        if (!Objects.equals(this.clusterName, other.clusterName)) {
            return false;
        }
        return true;
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
