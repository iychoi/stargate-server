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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class NodeMapping {

    private String sourceNodeName;
    private Set<String> targetNodeNames = new HashSet<String>();
    
    public static NodeMapping createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (NodeMapping) JsonSerializer.fromJsonFile(file, NodeMapping.class);
    }
    
    public static NodeMapping createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (NodeMapping) JsonSerializer.fromJson(json, NodeMapping.class);
    }
    
    NodeMapping() {
    }
    
    public NodeMapping(String sourceNodeName, Collection<String> targetNodeNames) {
        if(sourceNodeName == null || sourceNodeName.isEmpty()) {
            throw new IllegalArgumentException("sourceNodeName is null or empty");
        }
        
        if(targetNodeNames == null || targetNodeNames.isEmpty()) {
            throw new IllegalArgumentException("targetNodeNames is null or empty");
        }
        
        this.sourceNodeName = sourceNodeName;
        this.targetNodeNames.addAll(targetNodeNames);
    }
    
    public NodeMapping(String sourceNodeName, String targetNodeName) {
        if(sourceNodeName == null || sourceNodeName.isEmpty()) {
            throw new IllegalArgumentException("sourceNodeName is null or empty");
        }
        
        if(targetNodeName == null || targetNodeName.isEmpty()) {
            throw new IllegalArgumentException("targetNodeName is null or empty");
        }
        
        this.sourceNodeName = sourceNodeName;
        this.targetNodeNames.add(targetNodeName);
    }
    
    @JsonProperty("source_node_name")
    public String getSourceNodeName() {
        return this.sourceNodeName;
    }
    
    @JsonProperty("source_node_name")
    public void setSourceNodeName(String sourceNodeName) {
        this.sourceNodeName = sourceNodeName;
    }
    
    @JsonProperty("target_node_names")
    public Collection<String> getTargetNodeNames() {
        return Collections.unmodifiableCollection(this.targetNodeNames);
    }
    
    @JsonProperty("target_node_names")
    public void addTargetNodeNames(Collection<String> targetNodeNames) {
        for(String targetNodeName : targetNodeNames) {
            addTargetNodeName(targetNodeName);
        }
    }
    
    @JsonIgnore
    public void addTargetNodeName(String targetNodeName) {
        if(!this.targetNodeNames.contains(targetNodeName)) {
            this.targetNodeNames.add(targetNodeName);
        }
    }
    
    @Override
    @JsonIgnore
    public int hashCode() {
        int hash = 5;
        hash = 17 * hash + Objects.hashCode(this.sourceNodeName);
        for(String targetNodeName : this.targetNodeNames) {
            hash = 17 * hash + Objects.hashCode(targetNodeName);
        }
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
        final NodeMapping other = (NodeMapping) obj;
        if (!Objects.equals(this.sourceNodeName, other.sourceNodeName)) {
            return false;
        }
        if (!Objects.equals(this.targetNodeNames, other.targetNodeNames)) {
            return false;
        }
        return true;
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        return "NodeMapping{" + "sourceNodeName=" + sourceNodeName + '}';
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
