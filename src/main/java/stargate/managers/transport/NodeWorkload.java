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
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class NodeWorkload {
    private String nodeName;
    private double workload;
    
    public static NodeWorkload createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        return (NodeWorkload) JsonSerializer.fromJsonFile(file, NodeWorkload.class);
    }
    
    public static NodeWorkload createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (NodeWorkload) JsonSerializer.fromJson(json, NodeWorkload.class);
    }
    
    NodeWorkload() {
    }
    
    public NodeWorkload(String nodeName) {
        if(nodeName == null || nodeName.isEmpty()) {
            throw new IllegalArgumentException("nodeName is null or empty");
        }
        
        this.nodeName = nodeName;
        this.workload = 0;
    }
    
    public NodeWorkload(String nodeName, double workload) {
        if(nodeName == null || nodeName.isEmpty()) {
            throw new IllegalArgumentException("nodeName is null or empty");
        }
        
        this.nodeName = nodeName;
        this.workload = workload;
    }
    
    @JsonProperty("node_name")
    public String getNodeName() {
        return this.nodeName;
    }
    
    @JsonProperty("node_name")
    public void setNodeName(String nodeName) {
        if(nodeName == null || nodeName.isEmpty()) {
            throw new IllegalArgumentException("nodeName is null or empty");
        }
        
        this.nodeName = nodeName;
    }
    
    @JsonProperty("workload")
    public double getWorkload() {
        return this.workload;
    }
    
    @JsonProperty("workload")
    public void setWorkload(double workload) {
        this.workload = workload;
    }
    
    @JsonIgnore
    public synchronized void increaseWorkload(double delta) {
        this.workload += delta;
    }
    
    @JsonIgnore
    public void decreaseScheduledTransfers(double delta) {
        this.workload -= delta;
        if(this.workload < 0) {
            this.workload = 0;
        }
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
