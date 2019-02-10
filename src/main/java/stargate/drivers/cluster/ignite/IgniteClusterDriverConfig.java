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
package stargate.drivers.cluster.ignite;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.cluster.AbstractClusterDriverConfig;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.utils.ResourceUtils;

/**
 *
 * @author iychoi
 */
public class IgniteClusterDriverConfig extends AbstractClusterDriverConfig {
    
    public static final String DEFAULT_CLUSTER_NAME = "StargateCluster";
    
    private String clusterName = DEFAULT_CLUSTER_NAME;
    private File storageRootPath;
    private List<String> clusterNodes = new ArrayList<String>();
    
    public static IgniteClusterDriverConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (IgniteClusterDriverConfig) JsonSerializer.fromJsonFile(file, IgniteClusterDriverConfig.class);
    }
    
    public static IgniteClusterDriverConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (IgniteClusterDriverConfig) JsonSerializer.fromJson(json, IgniteClusterDriverConfig.class);
    }
    
    public IgniteClusterDriverConfig() {
        this.storageRootPath = new File(ResourceUtils.getStargateRoot(), "storage");
    }
    
    @JsonProperty("cluster_name")
    public void setClusterName(String clusterName) {
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.clusterName = clusterName;
    }
    
    @JsonProperty("cluster_name")
    public String getClusterName() {
        return this.clusterName;
    }
    
    @JsonProperty("storage_root_path")
    public void setStorageRootPath(String storageRootPath) {
        if(storageRootPath == null || storageRootPath.isEmpty()) {
            throw new IllegalArgumentException("storageRootPath is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.storageRootPath = new File(storageRootPath);
    }
    
    @JsonIgnore
    public void setStorageRootPath(File storageRootPath) {
        if(storageRootPath == null) {
            throw new IllegalArgumentException("storageRootPath is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.storageRootPath = storageRootPath;
    }
    
    @JsonProperty("storage_root_path")
    public String getStorageRootPathString() {
        return this.storageRootPath.getAbsolutePath();
    }
    
    @JsonIgnore
    public File getStorageRootPath() {
        return this.storageRootPath;
    }
    
    @JsonProperty("cluster_nodes")
    public void addClusterNodes(Collection<String> nodes) {
        if(nodes == null) {
            throw new IllegalArgumentException("nodes is null");
        }
        
        super.checkMutableAndRaiseException();
        
        for(String node : nodes) {
            addClusterNode(node);
        }
    }
    
    @JsonIgnore
    public void addClusterNode(String node) {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.clusterNodes.add(node);
    }
    
    @JsonProperty("cluster_nodes")
    public Collection<String> getClusterNodes() {
        return Collections.unmodifiableCollection(this.clusterNodes);
    }
}
