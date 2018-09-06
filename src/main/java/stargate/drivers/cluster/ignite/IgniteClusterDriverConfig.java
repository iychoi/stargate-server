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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.cluster.AbstractClusterDriverConfig;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class IgniteClusterDriverConfig extends AbstractClusterDriverConfig {
    
    private static final Log LOG = LogFactory.getLog(IgniteClusterDriverConfig.class);
    
    public static final String DEFAULT_CLUSTER_NAME = "StargateCluster";
    
    private String clusterName = DEFAULT_CLUSTER_NAME;
    
    public static IgniteClusterDriverConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (IgniteClusterDriverConfig) serializer.fromJsonFile(file, IgniteClusterDriverConfig.class);
    }
    
    public static IgniteClusterDriverConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (IgniteClusterDriverConfig) serializer.fromJson(json, IgniteClusterDriverConfig.class);
    }
    
    public IgniteClusterDriverConfig() {
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
}
