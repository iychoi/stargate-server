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
package stargate.drivers.datasource.hdfs;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.datasource.AbstractDataSourceDriverConfig;

/**
 *
 * @author iychoi
 */
public class HDFSDataSourceDriverConfig extends AbstractDataSourceDriverConfig {
    
    public static final String DEFAULT_SCHEME = "hdfs";
    
    private String scheme = DEFAULT_SCHEME;
    private String nameNodeURI = "hdfs://localhost:9000";
    private Path rootPath = new Path("/");
    private String hadoopUsername = "hdfs";
    
    public static HDFSDataSourceDriverConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (HDFSDataSourceDriverConfig) JsonSerializer.fromJsonFile(file, HDFSDataSourceDriverConfig.class);
    }
    
    public static HDFSDataSourceDriverConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (HDFSDataSourceDriverConfig) JsonSerializer.fromJson(json, HDFSDataSourceDriverConfig.class);
    }
    
    public HDFSDataSourceDriverConfig() {
    }
    
    @JsonProperty("scheme")
    public void setScheme(String scheme) {
        if(scheme == null || scheme.isEmpty()) {
            throw new IllegalArgumentException("scheme is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.scheme = scheme;
    }
    
    @JsonProperty("scheme")
    public String getScheme() {
        return this.scheme;
    }
    
    
    @JsonProperty("namenode_uri")
    public void setNameNodeURI(String nameNodeURI) {
        if(nameNodeURI == null || nameNodeURI.isEmpty()) {
            throw new IllegalArgumentException("nameNodeURI is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.nameNodeURI = nameNodeURI;
    }
    
    @JsonProperty("namenode_uri")
    public String getNameNodeURI() {
        return this.nameNodeURI;
    }
    
    @JsonProperty("root_path")
    public void setRootPath(String rootPath) {
        if(rootPath == null || rootPath.isEmpty()) {
            throw new IllegalArgumentException("rootPath is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.rootPath = new Path(rootPath);
    }
    
    @JsonIgnore
    public void setRootPath(Path rootPath) {
        if(rootPath == null) {
            throw new IllegalArgumentException("rootPath is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.rootPath = rootPath;
    }
    
    @JsonProperty("root_path")
    public String getRootPathString() {
        return this.rootPath.toString();
    }
    
    @JsonIgnore
    public Path getRootPath() {
        return this.rootPath;
    }

    @JsonProperty("hadoop_username")
    public void setHadoopUsername(String username) {
        if(username == null || username.isEmpty()) {
            throw new IllegalArgumentException("username is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.hadoopUsername = username;
    }
    
    @JsonProperty("hadoop_username")
    public String getHadoopUsername() {
        return this.hadoopUsername;
    }
}
