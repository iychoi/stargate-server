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
package stargate.drivers.datasource.localfs;

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.datasource.AbstractDataSourceDriverConfig;

/**
 *
 * @author iychoi
 */
public class LocalFSDataSourceDriverConfig extends AbstractDataSourceDriverConfig {
    
    private static final Log LOG = LogFactory.getLog(LocalFSDataSourceDriverConfig.class);
    
    public static final String DEFAULT_SCHEME = "file";
    
    private String scheme = DEFAULT_SCHEME;
    private File rootPath = new File("/");
    
    public static LocalFSDataSourceDriverConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (LocalFSDataSourceDriverConfig) serializer.fromJsonFile(file, LocalFSDataSourceDriverConfig.class);
    }
    
    public static LocalFSDataSourceDriverConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (LocalFSDataSourceDriverConfig) serializer.fromJson(json, LocalFSDataSourceDriverConfig.class);
    }
    
    public LocalFSDataSourceDriverConfig() {
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
    
    @JsonProperty("root_path")
    public void setRootPath(String rootPath) {
        if(rootPath == null || rootPath.isEmpty()) {
            throw new IllegalArgumentException("rootPath is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.rootPath = new File(rootPath);
    }
    
    @JsonIgnore
    public void setRootPath(File rootPath) {
        if(rootPath == null) {
            throw new IllegalArgumentException("rootPath is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.rootPath = rootPath;
    }
    
    @JsonProperty("root_path")
    public String getRootPathString() {
        return this.rootPath.getAbsolutePath();
    }
    
    @JsonIgnore
    public File getRootPath() {
        return this.rootPath;
    }
}
