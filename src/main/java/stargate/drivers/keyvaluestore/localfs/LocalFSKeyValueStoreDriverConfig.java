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
package stargate.drivers.keyvaluestore.localfs;

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.keyvaluestore.AbstractKeyValueStoreDriverConfig;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class LocalFSKeyValueStoreDriverConfig extends AbstractKeyValueStoreDriverConfig {
    
    private static final Log LOG = LogFactory.getLog(LocalFSKeyValueStoreDriverConfig.class);
    
    private File rootPath = new File("/");
    
    public static LocalFSKeyValueStoreDriverConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (LocalFSKeyValueStoreDriverConfig) serializer.fromJsonFile(file, LocalFSKeyValueStoreDriverConfig.class);
    }
    
    public static LocalFSKeyValueStoreDriverConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (LocalFSKeyValueStoreDriverConfig) serializer.fromJson(json, LocalFSKeyValueStoreDriverConfig.class);
    }
    
    public LocalFSKeyValueStoreDriverConfig() {
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
