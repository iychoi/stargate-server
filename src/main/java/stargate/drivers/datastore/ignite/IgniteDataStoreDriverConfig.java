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
package stargate.drivers.datastore.ignite;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.datastore.AbstractDataStoreDriverConfig;

/**
 *
 * @author iychoi
 */
public class IgniteDataStoreDriverConfig extends AbstractDataStoreDriverConfig {
    
    private static final int DEFAULT_CHUNK_SIZE = 1 * 1024 * 1024; // 1MB
    
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    
    public static IgniteDataStoreDriverConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (IgniteDataStoreDriverConfig) JsonSerializer.fromJsonFile(file, IgniteDataStoreDriverConfig.class);
    }
    
    public static IgniteDataStoreDriverConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (IgniteDataStoreDriverConfig) JsonSerializer.fromJson(json, IgniteDataStoreDriverConfig.class);
    }
    
    public IgniteDataStoreDriverConfig() {
    }
    
    @JsonProperty("chunk_size")
    public void setChunkSize(int chunkSize) {
        super.checkMutableAndRaiseException();
        
        if(chunkSize < 0) {
            this.chunkSize = DEFAULT_CHUNK_SIZE;
        } else {
            this.chunkSize = chunkSize;
        }
    }
    
    @JsonProperty("chunk_size")
    public int getChunkSize() {
        return this.chunkSize;
    }
}
