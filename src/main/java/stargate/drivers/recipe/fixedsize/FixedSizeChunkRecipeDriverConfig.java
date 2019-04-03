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
package stargate.drivers.recipe.fixedsize;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.recipe.AbstractRecipeDriverConfig;

/**
 *
 * @author iychoi
 */
public class FixedSizeChunkRecipeDriverConfig extends AbstractRecipeDriverConfig {
    
    private static final int DEFAULT_CHUNK_SIZE = 1024*1024; // 1MB
    private static final String DEFAULT_HASH_ALGORITHM = "SHA-1";
    private static final int DEFAULT_BUFFER_SIZE = 64*1024; // 64KB
    
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    private String hashAlgorithm = DEFAULT_HASH_ALGORITHM;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    
    public static FixedSizeChunkRecipeDriverConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (FixedSizeChunkRecipeDriverConfig) JsonSerializer.fromJsonFile(file, FixedSizeChunkRecipeDriverConfig.class);
    }
    
    public static FixedSizeChunkRecipeDriverConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (FixedSizeChunkRecipeDriverConfig) JsonSerializer.fromJson(json, FixedSizeChunkRecipeDriverConfig.class);
    }
    
    public FixedSizeChunkRecipeDriverConfig() {
    }
    
    @JsonProperty("chunk_size")
    public void setChunkSize(int chunkSize) {
        if(chunkSize < 0) {
            throw new IllegalArgumentException("chunkSize is negative");
        }
        
        super.checkMutableAndRaiseException();
        
        this.chunkSize = chunkSize;
    }
    
    @JsonProperty("chunk_size")
    public int getChunkSize() {
        return this.chunkSize;
    }
    
    @JsonProperty("hash_algorithm")
    public void setHashAlgorithm(String hashAlgorithm) {
        if(hashAlgorithm == null || hashAlgorithm.isEmpty()) {
            throw new IllegalArgumentException("hashAlgorithm is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.hashAlgorithm = hashAlgorithm;
    }
    
    @JsonProperty("hash_algorithm")
    public String getHashAlgorithm() {
        return this.hashAlgorithm;
    }
    
    @JsonProperty("buffer_size")
    public void setBufferSize(int bufferSize) {
        super.checkMutableAndRaiseException();
        
        if(bufferSize <= 0) {
            this.bufferSize = DEFAULT_BUFFER_SIZE;
        } else {
            this.bufferSize = bufferSize;
        }
        
        this.bufferSize = bufferSize;
    }
    
    @JsonProperty("buffer_size")
    public int getBufferSize() {
        return this.bufferSize;
    }
}
