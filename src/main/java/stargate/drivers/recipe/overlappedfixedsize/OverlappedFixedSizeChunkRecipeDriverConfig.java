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
package stargate.drivers.recipe.overlappedfixedsize;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.recipe.AbstractRecipeDriverConfig;

/**
 *
 * @author iychoi
 */
public class OverlappedFixedSizeChunkRecipeDriverConfig extends AbstractRecipeDriverConfig {
    
    private static final int DEFAULT_CHUNK_SIZE = 1024*1024; // 1MB
    private static final int DEFAULT_OVERLAP_SIZE = 4*1024; // 4K
    private static final String DEFAULT_HASH_ALGORITHM = "SHA-1";
    
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    private int overlapSize = DEFAULT_OVERLAP_SIZE;
    private String hashAlgorithm = DEFAULT_HASH_ALGORITHM;
    
    public static OverlappedFixedSizeChunkRecipeDriverConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (OverlappedFixedSizeChunkRecipeDriverConfig) JsonSerializer.fromJsonFile(file, OverlappedFixedSizeChunkRecipeDriverConfig.class);
    }
    
    public static OverlappedFixedSizeChunkRecipeDriverConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (OverlappedFixedSizeChunkRecipeDriverConfig) JsonSerializer.fromJson(json, OverlappedFixedSizeChunkRecipeDriverConfig.class);
    }
    
    public OverlappedFixedSizeChunkRecipeDriverConfig() {
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
    
    @JsonProperty("overlap_size")
    public void setOverlapSize(int overlapSize) {
        if(overlapSize < 0) {
            throw new IllegalArgumentException("overlapSize is negative");
        }
        
        super.checkMutableAndRaiseException();
        
        this.overlapSize = overlapSize;
    }
    
    @JsonProperty("overlap_size")
    public int getOverlapSize() {
        return this.overlapSize;
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
}
