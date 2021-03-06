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
import stargate.commons.transport.TransferAssignment;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class PendingPrefetchSchedule {

    private TransferAssignment transferAssignment;
    private DataChunkCacheMetadata dataChunkCacheMetadata;
    
    public static PendingPrefetchSchedule createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (PendingPrefetchSchedule) JsonSerializer.fromJsonFile(file, PendingPrefetchSchedule.class);
    }
    
    public static PendingPrefetchSchedule createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (PendingPrefetchSchedule) JsonSerializer.fromJson(json, PendingPrefetchSchedule.class);
    }
    
    public PendingPrefetchSchedule() {
    }
    
    public PendingPrefetchSchedule(TransferAssignment transferAssignment) {
        if(transferAssignment == null) {
            throw new IllegalArgumentException("transferAssignment is null");
        }
        
        this.transferAssignment = transferAssignment;
        this.dataChunkCacheMetadata = null;
    }
    
    public PendingPrefetchSchedule(TransferAssignment transferAssignment, DataChunkCacheMetadata dataChunkCacheMetadata) {
        if(transferAssignment == null) {
            throw new IllegalArgumentException("transferAssignment is null");
        }
        
        if(dataChunkCacheMetadata == null) {
            throw new IllegalArgumentException("dataChunkCacheMetadata is null");
        }
        
        this.transferAssignment = transferAssignment;
        this.dataChunkCacheMetadata = dataChunkCacheMetadata;
    }
    
    @JsonProperty("assignment")
    public void setTransferAssignment(TransferAssignment transferAssignment) {
        if(transferAssignment == null) {
            throw new IllegalArgumentException("transferAssignment is null");
        }
        
        this.transferAssignment = transferAssignment;
    }

    @JsonProperty("assignment")    
    public TransferAssignment getTransferAssignment() {
        return this.transferAssignment;
    }
    
    @JsonProperty("data_chunk_cache_metadata")
    public void setDataChunkCacheMetadata(DataChunkCacheMetadata dataChunkCacheMetadata) {
        if(dataChunkCacheMetadata == null) {
            throw new IllegalArgumentException("dataChunkCacheMetadata is null");
        }
        
        this.dataChunkCacheMetadata = dataChunkCacheMetadata;
    }

    @JsonProperty("data_chunk_cache_metadata")    
    public DataChunkCacheMetadata getDataChunkCacheMetadata() {
        return this.dataChunkCacheMetadata;
    }
    
    public boolean hasDataChunkCacheMetadata() {
        return (this.dataChunkCacheMetadata != null);
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        return "PendingPrefetchSchedule";
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
