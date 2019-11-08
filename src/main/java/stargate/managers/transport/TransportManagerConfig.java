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
import stargate.commons.manager.ManagerConfig;
import stargate.managers.transport.layout.ContactNodeSelectionAlgorithms;
import stargate.managers.transport.layout.TransferLayoutAlgorithms;

/**
 *
 * @author iychoi
 */
public class TransportManagerConfig extends ManagerConfig {
    
    private static final int DEFAULT_TRANSFER_THREADS = 3;
    private static final int DEFAULT_PREFETCH_SCHEDULER_THREADS = 1;
    private static final int DEFAULT_DATA_TRANSFER_TIMEOUT_SEC = 60*5; // 5min
    private static final int DEFAULT_PENDING_PREFETCH_TIMEOUT_SEC = 24*60*60; // 1day
    private static final long DEFAULT_PREFETCH_WINDOW_SIZE = 0;
    private static final int DEFAULT_DIRECTORY_CACHE_TIMEOUT_SEC = 60*5; // 5min
    private static final int DEFAULT_RECIPE_CACHE_TIMEOUT_SEC = 24*60*60; // 1day
    private static final int DEFAULT_DATACHUNK_CACHE_TIMEOUT_SEC = 7*24*60*60; // 7day
    
    private TransferLayoutAlgorithms layoutAlgorithm = TransferLayoutAlgorithms.TRANSFER_LAYOUT_ALGORITHM_STATIC;
    private ContactNodeSelectionAlgorithms nodeSelectionAlgorithm = ContactNodeSelectionAlgorithms.CONTACT_NODE_SELECTION_ALGORITHM_ROUNDROBIN;
    
    private int transferThreads = DEFAULT_TRANSFER_THREADS;
    private int prefetchSchedulerThreads = DEFAULT_PREFETCH_SCHEDULER_THREADS;
    private int pendingPrefetchTimeout = DEFAULT_PENDING_PREFETCH_TIMEOUT_SEC;
    private int dataTransferTimeout = DEFAULT_DATA_TRANSFER_TIMEOUT_SEC;
    private long prefetchWindowSize = DEFAULT_PREFETCH_WINDOW_SIZE;
    private int directoryCacheTimeout = DEFAULT_DIRECTORY_CACHE_TIMEOUT_SEC;
    private int recipeCacheTimeout = DEFAULT_RECIPE_CACHE_TIMEOUT_SEC;
    private int dataChunkCacheTimeout = DEFAULT_DATACHUNK_CACHE_TIMEOUT_SEC;
        
    public static TransportManagerConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (TransportManagerConfig) JsonSerializer.fromJsonFile(file, TransportManagerConfig.class);
    }
    
    public static TransportManagerConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (TransportManagerConfig) JsonSerializer.fromJson(json, TransportManagerConfig.class);
    }
    
    public TransportManagerConfig() {
    }
    
    @JsonIgnore
    public void setLayoutAlgorithm(TransferLayoutAlgorithms layoutAlgorithm) {
        if(layoutAlgorithm == null) {
            throw new IllegalArgumentException("layoutAlgorithm is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.layoutAlgorithm = layoutAlgorithm;
    }
    
    @JsonProperty("transfer_layout")
    public void setLayoutAlgorithmString(String layoutAlgorithm) {
        if(layoutAlgorithm == null || layoutAlgorithm.isEmpty()) {
            throw new IllegalArgumentException("layoutAlgorithm is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.layoutAlgorithm = TransferLayoutAlgorithms.fromStringVal(layoutAlgorithm);
    }
    
    @JsonIgnore
    public TransferLayoutAlgorithms getLayoutAlgorithm() {
        return this.layoutAlgorithm;
    }
    
    @JsonProperty("transfer_layout")
    public String getLayoutAlgorithmString() {
        if(this.layoutAlgorithm != null) {
            return this.layoutAlgorithm.getStringVal();
        }
        return null;
    }
    
    @JsonIgnore
    public void setContactNodeSelectionAlgorithm(ContactNodeSelectionAlgorithms nodeSelectionAlgorithm) {
        if(nodeSelectionAlgorithm == null) {
            throw new IllegalArgumentException("nodeSelectionAlgorithm is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.nodeSelectionAlgorithm = nodeSelectionAlgorithm;
    }
    
    @JsonProperty("contact_node_selection")
    public void setContactNodeSelectionAlgorithmString(String nodeSelectionAlgorithm) {
        if(nodeSelectionAlgorithm == null || nodeSelectionAlgorithm.isEmpty()) {
            throw new IllegalArgumentException("nodeSelectionAlgorithm is null or empty");
        }
        
        super.checkMutableAndRaiseException();
        
        this.nodeSelectionAlgorithm = ContactNodeSelectionAlgorithms.fromStringVal(nodeSelectionAlgorithm);
    }
    
    @JsonIgnore
    public ContactNodeSelectionAlgorithms getContactNodeSelectionAlgorithm() {
        return this.nodeSelectionAlgorithm;
    }
    
    @JsonProperty("contact_node_selection")
    public String getContactNodeSelectionAlgorithmString() {
        if(this.nodeSelectionAlgorithm != null) {
            return this.nodeSelectionAlgorithm.getStringVal();
        }
        return null;
    }
    
    @JsonProperty("transfer_threads")
    public void setTransferThreads(int threads) {
        super.checkMutableAndRaiseException();
        
        if(threads <= 0) {
            this.transferThreads= DEFAULT_TRANSFER_THREADS;
        } else {
            this.transferThreads = threads;
        }
    }
    
    @JsonProperty("transfer_threads")
    public int getTransferThreads() {
        return this.transferThreads;
    }
    
    @JsonProperty("prefetch_scheduler_threads")
    public void setPrefetchSchedulerThreads(int threads) {
        super.checkMutableAndRaiseException();
        
        if(threads <= 0) {
            this.prefetchSchedulerThreads = DEFAULT_PREFETCH_SCHEDULER_THREADS;
        } else {
            this.prefetchSchedulerThreads = threads;
        }
    }
    
    @JsonProperty("prefetch_scheduler_threads")
    public int getPrefetchSchedulerThreads() {
        return this.prefetchSchedulerThreads;
    }
        
    @JsonProperty("pending_prefetch_timeout")
    public void setPendingPrefetchTimeoutSec(int sec) {
        super.checkMutableAndRaiseException();
        
        if(sec < 0) {
            this.pendingPrefetchTimeout = DEFAULT_PENDING_PREFETCH_TIMEOUT_SEC;
        } else {
            this.pendingPrefetchTimeout = sec;
        }
    }
    
    @JsonProperty("pending_prefetch_timeout")
    public int getPendingPrefetchTimeoutSec() {
        return this.pendingPrefetchTimeout;
    }
    
    @JsonProperty("data_transfer_timeout")
    public void setDataTransferTimeoutSec(int sec) {
        super.checkMutableAndRaiseException();
        
        if(sec < 0) {
            this.dataTransferTimeout = DEFAULT_DATA_TRANSFER_TIMEOUT_SEC;
        } else {
            this.dataTransferTimeout = sec;
        }
    }
    
    @JsonProperty("data_transfer_timeout")
    public int getDataTransferTimeoutSec() {
        return this.dataTransferTimeout;
    }
    
    @JsonProperty("prefetch_window_size")
    public void setPrefetchWindowSize(long length) {
        super.checkMutableAndRaiseException();
        
        if(length < 0) {
            this.prefetchWindowSize = DEFAULT_PREFETCH_WINDOW_SIZE;
        } else {
            this.prefetchWindowSize = length;
        }
    }
    
    @JsonProperty("prefetch_window_size")
    public long getPrefetchWindowSize() {
        return this.prefetchWindowSize;
    }
    
    @JsonProperty("directory_cache_timeout")
    public void setDirectoryCacheTimeoutSec(int sec) {
        super.checkMutableAndRaiseException();
        
        if(sec <= 0) {
            this.directoryCacheTimeout = DEFAULT_DIRECTORY_CACHE_TIMEOUT_SEC;
        } else {
            this.directoryCacheTimeout = sec;
        }
    }
    
    @JsonProperty("directory_cache_timeout")
    public int getDirectoryCacheTimeoutSec() {
        return this.directoryCacheTimeout;
    }
    
    @JsonProperty("recipe_cache_timeout")
    public void setRecipeCacheTimeoutSec(int sec) {
        super.checkMutableAndRaiseException();
        
        if(sec <= 0) {
            this.recipeCacheTimeout = DEFAULT_RECIPE_CACHE_TIMEOUT_SEC;
        } else {
            this.recipeCacheTimeout = sec;
        }
    }
    
    @JsonProperty("recipe_cache_timeout")
    public int getRecipeCacheTimeoutSec() {
        return this.recipeCacheTimeout;
    }
    
    @JsonProperty("data_chunk_cache_timeout")
    public void setDataChunkCacheTimeoutSec(int sec) {
        super.checkMutableAndRaiseException();
        
        if(sec < 0) {
            this.dataChunkCacheTimeout = DEFAULT_DATACHUNK_CACHE_TIMEOUT_SEC;
        } else {
            this.dataChunkCacheTimeout = sec;
        }
    }
    
    @JsonProperty("data_chunk_cache_timeout")
    public int getDataChunkCacheTimeoutSec() {
        return this.dataChunkCacheTimeout;
    }
}
