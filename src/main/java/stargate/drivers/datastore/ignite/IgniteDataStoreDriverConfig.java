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
    
    private static final int DEFAULT_PART_SIZE = 1024 * 1024; // 1 MB
    private static final long DEFAULT_PART_WAIT_TIMEOUT_SEC = 300; // 300 seconds
    private static final long DEFAULT_PART_WAIT_POLLING_INTERVAL_MILLISEC = 100; // 100 msec
    
    private int partSize = DEFAULT_PART_SIZE;
    private long partWaitTimeoutSec = DEFAULT_PART_WAIT_TIMEOUT_SEC;
    private long partWaitPollingIntervalMsec = DEFAULT_PART_WAIT_POLLING_INTERVAL_MILLISEC;
    
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
    
    @JsonProperty("part_size")
    public void setPartSize(int partSize) {
        super.checkMutableAndRaiseException();
        
        if(partSize < 0) {
            this.partSize = DEFAULT_PART_SIZE;
        } else {
            this.partSize = partSize;
        }
    }
    
    @JsonProperty("part_size")
    public int getPartSize() {
        return this.partSize;
    }
    
    @JsonProperty("part_wait_timeout_sec")
    public void setPartWaitTimeoutSec(long partWaitTimeoutSec) {
        super.checkMutableAndRaiseException();
        
        if(partWaitTimeoutSec < 0) {
            this.partWaitTimeoutSec = DEFAULT_PART_WAIT_TIMEOUT_SEC;
        } else {
            this.partWaitTimeoutSec = partWaitTimeoutSec;
        }
    }
    
    @JsonProperty("part_wait_timeout_sec")
    public long getDataWaitTimeoutSec() {
        return this.partWaitTimeoutSec;
    }
    
    @JsonProperty("part_wait_polling_interval_msec")
    public void setPartWaitPollingIntervalMsec(long partWaitPollingIntervalMsec) {
        super.checkMutableAndRaiseException();
        
        if(partWaitPollingIntervalMsec < 0) {
            this.partWaitPollingIntervalMsec = DEFAULT_PART_WAIT_POLLING_INTERVAL_MILLISEC;
        } else {
            this.partWaitPollingIntervalMsec = partWaitPollingIntervalMsec;
        }
    }
    
    @JsonProperty("part_wait_polling_interval_msec")
    public long getDataWaitPollingIntervalMsec() {
        return this.partWaitPollingIntervalMsec;
    }
}
