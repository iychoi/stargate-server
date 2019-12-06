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
    
    private static final int DEFAULT_PART_SIZE = 512 * 1024; // 512 KB
    private static final long DEFAULT_PART_WAIT_TIMEOUT_SEC = 300; // 300 seconds
    private static final long DEFAULT_PART_WAIT_SLEEP_MILLISEC = 500; // 500 msec
    
    private int partSize = DEFAULT_PART_SIZE;
    private long partWaitTimeoutSec = DEFAULT_PART_WAIT_TIMEOUT_SEC;
    private long partWaitSleepMsec = DEFAULT_PART_WAIT_SLEEP_MILLISEC;
    
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
    public long getPartWaitTimeoutSec() {
        return this.partWaitTimeoutSec;
    }
    
    @JsonProperty("part_wait_sleep_msec")
    public void setPartWaitSleepMsec(long partWaitSleepMsec) {
        super.checkMutableAndRaiseException();
        
        if(partWaitSleepMsec < 0) {
            this.partWaitSleepMsec = DEFAULT_PART_WAIT_SLEEP_MILLISEC;
        } else {
            this.partWaitSleepMsec = partWaitSleepMsec;
        }
    }
    
    @JsonProperty("part_wait_sleep_msec")
    public long getPartWaitSleepMsec() {
        return this.partWaitSleepMsec;
    }
}
