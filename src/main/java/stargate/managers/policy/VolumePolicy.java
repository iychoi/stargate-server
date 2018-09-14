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
package stargate.managers.policy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class VolumePolicy extends AbstractPolicy {

    private static final Log LOG = LogFactory.getLog(VolumePolicy.class);
    
    // scan interval for data updates
    public static final long DEFAULT_SCAN_FOR_DATA_UPDATE_INTERVAL_SEC = 60 * 5;
    public static final String SCAN_FOR_DATA_UPDATE_INTERVAL_SEC = "scan_for_data_update_interval_sec";
    
    // remote object metadata sync interval
    public static final int DEFAULT_REMOTE_OBJECT_METADATA_SYNC_INTERVAL_SEC = 60 * 60;
    public static final String REMOTE_OBJECT_METADATA_SYNC_INTERVAL_SEC = "remote_object_metadata_sync_interval_sec";
    
    public static VolumePolicy createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        return (VolumePolicy) JsonSerializer.fromJsonFile(file, VolumePolicy.class);
    }
    
    public static VolumePolicy createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (VolumePolicy) JsonSerializer.fromJson(json, VolumePolicy.class);
    }
    
    VolumePolicy() {
    }
    
    @JsonProperty("scan_for_data_update_interval_sec")
    public long getScanForDataUpdateIntervalSec() {
        return (long) this.get(SCAN_FOR_DATA_UPDATE_INTERVAL_SEC, DEFAULT_SCAN_FOR_DATA_UPDATE_INTERVAL_SEC);
    }
    
    @JsonProperty("scan_for_data_update_interval_sec")
    public void setScanForDataUpdateIntervalSec(long sec) throws IOException {
        long val = Math.max(0, sec);
        this.put(SCAN_FOR_DATA_UPDATE_INTERVAL_SEC, val);
    }
    
    @JsonProperty("remote_object_metadata_sync_interval_sec")
    public long getRemoteObjectMetadataSyncIntervalSec() {
        return (long) this.get(REMOTE_OBJECT_METADATA_SYNC_INTERVAL_SEC, DEFAULT_REMOTE_OBJECT_METADATA_SYNC_INTERVAL_SEC);
    }
    
    @JsonProperty("remote_object_metadata_sync_interval_sec")
    public void setRemoteObjectMetadataSyncIntervalSec(long sec) throws IOException {
        long val = Math.max(0, sec);
        this.put(REMOTE_OBJECT_METADATA_SYNC_INTERVAL_SEC, val);
    }
    
    @Override
    @JsonIgnore
    public Collection<String> getKeyList() {
        List<String> keys = new ArrayList<String>();
        keys.add(SCAN_FOR_DATA_UPDATE_INTERVAL_SEC);
        keys.add(REMOTE_OBJECT_METADATA_SYNC_INTERVAL_SEC);
        return Collections.unmodifiableCollection(keys);
    }
    
    @JsonIgnore
    public String getGroupName() {
        return "volume";
    }
    
    @JsonIgnore
    public VolumePolicy clone() {
        VolumePolicy newPolicy = new VolumePolicy();
        newPolicy.manager = this.manager;
        newPolicy.store.putAll(this.store);
        return newPolicy;
    }
}
