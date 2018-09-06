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
public class ClusterPolicy extends AbstractPolicy {

    private static final Log LOG = LogFactory.getLog(ClusterPolicy.class);
    
    // node failure report interval
    public static final long DEFAULT_NODE_FAILURE_REPORT_INTERVAL_SEC = 10;
    public static final String NODE_FAILURE_REPORT_INTERVAL_SEC = "node_failure_report_interval_sec";
    
    // failures to be blacklisted
    public static final int DEFAULT_NUM_FAILURES_TO_BE_BLACKLISTED = 5;
    public static final String NUM_FAILURES_TO_BE_BLACKLISTED = "num_failures_to_be_blacklisted";
    
    // remote cluster node synchronization
    public static final long DEFAULT_CLUSTER_NODE_SYNC_INTERVAL_SEC = 60 * 60;
    public static final String CLUSTER_NODE_SYNC_INTERVAL_SEC = "cluster_node_sync_interval_sec";
    
    public static ClusterPolicy createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterPolicy) serializer.fromJsonFile(file, ClusterPolicy.class);
    }
    
    public static ClusterPolicy createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (ClusterPolicy) serializer.fromJson(json, ClusterPolicy.class);
    }
    
    ClusterPolicy() {
    }
    
    @JsonProperty("node_failure_report_interval_sec")
    public long getNodeFailureReportIntervalSec() {
        return (long) this.get(NODE_FAILURE_REPORT_INTERVAL_SEC, DEFAULT_NODE_FAILURE_REPORT_INTERVAL_SEC);
    }
    
    @JsonProperty("node_failure_report_interval_sec")
    public void setNodeFailureReportIntervalSec(long sec) throws IOException {
        long val = Math.max(0, sec);
        this.put(NODE_FAILURE_REPORT_INTERVAL_SEC, val);
    }
    
    @JsonProperty("num_failures_to_be_blacklisted")
    public int getNumFailuresToBeBlacklisted() {
        return (int) this.get(NUM_FAILURES_TO_BE_BLACKLISTED, DEFAULT_NUM_FAILURES_TO_BE_BLACKLISTED);
    }
    
    @JsonProperty("num_failures_to_be_blacklisted")
    public void setNumFailuresToBeBlacklisted(int num) throws IOException {
        int val = Math.max(0, num);
        this.put(NUM_FAILURES_TO_BE_BLACKLISTED, val);
    }
    
    @JsonProperty("cluster_node_sync_interval_sec")
    public long getClusterNodeSyncIntervalSec() {
        return (long) this.get(CLUSTER_NODE_SYNC_INTERVAL_SEC, DEFAULT_CLUSTER_NODE_SYNC_INTERVAL_SEC);
    }
    
    @JsonProperty("cluster_node_sync_interval_sec")
    public void setClusterNodeSyncIntervalSec(long sec) throws IOException {
        long val = Math.max(0, sec);
        this.put(CLUSTER_NODE_SYNC_INTERVAL_SEC, val);
    }
    
    @Override
    @JsonIgnore
    public Collection<String> getKeyList() {
        List<String> keys = new ArrayList<String>();
        keys.add(NODE_FAILURE_REPORT_INTERVAL_SEC);
        keys.add(NUM_FAILURES_TO_BE_BLACKLISTED);
        keys.add(CLUSTER_NODE_SYNC_INTERVAL_SEC);
        return Collections.unmodifiableCollection(keys);
    }
    
    @JsonIgnore
    public String getGroupName() {
        return "cluster";
    }
    
    @JsonIgnore
    public ClusterPolicy clone() {
        ClusterPolicy newPolicy = new ClusterPolicy();
        newPolicy.manager = this.manager;
        newPolicy.store.putAll(this.store);
        return newPolicy;
    }
}
