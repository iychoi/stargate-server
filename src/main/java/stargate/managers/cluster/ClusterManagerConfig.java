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
package stargate.managers.cluster;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.manager.ManagerConfig;

/**
 *
 * @author iychoi
 */
public class ClusterManagerConfig extends ManagerConfig {
    
    private static final int DEFAULT_NODE_FAILURE_REPORT_INTERVAL_SEC = 60*5;
    private static final int DEFAULT_NUM_FAILURES_TO_BE_BLACKLISTED = 5;
    
    private int nodeFailureReportIntervalSec = DEFAULT_NODE_FAILURE_REPORT_INTERVAL_SEC;
    private int numFailuresToBeBlacklisted = DEFAULT_NUM_FAILURES_TO_BE_BLACKLISTED;
    
    public static ClusterManagerConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (ClusterManagerConfig) JsonSerializer.fromJsonFile(file, ClusterManagerConfig.class);
    }
    
    public static ClusterManagerConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (ClusterManagerConfig) JsonSerializer.fromJson(json, ClusterManagerConfig.class);
    }
    
    public ClusterManagerConfig() {
    }
    
    @JsonProperty("node_failure_report_interval_sec")
    public void setNodeFailureReportIntervalSec(int interval) {
        super.checkMutableAndRaiseException();
        
        if(interval < 0) {
            this.nodeFailureReportIntervalSec = DEFAULT_NODE_FAILURE_REPORT_INTERVAL_SEC;
        } else {
            this.nodeFailureReportIntervalSec = interval;
        }
    }
    
    @JsonProperty("node_failure_report_interval_sec")
    public int getNodeFailureReportIntervalSec() {
        return this.nodeFailureReportIntervalSec;
    }
    
    @JsonProperty("num_failures_for_blacklist")
    public void setNumFailuresForBlacklist(int num) {
        super.checkMutableAndRaiseException();
        
        if(num < 0) {
            this.numFailuresToBeBlacklisted = DEFAULT_NUM_FAILURES_TO_BE_BLACKLISTED;
        } else {
            this.numFailuresToBeBlacklisted = num;
        }
    }
    
    @JsonProperty("num_failures_for_blacklist")
    public int getNumFailuresForBlacklist() {
        return this.numFailuresToBeBlacklisted;
    }
}
