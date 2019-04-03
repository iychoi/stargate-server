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
package stargate.managers.statistics;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.manager.ManagerConfig;

/**
 *
 * @author iychoi
 */
public class StatisticsManagerConfig extends ManagerConfig {
    
    private static final int DEFAULT_STATISTICS_ENTRY_CAPACITY = 1000;
    
    private int entryCapacity = DEFAULT_STATISTICS_ENTRY_CAPACITY;
    
    public static StatisticsManagerConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (StatisticsManagerConfig) JsonSerializer.fromJsonFile(file, StatisticsManagerConfig.class);
    }
    
    public static StatisticsManagerConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (StatisticsManagerConfig) JsonSerializer.fromJson(json, StatisticsManagerConfig.class);
    }
    
    public StatisticsManagerConfig() {
    }
    
    @JsonProperty("entry_capacity")
    public void setStatisticsEntryCapacity(int capacity) {
        super.checkMutableAndRaiseException();
        
        if(capacity < 0) {
            this.entryCapacity = DEFAULT_STATISTICS_ENTRY_CAPACITY;
        } else {
            this.entryCapacity = capacity;
        }
    }
    
    @JsonProperty("entry_capacity")
    public int getStatisticsEntryCapacity() {
        return this.entryCapacity;
    }
}
