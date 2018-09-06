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
package stargate.service;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.cluster.Cluster;
import stargate.commons.config.AbstractImmutableConfig;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.utils.JsonSerializer;


/**
 *
 * @author iychoi
 */
public class UserConfig extends AbstractImmutableConfig {
    
    private static final Log LOG = LogFactory.getLog(UserConfig.class);
    
    private Map<String, Cluster> remoteClusters = new HashMap<String, Cluster>();
    private Map<String, DataExportEntry> dataExportEntries = new HashMap<String, DataExportEntry>();
    
    public static UserConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (UserConfig) serializer.fromJsonFile(file, UserConfig.class);
    }
    
    public static UserConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (UserConfig) serializer.fromJson(json, UserConfig.class);
    }
    
    public UserConfig() {
    }
    
    @Override
    public void setImmutable() {
        super.setImmutable();
    }
    
    @JsonProperty("remote_clusters")
    public void addRemoteClusters(Collection<Cluster> clusters) {
        if(clusters == null) {
            throw new IllegalArgumentException("clusters is null");
        }
        
        super.checkMutableAndRaiseException();
        
        for(Cluster cluster : clusters) {
            this.remoteClusters.put(cluster.getName(), cluster);
        }
    }
    
    @JsonIgnore
    public void addRemoteCluster(Cluster cluster) {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.remoteClusters.put(cluster.getName(), cluster);
    }
    
    @JsonProperty("remote_clusters")
    public Collection<Cluster> getRemoteClusters() {
        return Collections.unmodifiableCollection(this.remoteClusters.values());
    }
    
    @JsonProperty("data_exports")
    public void addDataExportEntries(Collection<DataExportEntry> entries) {
        if(entries == null) {
            throw new IllegalArgumentException("entries is null");
        }
        
        super.checkMutableAndRaiseException();
        
        for(DataExportEntry entry : entries) {
            this.dataExportEntries.put(entry.getStargatePath(), entry);
        }
    }
    
    @JsonIgnore
    public void addDataExportEntry(DataExportEntry entry) {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.dataExportEntries.put(entry.getStargatePath(), entry);
    }
    
    @JsonProperty("data_exports")
    public Collection<DataExportEntry> getDataExportEntries() {
        return Collections.unmodifiableCollection(this.dataExportEntries.values());
    }
}
