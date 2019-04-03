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

import stargate.commons.statistics.StatisticsEntry;
import stargate.commons.statistics.Statistics;
import stargate.commons.statistics.StatisticsType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.driver.NullDriver;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.utils.DateTimeUtils;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class StatisticsManager extends AbstractManager<NullDriver> {
    
    private static final Log LOG = LogFactory.getLog(StatisticsManager.class);
    
    private static StatisticsManager instance;
    
    private Map<StatisticsType, Statistics> statistics = new ConcurrentHashMap<StatisticsType, Statistics>();
    protected long lastUpdateTime;
    
    
    public static StatisticsManager getInstance(StargateService service, StatisticsManagerConfig config) throws ManagerNotInstantiatedException {
        synchronized (StatisticsManager.class) {
            if(instance == null) {
                instance = new StatisticsManager(service, config);
            }
            return instance;
        }
    }
    
    public static StatisticsManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (StatisticsManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("StatisticsManager is not started");
            }
            return instance;
        }
    }
    
    StatisticsManager(StargateService service, StatisticsManagerConfig config) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.setService(service);
        this.setConfig(config);
    }
    
    @Override
    public synchronized void start() throws IOException {
        super.start();
    }
    
    @Override
    public synchronized void stop() throws IOException {
        this.statistics.clear();
        
        super.stop();
    }
    
    public Statistics getStatistics(StatisticsType type) throws IOException {
        if(type == null) {
            throw new IllegalStateException("type is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        Statistics stat = this.statistics.get(type);
        return stat;
    }
    
    public Collection<StatisticsEntry> getStatisticsEntries(StatisticsType type) throws IOException {
        if(type == null) {
            throw new IllegalStateException("type is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        Statistics stat = this.statistics.get(type);
        if(stat == null) {
            return Collections.unmodifiableCollection(new ArrayList<StatisticsEntry>());
        }
        
        return stat.getEntries();
    }
    
    public synchronized void addStatistics(StatisticsType type, String value) throws IOException {
        if(type == null) {
            throw new IllegalStateException("type is null");
        }
        
        if(value == null || value.isEmpty()) {
            throw new IllegalStateException("value is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        Statistics stat = this.statistics.get(type);
        if(stat == null) {
            StatisticsManagerConfig managerConfig = (StatisticsManagerConfig) this.config;
            
            stat = new Statistics(type, managerConfig.getStatisticsEntryCapacity());
            this.statistics.put(type, stat);
        }
        
        long timestamp = DateTimeUtils.getTimestamp();
        stat.addEntry(new StatisticsEntry(value, timestamp));
        
        this.lastUpdateTime = timestamp;
    }
    
    public synchronized void addStatistics(StatisticsType type, StatisticsEntry entry) throws IOException {
        if(type == null) {
            throw new IllegalStateException("type is null");
        }
        
        if(entry == null) {
            throw new IllegalStateException("entry is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        Statistics stat = this.statistics.get(type);
        if(stat == null) {
            StatisticsManagerConfig managerConfig = (StatisticsManagerConfig) this.config;
            
            stat = new Statistics(type, managerConfig.getStatisticsEntryCapacity());
            this.statistics.put(type, stat);
        }
        
        stat.addEntry(entry);
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
    }
    
    public synchronized void addRecipeChunkCreationStatistics(String path, long offset, long len, String hash) throws IOException {
        String format = String.format("path(%s) off(%d) len(%d) hash(%s)", path, offset, len, hash);
        addStatistics(StatisticsType.STATISTICS_TYPE_RECIPE_CHUNK_CREATION, format);
    }
    
    public synchronized void addDataChunkTransferReceiveStartStatistics(String path, String hash) throws IOException {
        String format = String.format("RECV_BEGIN path(%s) hash(%s)", path, hash);
        addStatistics(StatisticsType.STATISTICS_TYPE_DATA_CHUNK_TRANSFER_RECEIVE, format);
    }
    
    public synchronized void addDataChunkTransferReceiveEndStatistics(String path, String hash) throws IOException {
        String format = String.format("RECV_FINISH path(%s) hash(%s)", path, hash);
        addStatistics(StatisticsType.STATISTICS_TYPE_DATA_CHUNK_TRANSFER_RECEIVE, format);
    }
    
    public synchronized void addDataChunkTransferSendStatistics(String hash) throws IOException {
        String format = String.format("hash(%s)", hash);
        addStatistics(StatisticsType.STATISTICS_TYPE_DATA_CHUNK_TRANSFER_SEND, format);
    }
    
    public void clearStatistics(StatisticsType type) throws IOException {
        if(type == null) {
            throw new IllegalStateException("type is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        Statistics stat = this.statistics.get(type);
        if(stat != null) {
            stat.clear();
        }
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
    }
    
    public synchronized void clearAllStatistics() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        Collection<Statistics> values = this.statistics.values();
        Iterator<Statistics> iterator = values.iterator();
        while(iterator.hasNext()) {
            Statistics stat = iterator.next();
            stat.clear();
        }
        
        this.statistics.clear();
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
    }
    
    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
}
