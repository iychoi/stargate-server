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
package stargate.managers.dataexport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.driver.NullDriver;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.utils.DateTimeUtils;
import stargate.managers.datastore.DataStoreManager;
import stargate.managers.event.AbstractStargateEventHandler;
import stargate.managers.event.EventManager;
import stargate.managers.event.StargateEvent;
import stargate.managers.event.StargateEventType;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class DataExportManager extends AbstractManager<NullDriver> {
    
    private static final Log LOG = LogFactory.getLog(DataExportManager.class);
    
    private static DataExportManager instance;
    
    private AbstractKeyValueStore dataExportEntryStore;
    private List<AbstractDataExportEventHandler> dataExportEventHandlers = new ArrayList<AbstractDataExportEventHandler>();
    protected long lastUpdateTime;
    
    private static final String DATA_EXPORT_STORE = "dexport";
    
    public static DataExportManager getInstance(StargateService service) throws ManagerNotInstantiatedException {
        synchronized (DataExportManager.class) {
            if(instance == null) {
                instance = new DataExportManager(service);
            }
            return instance;
        }
    }
    
    public static DataExportManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (DataExportManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("DataExportManager is not started");
            }
            return instance;
        }
    }
    
    DataExportManager(StargateService service) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        this.setService(service);
    }
    
    private StargateService getStargateService() {
        return (StargateService) this.getService();
    }
    
    @Override
    public synchronized void start() throws IOException {
        super.start();
        
        setEventHandler();
    }
    
    @Override
    public synchronized void stop() throws IOException {
        this.dataExportEventHandlers.clear();
        
        super.stop();
    }
    
    private void setEventHandler() {
        AbstractStargateEventHandler hander = new AbstractStargateEventHandler() {
            @Override
            public boolean accept(StargateEventType eventType) {
                if(eventType == StargateEventType.STARGATE_EVENT_TYPE_DATAEXPORT) {
                    return true;
                }
                return false;
            }

            @Override
            public void raised(EventManager manager, StargateEvent event) {
                DataExportEvent evt = (DataExportEvent) event.getValue();
                processDataExportEntryEvent(evt);
            }
        };
    }
    
    private synchronized void safeInitDataExportEntryStore() throws IOException {
        if(this.dataExportEntryStore == null) {
            try {
                StargateService stargateService = getStargateService();
                DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                this.dataExportEntryStore = keyValueStoreManager.getDriver().getKeyValueStore(DATA_EXPORT_STORE, DataExportEntry.class, EnumDataStoreProperty.DATASTORE_PROP_PERSISTENT_REPLICATED);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
    
    public synchronized Collection<DataExportEntry> getDataExportEntries() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataExportEntryStore();
        
        List<DataExportEntry> dataExportEntries = new ArrayList<DataExportEntry>();
        Map<String, Object> dataExportEntryMap = this.dataExportEntryStore.toMap();
        Set<Map.Entry<String, Object>> entrySet = dataExportEntryMap.entrySet();
        for(Map.Entry<String, Object> entry : entrySet) {
            DataExportEntry dataExportEntry = (DataExportEntry) entry.getValue();
            if(dataExportEntry != null) {
                dataExportEntries.add(dataExportEntry);
            }
        }
        
        // less efficient implementation
        //Collection<String> keys = this.dataExportEntryStore.keys();
        //for(String key : keys) {
        //    DataExportEntry entry = (DataExportEntry) this.dataExportEntryStore.get(key);
        //    if(entry != null) {
        //        dataExportEntries.add(entry);
        //    }
        //}
        
        return Collections.unmodifiableCollection(dataExportEntries);
    }
    
    public synchronized DataExportEntry getDataExportEntry(String stargatePath) throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataExportEntryStore();
        
        if(stargatePath == null || stargatePath.isEmpty()) {
            throw new IllegalArgumentException("stargatePath is null or empty");
        }
        
        return (DataExportEntry) this.dataExportEntryStore.get(stargatePath);
    }
    
    public synchronized boolean hasDataExportEntry(String stargatePath) throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataExportEntryStore();
        
        if(stargatePath == null || stargatePath.isEmpty()) {
            throw new IllegalArgumentException("stargatePath is null or empty");
        }
        
        return this.dataExportEntryStore.containsKey(stargatePath);
    }
    
    public synchronized void clearDataExportEntries() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataExportEntryStore();
        
        Collection<String> keys = this.dataExportEntryStore.keys();
        for(String key : keys) {
            // this is to raise a entry removal event
            removeDataExportEntry(key);
        }
    }
    
    public synchronized void addDataExportEntries(Collection<DataExportEntry> entries) throws DataExportManagerException, IOException {
        if(entries == null) {
            throw new IllegalArgumentException("entries is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataExportEntryStore();
        
        List<DataExportEntry> failed = new ArrayList<DataExportEntry>();
        
        for(DataExportEntry entry : entries) {
            try {
                addDataExportEntry(entry);
            } catch(DataExportManagerException ex) {
                failed.add(entry);
            }
        }
        
        if(!failed.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for(DataExportEntry entry : failed) {
                if(sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(entry.getStargatePath());
            }
            throw new DataExportManagerException("data export entries (" + sb.toString() + ") cannot be added (maybe already exist?)");
        }
    }
    
    public synchronized void addDataExportEntry(DataExportEntry entry) throws DataExportManagerException, IOException {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataExportEntryStore();
        
        if(this.dataExportEntryStore.containsKey(entry.getStargatePath())) {
            throw new DataExportManagerException("data export entry " + entry.getStargatePath() + " is already added");
        }
        
        this.dataExportEntryStore.put(entry.getStargatePath(), entry);
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
        
        try {
            raiseEventForDataExportEntryAdded(entry);
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        }
    }
    
    public synchronized void removeDataExportEntry(DataExportEntry entry) throws IOException {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataExportEntryStore();
        
        removeDataExportEntry(entry.getStargatePath());
    }
    
    public synchronized void removeDataExportEntry(String stargatePath) throws IOException {
        if(stargatePath == null || stargatePath.isEmpty()) {
            throw new IllegalArgumentException("stargatePath is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataExportEntryStore();
        
        DataExportEntry entry = (DataExportEntry) this.dataExportEntryStore.get(stargatePath);
        if(entry != null) {
            this.dataExportEntryStore.remove(stargatePath);
            
            this.lastUpdateTime = DateTimeUtils.getTimestamp();

            try {
                raiseEventForDataExportEntryRemoved(entry);
            } catch (InterruptedException ex) {
                throw new IOException(ex);
            }
        }
    }
    
    public synchronized void updateDataExportEntry(DataExportEntry entry) throws IOException {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataExportEntryStore();
        
        this.dataExportEntryStore.put(entry.getStargatePath(), entry);
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
        
        try {
            raiseEventForDataExportEntryUpdated(entry);
        } catch (InterruptedException ex) {
            throw new IOException(ex);
        }
    }
    
    public synchronized void addDataExportEventHandler(AbstractDataExportEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        this.dataExportEventHandlers.add(eventHandler);
    }
    
    public synchronized void removeDataExportEventHandler(AbstractDataExportEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        this.dataExportEventHandlers.remove(eventHandler);
    }

    private void raiseEventForDataExportEntryAdded(DataExportEntry entry) throws InterruptedException {
        DataExportEvent dataExportEntry = new DataExportEvent(DataExportEventType.DATAEXPORT_EVENT_TYPE_ADD, entry);
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_DATAEXPORT, dataExportEntry);
            eventManager.raiseStargateEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
        }
    }
    
    private void raiseEventForDataExportEntryRemoved(DataExportEntry entry) throws InterruptedException {
        DataExportEvent dataExportEntry = new DataExportEvent(DataExportEventType.DATAEXPORT_EVENT_TYPE_REMOVE, entry);
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_DATAEXPORT, dataExportEntry);
            eventManager.raiseStargateEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
        }
    }
    
    private void raiseEventForDataExportEntryUpdated(DataExportEntry entry) throws InterruptedException {
        DataExportEvent dataExportEntry = new DataExportEvent(DataExportEventType.DATAEXPORT_EVENT_TYPE_UPDATE, entry);
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_DATAEXPORT, dataExportEntry);
            eventManager.raiseStargateEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
        }
    }
    
    private void processDataExportEntryEvent(DataExportEvent event) {
        DataExportEntry entry = event.getEntry();
        switch(event.getEventType()) {
            case DATAEXPORT_EVENT_TYPE_ADD:
                LOG.debug("data export entry is added : " + entry.getStargatePath());
                for(AbstractDataExportEventHandler handler: this.dataExportEventHandlers) {
                    handler.added(this, entry);
                }
                break;
            case DATAEXPORT_EVENT_TYPE_REMOVE:
                LOG.debug("data export entry is removed : " + entry.getStargatePath());
                for(AbstractDataExportEventHandler handler: this.dataExportEventHandlers) {
                    handler.removed(this, entry);
                }
                break;
            case DATAEXPORT_EVENT_TYPE_UPDATE:
                LOG.debug("data export entry is updated : " + entry.getStargatePath());
                for(AbstractDataExportEventHandler handler: this.dataExportEventHandlers) {
                    handler.updated(this, entry);
                }
                break;
            default:
                LOG.error(String.format("cannot handle %s", event.getEventType().name()));
                break;
        }
    }
    
    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
}
