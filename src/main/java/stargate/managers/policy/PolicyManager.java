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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.driver.NullDriver;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.utils.DateTimeUtils;
import stargate.managers.datastore.DataStoreManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class PolicyManager extends AbstractManager<NullDriver> {
    
    private static final Log LOG = LogFactory.getLog(PolicyManager.class);
    
    private static PolicyManager instance;
    
    private ClusterPolicy clusterPolicy;
    private VolumePolicy volumePolicy;
    private AbstractKeyValueStore policyStore;
    private final Object policyStoreSyncObj = new Object();
    private List<AbstractPolicyEventHandler> policyEventHandlers = new ArrayList<AbstractPolicyEventHandler>();
    private final Object policyEventHandlersSyncObj = new Object();
    protected long lastUpdateTime;
    
    private static final String POLICY_STORE = "policy";
    
    public static PolicyManager getInstance(StargateService service) throws ManagerNotInstantiatedException {
        synchronized (PolicyManager.class) {
            if(instance == null) {
                instance = new PolicyManager(service);
            }
            return instance;
        }
    }
    
    public static PolicyManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (PolicyManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("DataExportManager is not started");
            }
            return instance;
        }
    }
    
    PolicyManager(StargateService service) throws ManagerNotInstantiatedException {
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
    }
    
    @Override
    public synchronized void stop() throws IOException {
        this.policyEventHandlers.clear();
        
        super.stop();
    }
    
    private void safeInitPolicyStore() throws IOException {
        synchronized(this.policyStoreSyncObj) {
            if(this.policyStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    this.policyStore = keyValueStoreManager.getDriver().getKeyValueStore(POLICY_STORE, DataExportEntry.class, EnumDataStoreProperty.DATASTORE_PROP_PERSISTENT_REPLICATED);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error("Manager is not instantiated", ex);
                    throw new IOException(ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error("Driver is not initialized", ex);
                    throw new IOException(ex);
                }
            }
        }
    }
    
    public Object get(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitPolicyStore();
        
        synchronized(this.policyStoreSyncObj) {
            return this.policyStore.get(key);
        }
    }
    
    public void put(String key, Object value) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(value == null) {
            throw new IllegalArgumentException("value is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitPolicyStore();
        
        synchronized(this.policyStoreSyncObj) {
            this.policyStore.put(key, value);
        
            this.lastUpdateTime = DateTimeUtils.getTimestamp();

            raiseEventForPolicyUpdated(key, value);
        }
    }
    
    public ClusterPolicy getClusterPolicy(boolean local) throws IOException {
        ClusterPolicy policy = getClusterPolicy();
        
        if(local) {
            ClusterPolicy clone = policy.clone();
            clone.makeLocal();
            return clone;
        } else {
            return policy;
        }
    }
    
    public ClusterPolicy getClusterPolicy() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitPolicyStore();
        
        if(this.clusterPolicy == null) {
            this.clusterPolicy = new ClusterPolicy();
            this.clusterPolicy.setManager(this);
        }
        
        return this.clusterPolicy;
    }
    
    public VolumePolicy getVolumePolicy(boolean local) throws IOException {
        VolumePolicy policy = getVolumePolicy();
        
        if(local) {
            VolumePolicy clone = policy.clone();
            clone.makeLocal();
            return clone;
        } else {
            return policy;
        }
    }
    
    public VolumePolicy getVolumePolicy() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitPolicyStore();
        
        if(this.volumePolicy == null) {
            this.volumePolicy = new VolumePolicy();
            this.volumePolicy.setManager(this);
        }
        
        return this.volumePolicy;
    }
    
    public void addPolicyEventHandler(AbstractPolicyEventHandler eventHandler) {
        synchronized(this.policyEventHandlersSyncObj) {
            this.policyEventHandlers.add(eventHandler);
        }
    }
    
    public void removePolicyEventHandler(AbstractPolicyEventHandler eventHandler) {
        synchronized(this.policyEventHandlersSyncObj) {
            this.policyEventHandlers.remove(eventHandler);
        }
    }

    private synchronized void raiseEventForPolicyUpdated(String key, Object value) {
        LOG.debug(String.format("policy is updated : key=%s, value=%s", key, String.valueOf(value)));
        
        synchronized(this.policyEventHandlersSyncObj) {
            for(AbstractPolicyEventHandler handler: this.policyEventHandlers) {
                handler.updated(this, key, value);
            }
        }
    }

    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
}
