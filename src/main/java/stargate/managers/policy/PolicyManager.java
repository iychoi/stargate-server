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
import stargate.commons.keyvaluestore.AbstractKeyValueStore;
import stargate.commons.keyvaluestore.EnumKeyValueStoreProperty;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.utils.DateTimeUtils;
import stargate.managers.keyvaluestore.KeyValueStoreManager;
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
    private List<AbstractPolicyEventHandler> policyEventHandlers = new ArrayList<AbstractPolicyEventHandler>();
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
    
    private synchronized void safeInitPolicyStore() throws IOException {
        if(this.policyStore == null) {
            try {
                StargateService stargateService = getStargateService();
                KeyValueStoreManager keyValueStoreManager = stargateService.getKeyValueStoreManager();
                this.policyStore = keyValueStoreManager.getDriver().getKeyValueStore(POLICY_STORE, DataExportEntry.class, EnumKeyValueStoreProperty.KEY_VALUE_STORE_PROP_PERSISTENT_REPLICATED);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
    
    public synchronized Object get(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitPolicyStore();
        
        return this.policyStore.get(key);
    }
    
    public synchronized void put(String key, Object value) throws IOException {
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
        
        this.policyStore.put(key, value);
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
        
        raiseEventForPolicyUpdated(key, value);
    }
    
    public synchronized ClusterPolicy getClusterPolicy(boolean local) throws IOException {
        ClusterPolicy policy = getClusterPolicy();
        
        if(local) {
            ClusterPolicy clone = policy.clone();
            clone.makeLocal();
            return clone;
        } else {
            return policy;
        }
    }
    
    public synchronized ClusterPolicy getClusterPolicy() throws IOException {
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
    
    public synchronized VolumePolicy getVolumePolicy(boolean local) throws IOException {
        VolumePolicy policy = getVolumePolicy();
        
        if(local) {
            VolumePolicy clone = policy.clone();
            clone.makeLocal();
            return clone;
        } else {
            return policy;
        }
    }
    
    public synchronized VolumePolicy getVolumePolicy() throws IOException {
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
    
    public synchronized void addPolicyEventHandler(AbstractPolicyEventHandler eventHandler) {
        this.policyEventHandlers.add(eventHandler);
    }
    
    public synchronized void removePolicyEventHandler(AbstractPolicyEventHandler eventHandler) {
        this.policyEventHandlers.remove(eventHandler);
    }

    synchronized void raiseEventForPolicyUpdated(String key, Object value) {
        LOG.debug(String.format("policy is updated : key=%s, value=%s", key, String.valueOf(value)));
        
        for(AbstractPolicyEventHandler handler: this.policyEventHandlers) {
            handler.updated(this, key, value);
        }
    }

    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
}
