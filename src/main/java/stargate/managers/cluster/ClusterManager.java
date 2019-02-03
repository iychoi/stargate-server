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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.AbstractClusterDriver;
import stargate.commons.cluster.AbstractLocalClusterEventHandler;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.cluster.NodeStatus;
import stargate.commons.driver.AbstractDriver;
import stargate.commons.driver.DriverFailedToLoadException;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerConfig;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.utils.DateTimeUtils;
import stargate.managers.datastore.DataStoreManager;
import stargate.managers.policy.ClusterPolicy;
import stargate.managers.policy.PolicyManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class ClusterManager extends AbstractManager<AbstractClusterDriver> {
    
    private static final Log LOG = LogFactory.getLog(ClusterManager.class);
    
    private static ClusterManager instance;
    
    // store cluster information
    private ClusterPolicy policy;
    private Node localNode;
    private Cluster localCluster;
    private AbstractKeyValueStore remoteClusterStore;
    private List<AbstractRemoteClusterEventHandler> remoteClusterEventHandlers = new ArrayList<AbstractRemoteClusterEventHandler>();
    protected long lastUpdateTime;
    
    private static final String REMOTE_CLUSTER_STORE = "rcluster";
    
    public static ClusterManager getInstance(StargateService service, Collection<AbstractClusterDriver> drivers) throws ManagerNotInstantiatedException {
        synchronized (ClusterManager.class) {
            if(instance == null) {
                instance = new ClusterManager(service, drivers);
            }
            return instance;
        }
    }
    
    public static ClusterManager getInstance(StargateService service, ManagerConfig config) throws ManagerNotInstantiatedException {
        synchronized (ClusterManager.class) {
            if(instance == null) {
                if(config == null) {
                    throw new IllegalArgumentException("config is null");
                }
                
                try {
                    // type cast
                    Collection<AbstractDriver> drivers = (Collection<AbstractDriver>) config.getDrivers();
                    List<AbstractClusterDriver> clusterDrivers = new ArrayList<AbstractClusterDriver>();
                    for(AbstractDriver driver : drivers) {
                        clusterDrivers.add((AbstractClusterDriver) driver);
                    }
                    instance = new ClusterManager(service, clusterDrivers);
                } catch (DriverFailedToLoadException ex) {
                    LOG.error(ex);
                    throw new ManagerNotInstantiatedException(ex.toString());
                }
            }
            return instance;
        }
    }
    
    public static ClusterManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (ClusterManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("ClusterManager is not started");
            }
            return instance;
        }
    }
    
    ClusterManager(StargateService service, Collection<AbstractClusterDriver> drivers) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(drivers == null || drivers.isEmpty()) {
            throw new IllegalArgumentException("drivers is null or empty");
        }
        
        this.setService(service);
        
        for(AbstractClusterDriver driver : drivers) {
            this.drivers.add(driver);
        }
    }
    
    public AbstractClusterDriver getDriver() {
        if(this.drivers.size() > 0) {
            return this.drivers.get(0);
        }
        return null;
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
        this.remoteClusterEventHandlers.clear();
        
        super.stop();
    }
    
    private synchronized void safeInitLocalCluster() throws IOException {
        AbstractClusterDriver driver = getDriver();
        
        long currentTime = DateTimeUtils.getTimestamp();
        if(this.localCluster == null) {
            this.localCluster = driver.getLocalCluster();
            this.lastUpdateTime = currentTime;
        }
        
        if(this.localNode == null) {
            this.localNode = driver.getLocalNode();
            this.lastUpdateTime = currentTime;
        }
    }
    
    private synchronized void safeInitRemoteClusterStore() throws IOException {
        if(this.remoteClusterStore == null) {
            try {
                StargateService stargateService = getStargateService();
                DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();

                this.remoteClusterStore = keyValueStoreManager.getDriver().getKeyValueStore(REMOTE_CLUSTER_STORE, Cluster.class, EnumDataStoreProperty.DATASTORE_PROP_PERSISTENT_REPLICATED);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
    
    private synchronized void safeInitClusterPolicy() throws IOException {
        if(this.policy == null) {
            try {
                StargateService stargateService = getStargateService();
                PolicyManager policyManager = stargateService.getPolicyManager();
                this.policy = policyManager.getClusterPolicy();
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
    
    public synchronized Node getLeaderNode() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalCluster();
        
        AbstractClusterDriver driver = getDriver();
        String leaderNodeName = driver.getLeaderNodeName();
        
        return this.localCluster.getNode(leaderNodeName); 
    }
    
    public synchronized boolean isLeaderNode() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalCluster();
        
        AbstractClusterDriver driver = getDriver();
        return driver.isLeaderNode();
    }
    
    public synchronized Node getLocalNode() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalCluster();
        
        return this.localNode;
    }
    
    public synchronized Cluster getLocalCluster() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalCluster();
        
        return this.localCluster;
    }
    
    public synchronized String getLocalClusterName() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalCluster();
        
        return this.localCluster.getName();
    }
    
    public boolean isLocalNode(String name) throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalCluster();
        
        return this.localCluster.hasNode(name);
    }
    
    public synchronized Collection<Cluster> getRemoteClusters() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        List<Cluster> remoteClusters = new ArrayList<Cluster>();
        Map<String, Object> remoteClusterMap = this.remoteClusterStore.toMap();
        Set<Map.Entry<String, Object>> entrySet = remoteClusterMap.entrySet();
        for(Map.Entry<String, Object> entry : entrySet) {
            Cluster cluster = (Cluster) entry.getValue();
            if(cluster != null) {
                remoteClusters.add(cluster);
            }
        }
        
        // less efficient implementation
        //Collection<String> keys = this.remoteClusterStore.keys();
        //for(String key : keys) {
        //    Cluster cluster = (Cluster) this.remoteClusterStore.get(key);
        //    if(cluster != null) {
        //        remoteClusters.add(cluster);
        //    }
        //}
        
        return Collections.unmodifiableCollection(remoteClusters);
    }
    
    public synchronized Collection<String> getRemoteClusterNames() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        List<String> remoteClusterNames = new ArrayList<String>();
        Collection<String> keys = this.remoteClusterStore.keys();
        remoteClusterNames.addAll(keys);
        
        return Collections.unmodifiableCollection(remoteClusterNames);
    }
    
    public synchronized Cluster getRemoteCluster(String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        return (Cluster) this.remoteClusterStore.get(name);
    }
    
    public synchronized boolean hasRemoteCluster(String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        return this.remoteClusterStore.containsKey(name);
    }
    
    public synchronized void clearRemoteClusters() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        Collection<String> keys = this.remoteClusterStore.keys();
        for(String key : keys) {
            // this is to raise a cluster removal event
            removeRemoteCluster(key);
        }
    }
    
    public synchronized void addRemoteClusters(Collection<Cluster> clusters) throws ClusterManagerException, IOException {
        if(clusters == null) {
            throw new IllegalArgumentException("clusters is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        List<Cluster> failed = new ArrayList<Cluster>();
        
        for(Cluster cluster : clusters) {
            try {
                addRemoteCluster(cluster);
            } catch(ClusterManagerException ex) {
                failed.add(cluster);
            }
        }
        
        if(!failed.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for(Cluster cluster : failed) {
                if(sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(cluster.getName());
            }
            throw new ClusterManagerException("clusters (" + sb.toString() + ") cannot be added (maybe already exist?)");
        }
    }
    
    public synchronized void addRemoteCluster(Cluster cluster) throws ClusterManagerException, IOException {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        if(this.remoteClusterStore.containsKey(cluster.getName())) {
            throw new ClusterManagerException("cluster " + cluster.getName() + " is already added");
        }
        
        this.remoteClusterStore.put(cluster.getName(), cluster);
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
        
        raiseEventForRemoteClusterAdded(cluster);
    }
    
    public synchronized void removeRemoteCluster(Cluster cluster) throws IOException {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        removeRemoteCluster(cluster.getName());
    }
    
    public synchronized void removeRemoteCluster(String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        Cluster cluster = (Cluster) this.remoteClusterStore.get(name);
        if(cluster != null) {
            this.remoteClusterStore.remove(name);
            
            this.lastUpdateTime = DateTimeUtils.getTimestamp();

            raiseEventForRemoteClusterRemoved(cluster);
        }
    }
    
    public synchronized void updateRemoteClusters(Collection<Cluster> clusters) throws IOException {
        if(clusters == null) {
            throw new IllegalArgumentException("clusters is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        for(Cluster cluster : clusters) {
            updateRemoteCluster(cluster);
        }
    }
    
    public synchronized void updateRemoteCluster(Cluster cluster) throws IOException {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        this.remoteClusterStore.put(cluster.getName(), cluster);
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
        
        raiseEventForRemoteClusterUpdated(cluster);
    }
    
    public synchronized void addLocalClusterEventHandler(AbstractLocalClusterEventHandler eventHandler) {
        AbstractClusterDriver driver = getDriver();
        driver.addLocalClusterEventHandler(eventHandler);
    }
    
    public synchronized void removeLocalClusterEventHandler(AbstractLocalClusterEventHandler eventHandler) {
        AbstractClusterDriver driver = getDriver();
        driver.removeLocalClusterEventHandler(eventHandler);
    }
    
    public synchronized void addRemoteClusterEventHandler(AbstractRemoteClusterEventHandler eventHandler) {
        this.remoteClusterEventHandlers.add(eventHandler);
    }
    
    public synchronized void removeRemoteClusterEventHandler(AbstractRemoteClusterEventHandler eventHandler) {
        this.remoteClusterEventHandlers.remove(eventHandler);
    }

    private synchronized void raiseEventForRemoteClusterAdded(Cluster cluster) {
        LOG.debug("remote cluster is added : " + cluster.getName());
        
        for(AbstractRemoteClusterEventHandler handler: this.remoteClusterEventHandlers) {
            handler.added(cluster);
        }
    }
    
    private synchronized void raiseEventForRemoteClusterRemoved(Cluster cluster) {
        LOG.debug("remote cluster is removed : " + cluster.getName());
        
        for(AbstractRemoteClusterEventHandler handler: this.remoteClusterEventHandlers) {
            handler.removed(cluster);
        }
    }
    
    private synchronized void raiseEventForRemoteClusterUpdated(Cluster cluster) {
        LOG.debug("remote cluster is updated : " + cluster.getName());
        
        for(AbstractRemoteClusterEventHandler handler: this.remoteClusterEventHandlers) {
            handler.updated(cluster);
        }
    }
    
    public synchronized void reportLocalNodeUnreachable(String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }

        safeInitLocalCluster();
        safeInitClusterPolicy();
        
        Node node = this.localCluster.getNode(name);
        if(node != null) {
            long currentTime = DateTimeUtils.getTimestamp();
            NodeStatus status = node.getStatus();

            if(DateTimeUtils.timeElapsedSec(status.getLastFailureTime(), currentTime, this.policy.getNodeFailureReportIntervalSec())) {
                status.increaseFailureCount(true);

                if(!status.isBlacklisted()) {
                    if(status.getFailureCount() >= this.policy.getNumFailuresToBeBlacklisted()) {
                        status.setBlacklisted(true);
                    }
                }

                this.lastUpdateTime = currentTime;
            }
        }
    }
    
    public synchronized void reportRemoteNodeUnreachable(String clusterName, String nodeName) throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitClusterPolicy();
        safeInitRemoteClusterStore();
        
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }

        if(nodeName == null || nodeName.isEmpty()) {
            throw new IllegalArgumentException("nodeName is null or empty");
        }
        
        Cluster cluster = (Cluster) this.remoteClusterStore.get(clusterName);
        if(cluster != null) {
            Node node = cluster.getNode(nodeName);
            if(node != null) {
                long currentTime = DateTimeUtils.getTimestamp();
                NodeStatus status = node.getStatus();

                if(DateTimeUtils.timeElapsedSec(status.getLastFailureTime(), currentTime, this.policy.getNodeFailureReportIntervalSec())) {
                    status.increaseFailureCount(true);

                    if(!status.isBlacklisted()) {
                        if(status.getFailureCount() >= this.policy.getNumFailuresToBeBlacklisted()) {
                            status.setBlacklisted(true);
                        }
                    }

                    this.lastUpdateTime = currentTime;

                    // update
                    this.remoteClusterStore.put(clusterName, cluster);
                }
            }
        }
    }

    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
}
