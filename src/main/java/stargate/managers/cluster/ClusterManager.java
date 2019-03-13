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
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerConfig;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.utils.DateTimeUtils;
import stargate.managers.datastore.DataStoreManager;
import stargate.commons.event.AbstractEventHandler;
import stargate.managers.event.EventManager;
import stargate.commons.event.StargateEvent;
import stargate.commons.event.StargateEventType;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class ClusterManager extends AbstractManager<AbstractClusterDriver> {
    
    private static final Log LOG = LogFactory.getLog(ClusterManager.class);
    
    private static final int DEFAULT_NODE_FAILURE_REPORT_INTERVAL_SEC = 60*5;
    private static final int DEFAULT_NUM_FAILURES_TO_BE_BLACKLISTED = 5;
    
    private static ClusterManager instance;
    
    private int nodeFailureReportIntervalSec = DEFAULT_NODE_FAILURE_REPORT_INTERVAL_SEC;
    private int numFailuresToBeBlacklisted = DEFAULT_NUM_FAILURES_TO_BE_BLACKLISTED;
    
    // store cluster information
    private Node localNode;
    private Cluster localCluster;
    private AbstractKeyValueStore remoteClusterStore;
    private final Object remoteClusterStoreSyncObj = new Object();
    private List<AbstractRemoteClusterEventHandler> remoteClusterEventHandlers = new ArrayList<AbstractRemoteClusterEventHandler>();
    private final Object remoteClusterEventHandlersSyncObj = new Object();
    protected long lastUpdateTime;
    
    private static final String REMOTE_CLUSTER_STORE = "remote_cluster";
    
    public static ClusterManager getInstance(StargateService service, ManagerConfig config, Collection<AbstractClusterDriver> drivers) throws ManagerNotInstantiatedException {
        synchronized (ClusterManager.class) {
            if(instance == null) {
                instance = new ClusterManager(service, config, drivers);
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
                    instance = new ClusterManager(service, config, clusterDrivers);
                } catch (DriverFailedToLoadException ex) {
                    LOG.error("Could not load driver", ex);
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
    
    ClusterManager(StargateService service, ManagerConfig config, Collection<AbstractClusterDriver> drivers) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(drivers == null || drivers.isEmpty()) {
            throw new IllegalArgumentException("drivers is null or empty");
        }
        
        this.setService(service);
        this.setConfig(config);
        
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
        
        setEventHandler();
    }
    
    @Override
    public synchronized void stop() throws IOException {
        synchronized(this.remoteClusterEventHandlers) {
            this.remoteClusterEventHandlers.clear();
        }
        
        super.stop();
    }
    
    private void setEventHandler() throws IOException {
        AbstractEventHandler hander = new AbstractEventHandler() {
            private final StargateEventType[] acceptedEventTypes = {StargateEventType.STARGATE_EVENT_TYPE_REMOTECLUSTER};
                    
            @Override
            public StargateEventType[] getAcceptedTypes() {
                return this.acceptedEventTypes;
            }
            
            @Override
            public void raised(StargateEvent event) {
                String jsonValue = event.getJsonValue();
                try {
                    RemoteClusterEvent evt = RemoteClusterEvent.createInstance(jsonValue);
                    processRemoteClusterEvent(evt);
                } catch (IOException ex) {
                    LOG.error("IOException", ex);
                }
            }
        };
        
        StargateService stargateService = getStargateService();
        stargateService.addEventHandler(hander);
    }
    
    private void safeInitLocalCluster() throws IOException {
        try {
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
        } catch (DriverNotInitializedException ex) {
            LOG.error("Driver is not initialized", ex);
            throw new IOException(ex);
        }
    }
    
    private void safeInitRemoteClusterStore() throws IOException {
        synchronized(this.remoteClusterStoreSyncObj) {
            if(this.remoteClusterStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();

                    this.remoteClusterStore = keyValueStoreManager.getDriver().getKeyValueStore(REMOTE_CLUSTER_STORE, Cluster.class, EnumDataStoreProperty.DATASTORE_PROP_PERSISTENT_REPLICATED);
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
    
    public synchronized Node getLeaderNode() throws IOException, DriverNotInitializedException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalCluster();
        
        AbstractClusterDriver driver = getDriver();
        String leaderNodeName = driver.getLeaderNodeName();
        
        return this.localCluster.getNode(leaderNodeName); 
    }
    
    public synchronized boolean isLeaderNode() throws IOException, DriverNotInitializedException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalCluster();
        
        AbstractClusterDriver driver = getDriver();
        return driver.isLeaderNode();
    }
    
    public synchronized Node getLocalNode() throws IOException, DriverNotInitializedException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalCluster();
        
        return this.localNode;
    }
    
    public synchronized Cluster getLocalCluster() throws IOException, DriverNotInitializedException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalCluster();
        
        return this.localCluster;
    }
    
    public synchronized String getLocalClusterName() throws IOException, DriverNotInitializedException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalCluster();
        
        return this.localCluster.getName();
    }
    
    public synchronized boolean isLocalNode(String name) throws IOException, DriverNotInitializedException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalCluster();
        
        return this.localCluster.hasNode(name);
    }
    
    public Collection<Cluster> getRemoteClusters() throws IOException, DriverNotInitializedException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        synchronized(this.remoteClusterStoreSyncObj) {
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
    }
    
    public Collection<String> getRemoteClusterNames() throws IOException, DriverNotInitializedException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        synchronized(this.remoteClusterStoreSyncObj) {
            List<String> remoteClusterNames = new ArrayList<String>();
            Collection<String> keys = this.remoteClusterStore.keys();
            remoteClusterNames.addAll(keys);

            return Collections.unmodifiableCollection(remoteClusterNames);
        }
    }
    
    public Cluster getRemoteCluster(String name) throws IOException, DriverNotInitializedException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        synchronized(this.remoteClusterStoreSyncObj) {
            return (Cluster) this.remoteClusterStore.get(name);
        }
    }
    
    public boolean hasRemoteCluster(String name) throws IOException, DriverNotInitializedException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        synchronized(this.remoteClusterStoreSyncObj) {
            return this.remoteClusterStore.containsKey(name);
        }
    }
    
    public void clearRemoteClusters() throws IOException, DriverNotInitializedException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        synchronized(this.remoteClusterStoreSyncObj) {
            Collection<String> keys = this.remoteClusterStore.keys();
            for(String key : keys) {
                // this is to raise a cluster removal event
                removeRemoteCluster(key);
            }
        }
    }
    
    public void addRemoteClusters(Collection<Cluster> clusters) throws ClusterManagerException, IOException, DriverNotInitializedException {
        if(clusters == null) {
            throw new IllegalArgumentException("clusters is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        List<Cluster> failed = new ArrayList<Cluster>();
        
        synchronized(this.remoteClusterStoreSyncObj) {
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
    }
    
    public void addRemoteCluster(Cluster cluster) throws ClusterManagerException, IOException, DriverNotInitializedException {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        synchronized(this.remoteClusterStoreSyncObj) {
            if(this.remoteClusterStore.containsKey(cluster.getName())) {
                throw new ClusterManagerException("cluster " + cluster.getName() + " is already added");
            }

            LOG.debug(String.format("Adding a new remote cluster : %s", cluster.getName()));

            this.remoteClusterStore.put(cluster.getName(), cluster);

            this.lastUpdateTime = DateTimeUtils.getTimestamp();

            raiseEventForRemoteClusterAdded(cluster);
        }
    }
    
    public void removeRemoteCluster(Cluster cluster) throws IOException, DriverNotInitializedException {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        synchronized(this.remoteClusterStoreSyncObj) {
            removeRemoteCluster(cluster.getName());
        }
    }
    
    public void removeRemoteCluster(String name) throws IOException, DriverNotInitializedException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        synchronized(this.remoteClusterStoreSyncObj) {
            Cluster cluster = (Cluster) this.remoteClusterStore.get(name);
            if(cluster != null) {
                LOG.debug(String.format("Removing a remote cluster : %s", name));

                this.remoteClusterStore.remove(name);

                this.lastUpdateTime = DateTimeUtils.getTimestamp();

                raiseEventForRemoteClusterRemoved(cluster);
            }
        }
    }
    
    public void updateRemoteClusters(Collection<Cluster> clusters) throws IOException, DriverNotInitializedException {
        if(clusters == null) {
            throw new IllegalArgumentException("clusters is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        synchronized(this.remoteClusterStoreSyncObj) {
            for(Cluster cluster : clusters) {
                updateRemoteCluster(cluster);
            }
        }
    }
    
    public void updateRemoteCluster(Cluster cluster) throws IOException, DriverNotInitializedException {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        LOG.debug(String.format("Updating a remote cluster : %s", cluster.getName()));
        
        synchronized(this.remoteClusterStoreSyncObj) {
            this.remoteClusterStore.put(cluster.getName(), cluster);

            this.lastUpdateTime = DateTimeUtils.getTimestamp();

            raiseEventForRemoteClusterUpdated(cluster);
        }
    }
    
    public void addLocalClusterEventHandler(AbstractLocalClusterEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        AbstractClusterDriver driver = getDriver();
        driver.addLocalClusterEventHandler(eventHandler);
    }
    
    public void removeLocalClusterEventHandler(AbstractLocalClusterEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        AbstractClusterDriver driver = getDriver();
        driver.removeLocalClusterEventHandler(eventHandler);
    }
    
    public void addRemoteClusterEventHandler(AbstractRemoteClusterEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        synchronized(this.remoteClusterEventHandlersSyncObj) {
            this.remoteClusterEventHandlers.add(eventHandler);
        }
    }
    
    public void removeRemoteClusterEventHandler(AbstractRemoteClusterEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        synchronized(this.remoteClusterEventHandlersSyncObj) {
            this.remoteClusterEventHandlers.remove(eventHandler);
        }
    }

    private void raiseEventForRemoteClusterAdded(Cluster cluster) throws IOException, DriverNotInitializedException {
        RemoteClusterEvent remoteClusterEvent = new RemoteClusterEvent(RemoteClusterEventType.REMOTECLUSTER_EVENT_TYPE_ADD, cluster);
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Cluster localCluster = clusterManager.getLocalCluster();
            Node localNode = clusterManager.getLocalNode();
            Collection<String> nodeNames = localCluster.getNodeNames();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_REMOTECLUSTER, nodeNames, localNode.getName(), remoteClusterEvent.toJson());
            eventManager.raiseEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
        }
    }
    
    private void raiseEventForRemoteClusterRemoved(Cluster cluster) throws IOException, DriverNotInitializedException {
        RemoteClusterEvent remoteClusterEvent = new RemoteClusterEvent(RemoteClusterEventType.REMOTECLUSTER_EVENT_TYPE_REMOVE, cluster);
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Cluster localCluster = clusterManager.getLocalCluster();
            Node localNode = clusterManager.getLocalNode();
            Collection<String> nodeNames = localCluster.getNodeNames();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_REMOTECLUSTER, nodeNames, localNode.getName(), remoteClusterEvent.toJson());
            eventManager.raiseEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
        }
    }
    
    private void raiseEventForRemoteClusterUpdated(Cluster cluster) throws IOException, DriverNotInitializedException {
        RemoteClusterEvent remoteClusterEvent = new RemoteClusterEvent(RemoteClusterEventType.REMOTECLUSTER_EVENT_TYPE_UPDATE, cluster);
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Cluster localCluster = clusterManager.getLocalCluster();
            Node localNode = clusterManager.getLocalNode();
            Collection<String> nodeNames = localCluster.getNodeNames();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_REMOTECLUSTER, nodeNames, localNode.getName(), remoteClusterEvent.toJson());
            eventManager.raiseEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
        }
    }
    
    public synchronized void reportLocalNodeUnreachable(String name) throws IOException, DriverNotInitializedException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }

        safeInitLocalCluster();
        
        Node node = this.localCluster.getNode(name);
        if(node != null) {
            long currentTime = DateTimeUtils.getTimestamp();
            NodeStatus status = node.getStatus();

            if(DateTimeUtils.timeElapsedSec(status.getLastFailureTime(), currentTime, this.nodeFailureReportIntervalSec)) {
                status.increaseFailureCount(true);

                if(!status.isBlacklisted()) {
                    if(status.getFailureCount() >= this.numFailuresToBeBlacklisted) {
                        status.setBlacklisted(true);
                    }
                }

                this.lastUpdateTime = currentTime;
            }
        }
    }
    
    public void reportRemoteNodeUnreachable(String clusterName, String nodeName) throws IOException, DriverNotInitializedException {
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }

        if(nodeName == null || nodeName.isEmpty()) {
            throw new IllegalArgumentException("nodeName is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteClusterStore();
        
        synchronized(this.remoteClusterStoreSyncObj) {
            Cluster cluster = (Cluster) this.remoteClusterStore.get(clusterName);
            if(cluster != null) {
                Node node = cluster.getNode(nodeName);
                if(node != null) {
                    long currentTime = DateTimeUtils.getTimestamp();
                    NodeStatus status = node.getStatus();

                    if(DateTimeUtils.timeElapsedSec(status.getLastFailureTime(), currentTime, this.nodeFailureReportIntervalSec)) {
                        status.increaseFailureCount(true);

                        if(!status.isBlacklisted()) {
                            if(status.getFailureCount() >= this.numFailuresToBeBlacklisted) {
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
    }
    
    private void processRemoteClusterEvent(RemoteClusterEvent event) {
        Cluster cluster = event.getCluster();
        switch(event.getEventType()) {
            case REMOTECLUSTER_EVENT_TYPE_ADD:
                LOG.debug(String.format("remote cluster is added : %s", cluster.getName()));
                synchronized(this.remoteClusterEventHandlersSyncObj) {
                    for(AbstractRemoteClusterEventHandler handler: this.remoteClusterEventHandlers) {
                        handler.added(this, cluster);
                    }
                }
                break;
            case REMOTECLUSTER_EVENT_TYPE_REMOVE:
                LOG.debug(String.format("remote cluster is removed : %s", cluster.getName()));
                synchronized(this.remoteClusterEventHandlersSyncObj) {
                    for(AbstractRemoteClusterEventHandler handler: this.remoteClusterEventHandlers) {
                        handler.removed(this, cluster);
                    }
                }
                break;
            case REMOTECLUSTER_EVENT_TYPE_UPDATE:
                LOG.debug(String.format("remote cluster is updated : %s", cluster.getName()));
                synchronized(this.remoteClusterEventHandlersSyncObj) {
                    for(AbstractRemoteClusterEventHandler handler: this.remoteClusterEventHandlers) {
                        handler.updated(this, cluster);
                    }
                }
                break;
            default:
                LOG.error(String.format("cannot handle %s", event.getEventType().name()));
                break;
        }
    }

    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public void setLastUpdateTime(long time) {
        if(time < 0) {
            throw new IllegalArgumentException("time is negative");
        }
        
        this.lastUpdateTime = time;
    }
}
