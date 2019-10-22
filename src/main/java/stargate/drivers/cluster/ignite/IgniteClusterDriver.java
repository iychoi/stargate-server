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
package stargate.drivers.cluster.ignite;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import javax.cache.Cache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import stargate.commons.cluster.AbstractClusterDriver;
import stargate.commons.cluster.AbstractClusterDriverConfig;
import stargate.commons.cluster.AbstractLocalClusterEventHandler;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.cluster.NodeStatus;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.transport.TransportServiceInfo;
import stargate.commons.userinterface.UserInterfaceServiceInfo;
import stargate.commons.utils.IPUtils;
import stargate.drivers.ignite.IgniteDriver;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.transport.TransportManager;
import stargate.managers.userinterface.UserInterfaceManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class IgniteClusterDriver extends AbstractClusterDriver {

    private static final Log LOG = LogFactory.getLog(IgniteClusterDriver.class);
    
    private IgniteClusterDriverConfig config;
    private IgniteDriver igniteDriver;
    private IgniteCache<String, String> nodes;
    private Node localNode;
    private boolean listenEvent = true;
    private Set<AbstractLocalClusterEventHandler> localClusterEventHandlers = new HashSet<AbstractLocalClusterEventHandler>();
    private final Object localClusterEventHandlersSyncObj = new Object();
    
    private static final String LOCAL_CLUSTER_NODE_STORE = "LOCAL_CLUSTER_NODES";
    
    public IgniteClusterDriver(AbstractDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof IgniteClusterDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of IgniteClusterDriverConfig");
        }
        
        this.config = (IgniteClusterDriverConfig) config;
        setIgniteDriver();
    }
    
    public IgniteClusterDriver(AbstractClusterDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof IgniteClusterDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of IgniteClusterDriverConfig");
        }
        
        this.config = (IgniteClusterDriverConfig) config;
        setIgniteDriver();
    }
    
    public IgniteClusterDriver(IgniteClusterDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
        setIgniteDriver();
    }
    
    private void setIgniteDriver() {
        IgniteDriver.setClusterName(this.config.getClusterName());
        IgniteDriver.setStorageRootPath(this.config.getStorageRootPath());
        IgniteDriver.setWorkPath(this.config.getWorkPath());
        IgniteDriver.addClusterNodes(this.config.getClusterNodes());
    }
    
    @Override
    public synchronized void init() throws IOException {
        super.init();
        
        LOG.debug("Initializing Ignite Cluster Driver");
        
        String clusterName = this.config.getClusterName();
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is not set");
        }
        
        this.igniteDriver = IgniteDriver.getInstance();
        this.igniteDriver.init();
        
        setEventHandler();
    }

    @Override
    public synchronized void uninit() throws IOException {
        this.listenEvent = false;
        this.localClusterEventHandlers.clear();
        
        
        if(this.localNode != null) {
            this.localNode = null;
        }
        
        if(this.nodes != null) {
            this.nodes.destroy();
            this.nodes = null;
        }
        
        if(this.igniteDriver != null && this.igniteDriver.isStarted()) {
            this.igniteDriver.uninit();
        }
        
        if(this.igniteDriver != null) {
            this.igniteDriver = null;
        }
        
        super.uninit();
    }
    
    private ClusterManager getClusterManager() {
        if(this.manager == null) {
            throw new IllegalStateException("manager is not initialized");
        }
        
        return (ClusterManager) this.manager;
    }
    
    private StargateService getStargateService() {
        ClusterManager clusterManager = getClusterManager();
        StargateService stargateService = (StargateService) clusterManager.getService();
        return stargateService;
    }
    
    private void safeInitNodeStore() throws IOException {
        if(this.nodes == null) {
            Ignite ignite = this.igniteDriver.getIgnite();
            CacheConfiguration<String, String> cc = new CacheConfiguration<String, String>();
            cc.setCacheMode(CacheMode.REPLICATED);
            cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            cc.setEvictionPolicy(null);
            cc.setCopyOnRead(false);
            cc.setOnheapCacheEnabled(true);
            cc.setReadFromBackup(true);
            cc.setDataRegionName(IgniteDriver.VOLATILE_REGION_NAME);
            cc.setName(LOCAL_CLUSTER_NODE_STORE);

            this.nodes = ignite.getOrCreateCache(cc);
        }
        
        if(this.localNode == null) {
            this.localNode = makeLocalNode();
            this.nodes.put(this.localNode.getName(), this.localNode.toJson());
        }
    }
    
    private void setEventHandler() throws IOException {
        this.listenEvent = true;
        
        Ignite ignite = this.igniteDriver.getIgnite();
        IgnitePredicate<DiscoveryEvent> ignitePrediceate = new IgnitePredicate<DiscoveryEvent>() {
            @Override
            public boolean apply(DiscoveryEvent event) {
                ClusterNode eventNode = event.eventNode();
                String eventNodeName = igniteDriver.getNodeNameFromClusterNode(eventNode);
                
                switch(event.type()) {
                    case EventType.EVT_NODE_JOINED:
                        raiseEventForLocalClusterNodeJoined(eventNodeName);
                        break;
                    case EventType.EVT_NODE_FAILED:
                        raiseEventForLocalClusterNodeFailed(eventNodeName);
                        break;
                    case EventType.EVT_NODE_LEFT:
                        raiseEventForLocalClusterNodeLeft(eventNodeName);
                        break;
                    default:
                        break;
                }
                // continue listening
                return listenEvent;
            }
        };
        
        ignite.events().localListen(ignitePrediceate, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_LEFT);
    }
    
    private Node makeLocalNode() throws IOException {
        String clusterName = this.config.getClusterName();
        String nodeName = this.igniteDriver.getLocalNodeName();
        
        Collection<String> nonDataNodes = this.config.getNonDataNodes();
        
        Set<String> hostnames = new HashSet<String>();
        
        Collection<String> igniteHostNames = this.igniteDriver.getLocalNodeHostNames();
        Collection<String> localHostNames = IPUtils.getHostNames();
        
        hostnames.addAll(igniteHostNames);
        hostnames.addAll(localHostNames);
        
        boolean dataNode = true;
        for(String nonDataNode : nonDataNodes) {
            if(hostnames.contains(nonDataNode)) {
                dataNode = false;
                break;
            }
        }
        
        try {
            StargateService stargateService = getStargateService();
            TransportManager transportManager = stargateService.getTransportManager();
            TransportServiceInfo transportServiceInfo = transportManager.getServiceInfo();
            
            UserInterfaceManager userInterfaceManager = stargateService.getUserInterfaceManager();
            UserInterfaceServiceInfo userInterfaceServiceInfo = userInterfaceManager.getServiceInfo();
            
            Node stargateNode = new Node(nodeName, clusterName, dataNode, new NodeStatus(), transportServiceInfo, userInterfaceServiceInfo, hostnames);
            LOG.debug(String.format("Local node - %s", stargateNode.toJson()));
            return stargateNode;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        } catch (DriverNotInitializedException ex) {
            LOG.error("Driver is not initialized", ex);
            throw new IOException(ex);
        }
    }
    
    // this must not be synchronized because this function is called while 
    // init() is blocked
    @Override
    public void activateCluster() throws IOException, DriverNotInitializedException {
        this.igniteDriver.activate();
    }
    
    @Override
    public boolean isClusterActive() throws IOException, DriverNotInitializedException {
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        return this.igniteDriver.isActive();
    }
    
    @Override
    public synchronized Node getLocalNode() throws IOException, DriverNotInitializedException {
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        safeInitNodeStore();
        
        return this.localNode;
    }

    @Override
    public synchronized Cluster getLocalCluster() throws IOException, DriverNotInitializedException {
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        safeInitNodeStore();
        
        String clusterName = this.config.getClusterName();
        Cluster stargateCluster = new Cluster(clusterName);
        
        Iterator<Cache.Entry<String, String>> iterator = this.nodes.iterator();
        while(iterator.hasNext()) {
            Cache.Entry<String, String> entry = iterator.next();
            String nodeJson = entry.getValue();
            Node node = Node.createInstance(nodeJson);
            
            stargateCluster.addOrUpdateNode(node, true);
        }
        
        return stargateCluster;
    }

    @Override
    public synchronized String getLeaderNodeName() throws IOException, DriverNotInitializedException {
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        Ignite ignite = this.igniteDriver.getIgnite();
        IgniteCluster cluster = ignite.cluster();
        
        // Dynamic cluster group representing the oldest cluster node.
        // Will automatically shift to the next oldest, if the oldest
        // node crashes.
        ClusterGroup oldestNode = cluster.forOldest();
        ClusterNode node = oldestNode.node();
        return this.igniteDriver.getNodeNameFromClusterNode(node);
    }

    @Override
    public boolean isLeaderNode() throws IOException, DriverNotInitializedException {
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        String leaderNodeName = getLeaderNodeName();
        Node localNode = getLocalNode();
        
        return localNode.getName().equals(leaderNodeName);
    }
    
    @Override
    public void addLocalClusterEventHandler(AbstractLocalClusterEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        synchronized(this.localClusterEventHandlersSyncObj) {
            this.localClusterEventHandlers.add(eventHandler);
        }
    }
    
    @Override
    public void removeLocalClusterEventHandler(AbstractLocalClusterEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        synchronized(this.localClusterEventHandlersSyncObj) {
            this.localClusterEventHandlers.remove(eventHandler);
        }
    }
    
    private void raiseEventForLocalClusterNodeJoined(String nodeName) {
        LOG.debug("local node joined : " + nodeName);
        
        synchronized(this.localClusterEventHandlersSyncObj) {
            for(AbstractLocalClusterEventHandler handler: this.localClusterEventHandlers) {
                handler.nodeJoined(nodeName);
            }
        }
    }
    
    private void raiseEventForLocalClusterNodeFailed(String nodeName) {
        LOG.debug("local node failed : " + nodeName);
        
        synchronized(this.localClusterEventHandlersSyncObj) {
            for(AbstractLocalClusterEventHandler handler: this.localClusterEventHandlers) {
                handler.nodeFailed(nodeName);
            }
        }
    }
    
    private void raiseEventForLocalClusterNodeLeft(String nodeName) {
        LOG.debug("local node left : " + nodeName);
        
        synchronized(this.localClusterEventHandlersSyncObj) {
            for(AbstractLocalClusterEventHandler handler: this.localClusterEventHandlers) {
                handler.nodeLeft(nodeName);
            }
        }
    }
}
