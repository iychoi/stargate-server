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
import stargate.commons.cluster.AbstractClusterDriver;
import stargate.commons.cluster.AbstractClusterDriverConfig;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.cluster.NodeStatus;
import stargate.commons.driver.AbstractDriverConfig;
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
    
    private static final String LOCAL_CLUSTER_NODE_STORE = "LOCAL_CLUSTER_NODES";
    
    private IgniteClusterDriverConfig config;
    private IgniteDriver igniteDriver;
    private IgniteCache<String, String> nodes;
    private Node localNode;
    
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
        IgniteDriver.setStorageRootPath(this.config.getStorageRootPath());
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
    }

    @Override
    public synchronized void uninit() throws IOException {
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
    
    private synchronized void safeInitNodeStore() throws IOException {
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
    }
    
    private synchronized Node makeLocalNode() throws IOException {
        Ignite ignite = this.igniteDriver.getIgnite();
        
        IgniteCluster igniteCluster = ignite.cluster();
        ClusterNode localNode = igniteCluster.localNode();

        String nodeName = localNode.consistentId().toString();
        String clusterName = this.config.getClusterName();
        Set<String> hostnames = new HashSet<String>();
        Collection<String> igniteHostNames = localNode.hostNames();
        Collection<String> localHostNames = IPUtils.getHostNames();
        hostnames.addAll(igniteHostNames);
        for(String hostname : localHostNames) {
            if(!hostnames.contains(hostname)) {
                hostnames.add(hostname);
            }
        }
        
        try {    
            StargateService stargateService = getStargateService();
            TransportManager transportManager = stargateService.getTransportManager();
            TransportServiceInfo transportServiceInfo = transportManager.getServiceInfo();
            
            UserInterfaceManager userInterfaceManager = stargateService.getUserInterfaceManager();
            UserInterfaceServiceInfo userInterfaceServiceInfo = userInterfaceManager.getServiceInfo();
            
            Node stargateNode = new Node(nodeName, clusterName, new NodeStatus(), transportServiceInfo, userInterfaceServiceInfo, hostnames);
            LOG.debug(String.format("Local node - %s", stargateNode.toJson()));
            return stargateNode;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    // this must not be synchronized because this function is called while 
    // init() is blocked
    @Override
    public void activateCluster() throws IOException {
        this.igniteDriver.activate();
    }
    
    @Override
    public boolean isClusterActive() throws IOException {
        return this.igniteDriver.isActive();
    }
    
    @Override
    public synchronized Node getLocalNode() throws IOException {
        safeInitNodeStore();
        
        if(this.localNode == null) {
            this.localNode = makeLocalNode();
            this.nodes.put(this.localNode.getName(), this.localNode.toJson());
        }
        return this.localNode;
    }

    @Override
    public synchronized Cluster getLocalCluster() throws IOException {
        safeInitNodeStore();
        
        if(this.localNode == null) {
            this.localNode = makeLocalNode();
            this.nodes.put(this.localNode.getName(), this.localNode.toJson());
        }
        
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
    public synchronized String getLeaderNodeName() throws IOException {
        Ignite ignite = this.igniteDriver.getIgnite();
        IgniteCluster cluster = ignite.cluster();
        
        // Dynamic cluster group representing the oldest cluster node.
        // Will automatically shift to the next oldest, if the oldest
        // node crashes.
        ClusterGroup oldestNode = cluster.forOldest();
        ClusterNode node = oldestNode.node();
        return node.id().toString();
    }

    @Override
    public boolean isLeaderNode() throws IOException {
        String leaderNodeName = getLeaderNodeName();
        Node localNode = getLocalNode();
        
        return localNode.getName().equals(leaderNodeName);
    }
}
