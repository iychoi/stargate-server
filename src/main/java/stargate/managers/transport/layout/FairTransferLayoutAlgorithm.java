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
package stargate.managers.transport.layout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.event.AbstractEventHandler;
import stargate.commons.event.StargateEvent;
import stargate.commons.event.StargateEventType;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.service.AbstractService;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.event.EventManager;
import stargate.managers.transport.TransportManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class FairTransferLayoutAlgorithm extends AbstractTransferLayoutAlgorithm {
    
    private static final Log LOG = LogFactory.getLog(FairTransferLayoutAlgorithm.class);
    
    private static final int CLUSTER_WORKLOADS_MAX_ENTRY_SIZE = 100;
    
    private AbstractKeyValueStore dataCacheStore;
    private Map<String, ClusterWorkload> clusterWorkloads = new LRUMap<String, ClusterWorkload>(CLUSTER_WORKLOADS_MAX_ENTRY_SIZE);
    private Map<String, ClusterWorkload> clusterWorkloadDelta = new LRUMap<String, ClusterWorkload>(CLUSTER_WORKLOADS_MAX_ENTRY_SIZE);
    
    private static final double WORKLOAD_INCREMENT = 10;
    
    public FairTransferLayoutAlgorithm(AbstractService service, TransportManager manager, AbstractKeyValueStore dataCacheStore) throws IOException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(manager == null) {
            throw new IllegalArgumentException("manager is null");
        }
        
        if(dataCacheStore == null) {
            throw new IllegalArgumentException("dataCacheStore is null");
        }
        
        if(!(service instanceof StargateService)) {
            throw new IllegalArgumentException("service is not an instance of StargateService");
        }
        
        if(!service.isStarted()) {
            throw new IllegalArgumentException("service is not started");
        }
        
        if(!manager.isStarted()) {
            throw new IllegalArgumentException("manager is not started");
        }
        
        this.service = (StargateService) service;
        this.manager = manager;
        
        this.dataCacheStore = dataCacheStore;
        
        setEventHandler();
    }
    
    public FairTransferLayoutAlgorithm(StargateService service, TransportManager manager, AbstractKeyValueStore dataCacheStore) throws IOException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(manager == null) {
            throw new IllegalArgumentException("manager is null");
        }
        
        if(dataCacheStore == null) {
            throw new IllegalArgumentException("dataCacheStore is null");
        }
        
        if(!service.isStarted()) {
            throw new IllegalArgumentException("service is not started");
        }
        
        if(!manager.isStarted()) {
            throw new IllegalArgumentException("manager is not started");
        }
        
        this.service = service;
        this.manager = manager;
        
        this.dataCacheStore = dataCacheStore;
        
        setEventHandler();
    }
    
    private void setEventHandler() throws IOException {
        AbstractEventHandler hander = new AbstractEventHandler() {
            private final StargateEventType[] acceptedEventTypes = {StargateEventType.STARGATE_EVENT_TYPE_TRANSFER_WORKLOAD};
                    
            @Override
            public StargateEventType[] getAcceptedTypes() {
                return this.acceptedEventTypes;
            }

            @Override
            public void raised(StargateEvent event) {
                String jsonValue = event.getJsonValue();
                try {
                    TransferWorkloadEvent evt = TransferWorkloadEvent.createInstance(jsonValue);
                    processTransferWorkloadEvent(event.getSenderNodeName(), evt);
                } catch (IOException ex) {
                    LOG.error("IOException", ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error("Driver is not initialized", ex);
                }
            }
        };
        
        StargateService stargateService = getStargateService();
        stargateService.addEventHandler(hander);
    }
    
    private synchronized void applyWorkloadDelta(ClusterWorkload clusterWorkloadDelta) {
        ClusterWorkload existingClusterWorkload = this.clusterWorkloads.get(clusterWorkloadDelta.getClusterName());
        if(existingClusterWorkload == null) {
            // not exist
            this.clusterWorkloads.put(clusterWorkloadDelta.getClusterName(), clusterWorkloadDelta);
        } else {
            // exist
            existingClusterWorkload.increaseClusterWorkload(clusterWorkloadDelta);
        }
    }
    
    private synchronized void applyWorkloadDelta(String clusterName, NodeWorkload nodeWorkloadDelta) {
        ClusterWorkload existingClusterWorkload = this.clusterWorkloads.get(clusterName);
        if(existingClusterWorkload == null) {
            // not exist
            ClusterWorkload clusterWorkload = new ClusterWorkload(clusterName);
            clusterWorkload.increaseNodeWorkload(nodeWorkloadDelta);
            
            this.clusterWorkloads.put(clusterName, clusterWorkload);
        } else {
            // exist
            existingClusterWorkload.increaseNodeWorkload(nodeWorkloadDelta);
        }
    }
    
    private synchronized void syncWorkloadDelta(ClusterWorkload clusterWorkload) throws IOException, DriverNotInitializedException {
        // apply to local node
        applyWorkloadDelta(clusterWorkload);

        // send to remote node
        raiseEventForWorkloadSync(clusterWorkload);
        
        this.clusterWorkloadDelta.remove(clusterWorkload.getClusterName());
    }
    
    private synchronized void syncWorkloadDelta(String clusterName, NodeWorkload nodeWorkload) throws IOException, DriverNotInitializedException {
        // apply to local node
        applyWorkloadDelta(clusterName, nodeWorkload);

        // send to remote node
        ClusterWorkload clusterWorkload = new ClusterWorkload(clusterName);
        clusterWorkload.setNodeWorkload(nodeWorkload);
        
        raiseEventForWorkloadSync(clusterWorkload);
        
        ClusterWorkload existingClusterWorkload = this.clusterWorkloadDelta.get(clusterName);
        if(existingClusterWorkload != null) {
            existingClusterWorkload.clearNodeWorkload(nodeWorkload.getNodeName());
        }
    }
    
    private synchronized ClusterWorkload getCurrentNodeWorkloads(Cluster cluster) throws IOException {
        ClusterWorkload currentWorkload = new ClusterWorkload(cluster.getName());
        
        ClusterWorkload workload = this.clusterWorkloads.get(cluster.getName());
        ClusterWorkload delta = this.clusterWorkloadDelta.get(cluster.getName());
        
        if(workload != null) {
            currentWorkload.increaseClusterWorkload(workload);
        }
        
        if(delta != null) {
            currentWorkload.increaseClusterWorkload(delta);
        }
        
        return currentWorkload;
    }
    
    @Override
    public void increaseNodeWorkload(Cluster cluster, Node node) throws IOException {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        if(node == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        ClusterWorkload clusterWorkload = this.clusterWorkloadDelta.get(cluster.getName());
        if(clusterWorkload == null) {
            clusterWorkload = new ClusterWorkload(cluster.getName());
            this.clusterWorkloadDelta.put(cluster.getName(), clusterWorkload);
        }
        
        NodeWorkload nodeWorkload = clusterWorkload.getNodeWorkloadWithDefault(node.getName());
        nodeWorkload.increaseWorkload(WORKLOAD_INCREMENT);
        
        try {
            syncWorkloadDelta(cluster.getName(), nodeWorkload);
        } catch (DriverNotInitializedException ex) {
            LOG.error("Driver is not initialized", ex);
            throw new IOException(ex);
        }
    }
    
    @Override
    public void decreaseNodeWorkload(Cluster cluster, Node node) throws IOException {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        if(node == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        ClusterWorkload clusterWorkload = this.clusterWorkloadDelta.get(cluster.getName());
        if(clusterWorkload == null) {
            clusterWorkload = new ClusterWorkload(cluster.getName());
            this.clusterWorkloadDelta.put(cluster.getName(), clusterWorkload);
        }
        
        NodeWorkload nodeWorkload = clusterWorkload.getNodeWorkloadWithDefault(node.getName());
        nodeWorkload.increaseWorkload(-1 * WORKLOAD_INCREMENT);
        
        try {
            syncWorkloadDelta(cluster.getName(), nodeWorkload);
        } catch (DriverNotInitializedException ex) {
            LOG.error("Driver is not initialized", ex);
            throw new IOException(ex);
        }
    }
    
    @Override
    public Node determineLocalNode(Cluster cluster, Recipe recipe, String hash) throws IOException {
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null");
        }
        
        // at this point there must be a pending cache
        // use always the master
        String primaryNodeName = this.dataCacheStore.getPrimaryNodeForData(hash);
        Node node = cluster.getNode(primaryNodeName);
        return node;
    }
    
    @Override
    public Node determineRemoteNode(Cluster remoteCluster, Recipe recipe, String hash) throws IOException {
        if(remoteCluster == null) {
            throw new IllegalArgumentException("remoteCluster is null");
        }
        
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        RecipeChunk chunk = recipe.getChunk(hash);
        Collection<Integer> nodeIDs = chunk.getNodeIDs();
        Collection<String> nodeNames = recipe.getNodeNames(nodeIDs);

        ClusterWorkload clusterWorkload = getCurrentNodeWorkloads(remoteCluster);
        
        NodeWorkload lowestWorkload = null;
        
        List<String> shuffledNodeNames = new ArrayList<String>();
        shuffledNodeNames.addAll(nodeNames);
        
        // this is to give a chance equally to nodes which have the lowest workload
        Collections.shuffle(shuffledNodeNames);
        
        for(String nodeName : shuffledNodeNames) {
            NodeWorkload nodeWorkload = clusterWorkload.getNodeWorkloadWithDefault(nodeName);
            
            if(lowestWorkload == null) {
                lowestWorkload = nodeWorkload;
            } else {
                if(lowestWorkload.getWorkload() > nodeWorkload.getWorkload()) {
                    lowestWorkload = nodeWorkload;
                }
            }
        }
        
        String nodeName = lowestWorkload.getNodeName();
        return remoteCluster.getNode(nodeName);
    }
    
    private void raiseEventForWorkloadSync(ClusterWorkload clusterWorkloadDelta) throws IOException, DriverNotInitializedException {
        TransferWorkloadEvent transferWorkloadEvent = new TransferWorkloadEvent(TransferWorkloadEventType.TRANSFER_WORKLOAD_EVENT_TYPE_SYNC_DELTA, clusterWorkloadDelta);
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            
            ClusterManager clusterManager = stargateService.getClusterManager();
            Node localNode = clusterManager.getLocalNode();
            Cluster localCluster = clusterManager.getLocalCluster();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_TRANSFER_WORKLOAD, localCluster.getNodeNames(), localNode.getName(), transferWorkloadEvent.toJson());
            eventManager.raiseEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
        }
    }
    
    private void processTransferWorkloadEvent(String senderNodeName, TransferWorkloadEvent event) throws IOException, DriverNotInitializedException {
        switch(event.getEventType()) {
            case TRANSFER_WORKLOAD_EVENT_TYPE_SYNC_DELTA:
                try {
                    StargateService stargateService = getStargateService();

                    ClusterManager clusterManager = stargateService.getClusterManager();
                    
                    if(!clusterManager.isLocalNode(senderNodeName)) {
                        // local events are immediately processed
                        ClusterWorkload workload = event.getWorkload();
                        LOG.debug(String.format("A workload sync is requested : %s", workload.toString()));
                        applyWorkloadDelta(workload);
                    }
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error("Manager is not instantiated", ex);
                }
                break;
            default:
                LOG.error(String.format("cannot handle %s", event.getEventType().name()));
                break;
        }
    }
}
