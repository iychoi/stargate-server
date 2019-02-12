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
package stargate.managers.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.service.AbstractService;
import stargate.managers.datastore.DataStoreManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class RoundRobinContactNodeDeterminationAlgorithm extends AbstractContactNodeDeterminationAlgorithm {
    
    private static final Log LOG = LogFactory.getLog(RoundRobinContactNodeDeterminationAlgorithm.class);
    
    private AbstractKeyValueStore responsibleNodeMappingStore; // <String, ResponsibleNodeMapping>
    private final Object responsibleNodeMappingStoreSyncObj = new Object();
    
    private static final String RESPONSIBLE_NODE_MAPPING_STORE = "responsible_node_mapping";
    
    public RoundRobinContactNodeDeterminationAlgorithm(AbstractService service, TransportManager manager) {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(manager == null) {
            throw new IllegalArgumentException("manager is null");
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
    }
    
    public RoundRobinContactNodeDeterminationAlgorithm(StargateService service, TransportManager manager) {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(manager == null) {
            throw new IllegalArgumentException("manager is null");
        }
        
        if(!service.isStarted()) {
            throw new IllegalArgumentException("service is not started");
        }
        
        if(!manager.isStarted()) {
            throw new IllegalArgumentException("manager is not started");
        }
        
        this.service = service;
        this.manager = manager;
    }
    
    private void safeInitResponsibleNodeMappingStore() throws IOException {
        synchronized(this.responsibleNodeMappingStoreSyncObj) {
            if(this.responsibleNodeMappingStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    this.responsibleNodeMappingStore = keyValueStoreManager.getDriver().getKeyValueStore(RESPONSIBLE_NODE_MAPPING_STORE, ResponsibleNodeMapping.class, EnumDataStoreProperty.DATASTORE_PROP_VOLATILE_REPLICATED, TimeUnit.MINUTES, 5);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }
        }
    }
    
    @Override
    public Node getResponsibleRemoteNode(Cluster localCluster, Node localNode, Cluster remoteCluster) throws IOException {
        if(remoteCluster == null) {
            throw new IllegalArgumentException("remoteCluster is null");
        }
        
        ResponsibleNodeMapping responsibleRemoteNodeMappings = getResponsibleRemoteNodeMappings(localCluster, remoteCluster);

        NodeMapping mapping = responsibleRemoteNodeMappings.getNodeMapping(localNode.getName());
        Collection<String> targetNodeNames = mapping.getTargetNodeNames();

        for(String targetNodeName : targetNodeNames) {
            Node remoteNode = remoteCluster.getNode(targetNodeName);
            if(remoteNode != null) {
                return remoteNode;
            }
        }
        throw new IOException("Could not find a responsible remote node");
    }
    
    @Override
    public ResponsibleNodeMapping getResponsibleRemoteNodeMappings(Cluster localCluster, Cluster remoteCluster) throws IOException {
        if(remoteCluster == null) {
            throw new IllegalArgumentException("remoteCluster is null");
        }
        
        if(remoteCluster.getNodeNum() == 0) {
            throw new IOException(String.format("There's no node in a remote cluster : %s", remoteCluster.getName()));
        }
        
        safeInitResponsibleNodeMappingStore();
        
        synchronized(this.responsibleNodeMappingStoreSyncObj) {
            ResponsibleNodeMapping mappings = (ResponsibleNodeMapping) this.responsibleNodeMappingStore.get(remoteCluster.getName());
            if(mappings == null) {
                // make a new mapping
                List<Node> localNodes = new ArrayList<Node>();
                List<Node> remoteNodes = new ArrayList<Node>();

                localNodes.addAll(localCluster.getNodes());
                int localNodeNum = localNodes.size();
                remoteNodes.addAll(remoteCluster.getNodes());
                int remoteNodeNum = remoteNodes.size();

                // round-robin mapping
                mappings = new ResponsibleNodeMapping(remoteCluster.getName());

                if(localNodeNum >= remoteNodeNum) {
                    for(int i=0;i<localNodeNum;i++) {
                        Node localNode = localNodes.get(i);
                        Node remoteNode = remoteNodes.get(i % remoteNodeNum);

                        LOG.debug(String.format("Determined a responsible remote node of %s -> %s", localNode.getName(), remoteNode.getName()));

                        mappings.addNodeMapping(localNode.getName(), remoteNode.getName());
                    }
                } else {
                    for(int i=0;i<remoteNodeNum;i++) {
                        Node localNode = localNodes.get(i % localNodeNum);
                        Node remoteNode = remoteNodes.get(i);

                        LOG.debug(String.format("Determined a responsible remote node of %s -> %s", localNode.getName(), remoteNode.getName()));

                        mappings.addNodeMapping(localNode.getName(), remoteNode.getName());
                    }
                }

                this.responsibleNodeMappingStore.put(remoteCluster.getName(), mappings);
            }

            return mappings;
        }
    }
}
