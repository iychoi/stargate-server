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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.service.AbstractService;
import stargate.managers.transport.TransportManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class RoundRobinContactNodeSelectionAlgorithm extends AbstractContactNodeSelectionAlgorithm {
    
    private static final Log LOG = LogFactory.getLog(RoundRobinContactNodeSelectionAlgorithm.class);
    
    private Map<String, ResponsibleNodeMapping> responsibleNodeMappings = new HashMap<String, ResponsibleNodeMapping>();
    
    public RoundRobinContactNodeSelectionAlgorithm(AbstractService service, TransportManager manager) {
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
    
    public RoundRobinContactNodeSelectionAlgorithm(StargateService service, TransportManager manager) {
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
    
    @Override
    public Node getResponsibleRemoteNode(Cluster localCluster, Node localNode, Cluster remoteCluster) throws IOException {
        if(localCluster == null) {
            throw new IllegalArgumentException("localCluster is null");
        }
        
        if(localNode == null) {
            throw new IllegalArgumentException("localNode is null");
        }
        
        if(remoteCluster == null) {
            throw new IllegalArgumentException("remoteCluster is null");
        }
        
        if(remoteCluster.getNodeNum() == 0) {
            throw new IOException(String.format("There's no node in a remote cluster : %s", remoteCluster.getName()));
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
    public synchronized ResponsibleNodeMapping getResponsibleRemoteNodeMappings(Cluster localCluster, Cluster remoteCluster) throws IOException {
        ResponsibleNodeMapping mappings = this.responsibleNodeMappings.get(remoteCluster.getName());
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

            this.responsibleNodeMappings.put(remoteCluster.getName(), mappings);
        }

        return mappings;
    }
}
