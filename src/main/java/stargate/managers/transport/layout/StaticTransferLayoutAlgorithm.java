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
import java.util.Collection;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.datastore.AbstractBigKeyValueStore;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.service.AbstractService;
import stargate.commons.utils.StringUtils;
import stargate.managers.transport.TransportManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class StaticTransferLayoutAlgorithm extends AbstractTransferLayoutAlgorithm {
    
    private AbstractBigKeyValueStore dataCacheStore;
    
    public StaticTransferLayoutAlgorithm(AbstractService service, TransportManager manager, AbstractBigKeyValueStore dataCacheStore) {
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
    }
    
    public StaticTransferLayoutAlgorithm(StargateService service, TransportManager manager, AbstractBigKeyValueStore dataCacheStore) {
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
    }
    
    @Override
    public void increaseNodeWorkload(Cluster cluster, Node node) throws IOException {
        // noop
    }
    
    @Override
    public void decreaseNodeWorkload(Cluster cluster, Node node) throws IOException {
        // noop
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
        if(nodeIDs.isEmpty()) {
            throw new IOException(String.format("There is no node for hash %s", hash));
        }
        
        Collection<String> nodeNames = recipe.getNodeNames(nodeIDs);
        if(nodeNames.isEmpty()) {
            throw new IOException(String.format("Cannot get node names for node IDs: %s", StringUtils.getCommaSeparatedString(nodeIDs)));
        }
        
        for(String nodeName : nodeNames) {
            Node remoteNode = remoteCluster.getNode(nodeName);
            // we return very first node who has the chunk
            return remoteNode;
        }
        
        // ERROR
        throw new IOException(String.format("Cannot determine remote node for hash %s, nodeNames %s", hash, StringUtils.getCommaSeparatedString(nodeNames)));
    }
}
