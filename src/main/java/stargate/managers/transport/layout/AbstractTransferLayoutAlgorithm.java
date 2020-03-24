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
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.datastore.AbstractBigKeyValueStore;
import stargate.commons.recipe.Recipe;
import stargate.commons.service.AbstractService;
import stargate.managers.transport.TransportManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public abstract class AbstractTransferLayoutAlgorithm {

    protected StargateService service;
    protected TransportManager manager;
    protected AbstractBigKeyValueStore dataCacheStore;
    protected AbstractContactNodeSelectionAlgorithm contactNodeSelectionAlgorithm;
    
    public void setService(AbstractService service) {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(!(service instanceof StargateService)) {
            throw new IllegalArgumentException("service is not an instance of StargateService");
        }
        
        this.service = (StargateService) service;
    }
    
    public void setStargateService(StargateService service) {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        this.service = service;
    }
    
    public StargateService getStargateService() {
        return this.service;
    }
    
    public void setManager(TransportManager manager) {
        if(manager == null) {
            throw new IllegalArgumentException("manager is null");
        }
        
        this.manager = manager;
    }
    
    public TransportManager getManager() {
        return this.manager;
    }
    
    public void setDataCacheStore(AbstractBigKeyValueStore dataCacheStore) {
        if(dataCacheStore == null) {
            throw new IllegalArgumentException("dataCacheStore is null");
        }
        
        this.dataCacheStore = dataCacheStore;
    }
    
    public AbstractBigKeyValueStore getDataCacheStore() {
        return this.dataCacheStore;
    }
    
    public void setContactNodeAlgorithm(AbstractContactNodeSelectionAlgorithm contactNodeSelectionAlgorithm) {
        if(contactNodeSelectionAlgorithm == null) {
            throw new IllegalArgumentException("contactNodeSelectionAlgorithm is null");
        }
        
        this.contactNodeSelectionAlgorithm = contactNodeSelectionAlgorithm;
    }
    
    public AbstractContactNodeSelectionAlgorithm getContactNodeAlgorithm() {
        return this.contactNodeSelectionAlgorithm;
    }
    
    public abstract void increaseNodeWorkload(Cluster cluster, Node node) throws IOException;
    public abstract void decreaseNodeWorkload(Cluster cluster, Node node) throws IOException;
    
    public abstract Node determineLocalNode(Cluster cluster, Recipe recipe, String hash) throws IOException;
    public abstract Node determineRemoteNode(Cluster localCluster, Node localNode, Cluster remoteCluster, Recipe recipe, String hash) throws IOException;
}
