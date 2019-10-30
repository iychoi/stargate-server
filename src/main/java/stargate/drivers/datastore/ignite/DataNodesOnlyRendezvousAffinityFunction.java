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
package stargate.drivers.datastore.ignite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;

/**
 *
 * @author iychoi
 */
public class DataNodesOnlyRendezvousAffinityFunction extends RendezvousAffinityFunction {
    
    private static final Log LOG = LogFactory.getLog(DataNodesOnlyRendezvousAffinityFunction.class);
    
    private List<UUID> excludeNodes = new ArrayList<UUID>();
    
    public DataNodesOnlyRendezvousAffinityFunction() {
        super();
    }
    
    public void execludeNode(ClusterNode node) {
        LOG.debug(String.format("Execluding a node from affinity - %s", node.id()));
        this.excludeNodes.add(node.id());
    }
    
    @Override
    public List<ClusterNode> assignPartition(int part, List<ClusterNode> nodes, int backups, Map<UUID, Collection<ClusterNode>> neighborhoodCache) {
        List<ClusterNode> new_nodes = new ArrayList<ClusterNode>();
        
        for(ClusterNode node : nodes) {
            if(!this.excludeNodes.contains(node.id())) {
                new_nodes.add(node);
            }
        }
        
        return super.assignPartition(part, new_nodes, backups, null);
    }
    
    @Override
    public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        int parts = this.getPartitions();
        List<List<ClusterNode>> assignments = new ArrayList<>(parts);

        List<ClusterNode> nodes = affCtx.currentTopologySnapshot();
        
        List<ClusterNode> new_nodes = new ArrayList<ClusterNode>();
        for(ClusterNode node : nodes) {
            if(!this.excludeNodes.contains(node.id())) {
                new_nodes.add(node);
            }
        }
        
        for (int i = 0; i < parts; i++) {
            List<ClusterNode> partAssignment = super.assignPartition(i, new_nodes, affCtx.backups(), null);

            assignments.add(partAssignment);
        }

        return assignments;
    }
}
