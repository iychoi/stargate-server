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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import stargate.commons.utils.StringUtils;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteAffinityFunction implements AffinityFunction, Serializable {
    
    private static final Log LOG = LogFactory.getLog(IgniteAffinityFunction.class);
    
    private static List<String> nonDataNodes = new ArrayList<String>(); // IP address or domain name
    
    private RendezvousAffinityFunction baseAffinityFunction;
    private AffinityTopologyVersion cachedTopologyVersion;
    private List<List<ClusterNode>> cachedPartitions = new ArrayList<List<ClusterNode>>();
    
    public static void setNonDataNodes(Collection<String> nodes) {
        LOG.info(String.format("Set non-data nodes - %s", StringUtils.getCommaSeparatedString(nodes)));
        
        nonDataNodes.clear();
        nonDataNodes.addAll(nodes);
    }
    
    public IgniteAffinityFunction() {
        this.baseAffinityFunction = new RendezvousAffinityFunction();
    }
    
    @Override
    public void reset() {
        this.baseAffinityFunction.reset();
    }

    public int partitions() {
        return this.baseAffinityFunction.partitions();
    }

    @Override
    public int partition(Object key) {
        return this.baseAffinityFunction.partition(key);
    }

    @Override
    public void removeNode(UUID uuid) {
        this.baseAffinityFunction.removeNode(uuid);
    }
    
    @Override
    public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
        IgniteDriver igniteDriver;
        try {
            igniteDriver = IgniteDriver.getInstanceIfInitialized();
            
            int parts = this.baseAffinityFunction.partitions();
            List<List<ClusterNode>> assignments = new ArrayList<>(parts);
            
            AffinityTopologyVersion currentTopologyVersion = affCtx.currentTopologyVersion();
            if(this.cachedTopologyVersion != currentTopologyVersion) {
                this.cachedPartitions.clear();

                List<ClusterNode> nodes = affCtx.currentTopologySnapshot();
                this.cachedTopologyVersion = affCtx.currentTopologyVersion();

                List<ClusterNode> new_nodes = new ArrayList<ClusterNode>();
                for(ClusterNode node : nodes) {
                    String nodeName = igniteDriver.getNodeNameFromClusterNode(node);
                    if(!nonDataNodes.contains(nodeName)) {
                        new_nodes.add(node);
                    } else {
                        LOG.info(String.format("Ignoring a node from partitioning - %s", nodeName));
                    }
                }
                
                for (int i = 0; i < parts; i++) {
                    List<ClusterNode> partAssignment = this.baseAffinityFunction.assignPartition(i, new_nodes, affCtx.backups(), null);
                    assignments.add(partAssignment);
                    this.cachedPartitions.add(partAssignment);
                }
            } else {
                assignments.addAll(this.cachedPartitions);
            }

            return assignments;
        } catch (IOException ex) {
            LOG.error(ex);
            throw new RuntimeException(ex);
        }
    }
}
