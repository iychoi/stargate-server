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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteDataNodeFilter implements IgnitePredicate<ClusterNode> {

    private static final Log LOG = LogFactory.getLog(IgniteDataNodeFilter.class);
    
    private List<String> acceptedNodeNames = new ArrayList<String>();
    
    public IgniteDataNodeFilter(Collection<String> dataNodeNames) {
        this.acceptedNodeNames.addAll(dataNodeNames);
    }
    
    @Override
    public boolean apply(ClusterNode clusterNode) {
        try {
            IgniteDriver igniteLocalDriver = IgniteDriver.getInstanceIfInitialized();
            String nodeName = igniteLocalDriver.getNodeNameFromClusterNode(clusterNode);
            if(acceptedNodeNames.contains(nodeName)) {
                LOG.info(String.format("Accept node %s", nodeName));
                return true;
            }
            LOG.info(String.format("Filter node %s", nodeName));
            return false;
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }
}
