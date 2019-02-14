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
package stargate.drivers.schedule.ignite;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.lang.IgniteCallable;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.schedule.AbstractScheduleDriver;
import stargate.commons.schedule.AbstractScheduleDriverConfig;
import stargate.commons.schedule.DistributedTask;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteScheduleDriver extends AbstractScheduleDriver {

    private static final Log LOG = LogFactory.getLog(IgniteScheduleDriver.class);
    
    private IgniteScheduleDriverConfig config;
    private IgniteDriver igniteDriver;
    
    public IgniteScheduleDriver(AbstractDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof IgniteScheduleDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of IgniteScheduleDriverConfig");
        }
        
        this.config = (IgniteScheduleDriverConfig) config;
    }
    
    public IgniteScheduleDriver(AbstractScheduleDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof IgniteScheduleDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of IgniteScheduleDriverConfig");
        }
        
        this.config = (IgniteScheduleDriverConfig) config;
    }
    
    public IgniteScheduleDriver(IgniteScheduleDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void init() throws IOException {
        super.init();
        
        LOG.debug("Initializing Ignite Schedule Driver");
        
        this.igniteDriver = IgniteDriver.getInstance();
        this.igniteDriver.init();
    }

    @Override
    public synchronized void uninit() throws IOException {
        if(this.igniteDriver != null && this.igniteDriver.isStarted()) {
            this.igniteDriver.uninit();
        }
        
        if(this.igniteDriver != null) {
            this.igniteDriver = null;
        }
        
        super.uninit();
    }
    
    @Override
    public void scheduleTask(DistributedTask task) throws IOException, DriverNotInitializedException {
        if(task == null) {
            throw new IllegalArgumentException("task is null");
        }
        
        if(task.getNodeNames().isEmpty()) {
            throw new IllegalArgumentException("nodenames is empty");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }

        try {
            // process task!
            Ignite ignite = this.igniteDriver.getIgnite();
            ClusterGroup group = parseTaskGroup(task.getNodeNames());
            if(group.nodes() == null || group.nodes().isEmpty()) {
                throw new IOException("there's no target node");
            }
            
            IgniteCompute compute = ignite.compute(group).withAsync();
            compute.broadcast(makeIgniteCallable(task));
            ComputeTaskFuture future = compute.future();
            IgniteTaskFuture igniteTaskFuture = new IgniteTaskFuture(future);
            task.setFuture(igniteTaskFuture);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
    
    private ClusterGroup parseTaskGroup(Collection<String> nodeNames) throws IOException {
        boolean all = false;
        if(nodeNames.isEmpty()) {
            all = true;
        } else {
            for(String nodeName : nodeNames) {
                if(nodeName.equals("*")) {
                    all = true;
                    break;
                }
            }
        }
        
        Ignite ignite = this.igniteDriver.getIgnite();
        IgniteCluster cluster = ignite.cluster();
        ClusterGroup servers = cluster.forServers();
        
        if(all) {
            return servers;
        } else {
            List<UUID> nodeIDs = new ArrayList<UUID>();
            for(String nodeName : nodeNames) {
                UUID nodeID = this.igniteDriver.getClusterNodeIDFromName(nodeName);
                if(nodeID == null) {
                    throw new IllegalArgumentException(String.format("Cannot find a node for nodeName %s", nodeName));
                }
                nodeIDs.add(nodeID);
            }
            return servers.forNodeIds(nodeIDs);
        }
    }
    
    private IgniteCallable makeIgniteCallable(DistributedTask task) throws Exception {
        Callable c = task.getCallable();
        return new IgniteCallableWrapper(c);
    }
}
