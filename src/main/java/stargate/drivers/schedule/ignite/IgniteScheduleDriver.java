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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.lang.IgniteCallable;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.schedule.AbstractScheduleDriver;
import stargate.commons.schedule.AbstractScheduleDriverConfig;
import stargate.commons.schedule.Task;
import stargate.commons.schedule.TaskSchedule;
import stargate.drivers.ignite.IgniteDriver;
import stargate.managers.schedule.ScheduleManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class IgniteScheduleDriver extends AbstractScheduleDriver {

    private static final Log LOG = LogFactory.getLog(IgniteScheduleDriver.class);
    
    public static class IgniteCallableWrapper implements IgniteCallable {
        private Callable callable;
        
        IgniteCallableWrapper() {
        }
        
        public IgniteCallableWrapper(Callable callable) {
            this.callable = callable;
        }
        
        @Override
        public Object call() throws Exception {
            return this.callable.call();
        }
    }
    
    private IgniteScheduleDriverConfig config;
    private IgniteDriver igniteDriver;
    private Map<String, UUID> clusterNodeNameMappings = new HashMap<String, UUID>(); // name to ID mappings
    
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
        
        LOG.info("Initializing Ignite Schedule Driver");
        
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
    
    private ScheduleManager getScheduleManager() {
        if(this.manager == null) {
            throw new IllegalStateException("manager is not initialized");
        }
        
        return (ScheduleManager) this.manager;
    }
    
    private StargateService getStargateService() {
        ScheduleManager scheduleManager = getScheduleManager();
        StargateService stargateService = (StargateService) scheduleManager.getService();
        return stargateService;
    }
    
    @Override
    public void scheduleTask(Task task) throws IOException {
        if(task == null) {
            throw new IllegalArgumentException("task is null");
        }
        
        if(task.getNodeNames().isEmpty()) {
            throw new IllegalArgumentException("nodenames is empty");
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
            if(this.clusterNodeNameMappings.isEmpty()) {
                // fill mapping
                for(ClusterNode node : servers.nodes()) {
                    String consistentNodeName = node.consistentId().toString();
                    this.clusterNodeNameMappings.put(consistentNodeName, node.id());
                }
            }
            
            List<UUID> nodeIDs = new ArrayList<UUID>();
            for(String nodeName : nodeNames) {
                UUID nodeID = this.clusterNodeNameMappings.get(nodeName);
                if(nodeID == null) {
                    // check if there is a missing node
                    boolean foundNode = false;
                    for(ClusterNode node : servers.nodes()) {
                        String consistentNodeName = node.consistentId().toString();
                        if(nodeName.equals(consistentNodeName)) {
                            //found
                            foundNode = true;
                            this.clusterNodeNameMappings.put(consistentNodeName, node.id());
                            nodeIDs.add(node.id());
                            break;
                        }
                    }
                    
                    if(!foundNode) {
                        // error
                        throw new IOException(String.format("cannot find matching node : %s", nodeName));
                    }
                } else {
                    nodeIDs.add(nodeID);
                }
            }
            return servers.forNodeIds(nodeIDs);
        }
    }
    
    private IgniteCallable makeIgniteCallable(Task task) throws Exception {
        Callable c = task.getCallable();
        IgniteCallable igniteCallable = null;
        
        boolean repeat = false;
        long intervalMin = 0;
        long delaySec = 0;
        if(task instanceof TaskSchedule) {
            TaskSchedule ts = (TaskSchedule) task;
            delaySec = Math.max(ts.getDelay(), 1); // in sec
            intervalMin = Math.max(ts.getInterval() / 60, 1); // in min
            repeat = ts.isRepeat();
        }
        
        if(repeat) {
            // ignite does not support interval-based task scheduling.
            // so we convert the interval-based schedule to a coarse cron style
            String min = "*";
            String hour = "*";
            if(intervalMin <= 30) {
                min = String.format("*/%d", intervalMin);
                hour = "*";
            } else if(intervalMin < 60*12) {
                long intervalHour = Math.max(intervalMin / 60, 1);
                min = "0";
                hour = String.format("*/%d", intervalHour);
            } else {
                min = "0";
                hour = "0";
            }
            
            String extendedCronSyntax = String.format("{%d, %d} %s %s * * *", delaySec, 0, min, hour);
            /*            
            igniteRunnable = new IgniteRunnableWrapper(r, extendedCronSyntax) {
                @IgniteInstanceResource
                Ignite ignite;
                
                @Override
                public void run() {
                    ignite.scheduler().scheduleLocal(r, extendedCronSyntax);
                }
            };
            */
        } else {
            igniteCallable = new IgniteCallableWrapper(c);
        }
        
        return igniteCallable;
    }
}
