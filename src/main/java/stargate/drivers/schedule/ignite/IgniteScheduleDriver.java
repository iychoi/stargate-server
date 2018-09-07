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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.schedule.AbstractScheduleDriver;
import stargate.commons.schedule.AbstractScheduleDriverConfig;
import stargate.commons.schedule.Task;
import stargate.drivers.ignite.IgniteDriver;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.schedule.ScheduleManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class IgniteScheduleDriver extends AbstractScheduleDriver {

    private static final Log LOG = LogFactory.getLog(IgniteScheduleDriver.class);
    
    private static final String TASK_QUEUE = "TASK_QUEUE";
    
    private IgniteScheduleDriverConfig config;
    private IgniteDriver igniteDriver;
    private IgniteQueue<String> tasks;
    private boolean dispatchTask = true;
    private Thread taskDispatcherThread;
    
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
        this.dispatchTask = false;
        if(this.taskDispatcherThread != null) {
            if(this.taskDispatcherThread.isAlive()) {
                this.taskDispatcherThread.interrupt();
            }
            this.taskDispatcherThread = null;
        }
        
        if(this.tasks != null) {
            this.tasks.clear();
            this.tasks.close();
            this.tasks = null;
        }
        
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
    
    private synchronized void safeInitTaskQueues() throws IOException {
        if(this.tasks == null) {
            Ignite ignite = this.igniteDriver.getIgnite();
            CollectionConfiguration cc = new CollectionConfiguration();
            cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            cc.setBackups(0);
            cc.setCollocated(true);
            
            this.tasks = ignite.queue(TASK_QUEUE, 0, cc);
        }
    }
    
    @Override
    public void scheduleTask(Task task) throws IOException {
        safeInitTaskQueues();
        
        if(task == null) {
            throw new IllegalArgumentException("task is null");
        }

        this.tasks.put(task.toJson());
    }
    
    private synchronized void processTask(Task task) {
        
    }
    
    private void runTaskDispatcher() {
        this.dispatchTask = true;
        this.taskDispatcherThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while(dispatchTask) {
                        try {
                            StargateService stargateService = getStargateService();
                            ClusterManager clusterManager = stargateService.getClusterManager();
                            if(clusterManager.isLeaderNode()) {
                                // only leader node dispatches tasks
                                String taskJson = tasks.take();
                                Task task = Task.createInstance(taskJson);
                                
                                processTask(task);
                            }
                        } catch (IOException ex) {
                            LOG.error(ex);
                        } catch (ManagerNotInstantiatedException ex) {
                            LOG.error(ex);
                        }
                        
                        Thread.sleep(1000);
                    }
                } catch (InterruptedException ex) {
                    LOG.error(ex);
                }
            }
        });
        this.taskDispatcherThread.start();
    }
}
