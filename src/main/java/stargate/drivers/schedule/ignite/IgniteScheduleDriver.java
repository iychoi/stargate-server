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
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Node;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.schedule.AbstractScheduleDriver;
import stargate.commons.schedule.AbstractScheduleDriverConfig;
import stargate.commons.schedule.AbstractScheduledTask;

/**
 *
 * @author iychoi
 */
public class IgniteScheduleDriver extends AbstractScheduleDriver {

    private static final Log LOG = LogFactory.getLog(IgniteScheduleDriver.class);
    
    private IgniteScheduleDriverConfig config;
    
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
        
        LOG.info("Initializing Ignite Cluster Driver");
    }

    @Override
    public synchronized void uninit() throws IOException {
        super.uninit();
    }
    
    @Override
    public void addTask(Collection<Node> nodes, AbstractScheduledTask task) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
