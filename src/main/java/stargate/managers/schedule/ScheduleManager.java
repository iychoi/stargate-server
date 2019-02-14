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
package stargate.managers.schedule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.driver.AbstractDriver;
import stargate.commons.driver.DriverFailedToLoadException;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerConfig;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.schedule.AbstractScheduleDriver;
import stargate.commons.schedule.DistributedTask;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class ScheduleManager extends AbstractManager<AbstractScheduleDriver> {

    private static final Log LOG = LogFactory.getLog(ScheduleManager.class);
    
    private static ScheduleManager instance;
    
    private Timer timer;
    
    public static ScheduleManager getInstance(StargateService service, Collection<AbstractScheduleDriver> drivers) throws ManagerNotInstantiatedException {
        synchronized (ScheduleManager.class) {
            if(instance == null) {
                instance = new ScheduleManager(service, drivers);
            }
            return instance;
        }
    }
    
    public static ScheduleManager getInstance(StargateService service, ManagerConfig config) throws ManagerNotInstantiatedException {
        synchronized (ScheduleManager.class) {
            if(instance == null) {
                if(config == null) {
                    throw new IllegalArgumentException("config is null");
                }
                
                try {
                    // type cast
                    Collection<AbstractDriver> drivers = (Collection<AbstractDriver>) config.getDrivers();
                    List<AbstractScheduleDriver> scheduleDrivers = new ArrayList<AbstractScheduleDriver>();
                    for(AbstractDriver driver : drivers) {
                        scheduleDrivers.add((AbstractScheduleDriver) driver);
                    }
                    instance = new ScheduleManager(service, scheduleDrivers);
                } catch (DriverFailedToLoadException ex) {
                    LOG.error(ex);
                    throw new ManagerNotInstantiatedException(ex.toString());
                }
            }
            return instance;
        }
    }
    
    public static ScheduleManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (ScheduleManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("ScheduleManager is not started");
            }
            return instance;
        }
    }
    
    ScheduleManager(StargateService service, Collection<AbstractScheduleDriver> drivers) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(drivers == null || drivers.isEmpty()) {
            throw new IllegalArgumentException("drivers is null or empty");
        }
        
        this.setService(service);
        
        for(AbstractScheduleDriver driver : drivers) {
            this.drivers.add(driver);
        }
    }

    public AbstractScheduleDriver getDriver() {
        if(this.drivers.size() > 0) {
            return this.drivers.get(0);
        }
        return null;
    }
    
    @Override
    public synchronized void start() throws IOException {
        super.start();
        
        this.timer = new Timer("ScheduleManager Task Timer", false);
    }
    
    @Override
    public synchronized void stop() throws IOException {
        this.timer.cancel();
        this.timer.purge();
        this.timer = null;
        
        super.stop();
    }
    
    public void scheduleTask(DistributedTask task) throws IOException, DriverNotInitializedException {
        if(task == null) {
            throw new IllegalArgumentException("task is null");
        }
        
        AbstractScheduleDriver driver = getDriver();
        driver.scheduleTask(task);
    }
    
    public void scheduleTask(TimerTask task, long delay, long period) throws IOException {
        if(task == null) {
            throw new IllegalArgumentException("task is null");
        }
        
        this.timer.scheduleAtFixedRate(task, delay, period);
    }
}
