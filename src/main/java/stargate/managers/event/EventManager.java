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
package stargate.managers.event;

import stargate.commons.event.AbstractEventHandler;
import stargate.commons.event.StargateEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.driver.AbstractDriver;
import stargate.commons.driver.DriverFailedToLoadException;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.event.AbstractEventDriver;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerConfig;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class EventManager extends AbstractManager<AbstractEventDriver> {
    
    private static final Log LOG = LogFactory.getLog(EventManager.class);
    
    private static EventManager instance;
    
    public static EventManager getInstance(StargateService service, ManagerConfig config, Collection<AbstractEventDriver> drivers) throws ManagerNotInstantiatedException {
        synchronized (EventManager.class) {
            if(instance == null) {
                instance = new EventManager(service, config, drivers);
            }
            return instance;
        }
    }
    
    public static EventManager getInstance(StargateService service, ManagerConfig config) throws ManagerNotInstantiatedException {
        synchronized (EventManager.class) {
            if(instance == null) {
                if(config == null) {
                    throw new IllegalArgumentException("config is null");
                }
                
                try {
                    // type cast
                    Collection<AbstractDriver> drivers = (Collection<AbstractDriver>) config.getDrivers();
                    List<AbstractEventDriver> eventDrivers = new ArrayList<AbstractEventDriver>();
                    for(AbstractDriver driver : drivers) {
                        eventDrivers.add((AbstractEventDriver) driver);
                    }
                    instance = new EventManager(service, config, eventDrivers);
                } catch (DriverFailedToLoadException ex) {
                    LOG.error("Could not load driver", ex);
                    throw new ManagerNotInstantiatedException(ex.toString());
                }
            }
            return instance;
        }
    }
    
    public static EventManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (EventManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("EventManager is not started");
            }
            return instance;
        }
    }
    
    EventManager(StargateService service, ManagerConfig config, Collection<AbstractEventDriver> drivers) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(drivers == null || drivers.isEmpty()) {
            throw new IllegalArgumentException("drivers is null or empty");
        }
        
        this.setService(service);
        this.setConfig(config);
        
        for(AbstractEventDriver driver : drivers) {
            this.drivers.add(driver);
        }
    }
    
    public AbstractEventDriver getDriver() {
        if(this.drivers.size() > 0) {
            return this.drivers.get(0);
        }
        return null;
    }
    
    @Override
    public synchronized void start() throws IOException {
        super.start();
    }
    
    @Override
    public synchronized void stop() throws IOException {
        super.stop();
    }
    
    public void raiseEvent(StargateEvent event) throws IOException, DriverNotInitializedException {
        AbstractEventDriver driver = getDriver();
        driver.raiseEvent(event);
    }
    
    public void addEventHandler(AbstractEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        AbstractEventDriver driver = getDriver();
        driver.addEventHandler(eventHandler);
    }
    
    public void removeEventHandler(AbstractEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        AbstractEventDriver driver = getDriver();
        driver.removeEventHandler(eventHandler);
    }
}
