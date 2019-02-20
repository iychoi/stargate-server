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
package stargate.managers.userinterface;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.driver.AbstractDriver;
import stargate.commons.driver.DriverFailedToLoadException;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerConfig;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.userinterface.AbstractUserInterfaceDriver;
import stargate.commons.userinterface.UserInterfaceServiceInfo;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class UserInterfaceManager extends AbstractManager<AbstractUserInterfaceDriver> {

    private static final Log LOG = LogFactory.getLog(UserInterfaceManager.class);
    
    private static UserInterfaceManager instance;

    public static UserInterfaceManager getInstance(StargateService service, Collection<AbstractUserInterfaceDriver> drivers) throws ManagerNotInstantiatedException {
        synchronized (UserInterfaceManager.class) {
            if(instance == null) {
                instance = new UserInterfaceManager(service, drivers);
            }
            return instance;
        }
    }
    
    public static UserInterfaceManager getInstance(StargateService service, ManagerConfig config) throws ManagerNotInstantiatedException {
        synchronized (UserInterfaceManager.class) {
            if(instance == null) {
                if(config == null) {
                    throw new IllegalArgumentException("config is null");
                }
                
                try {
                    // type cast
                    Collection<AbstractDriver> drivers = (Collection<AbstractDriver>) config.getDrivers();
                    List<AbstractUserInterfaceDriver> userInterfaceDrivers = new ArrayList<AbstractUserInterfaceDriver>();
                    for(AbstractDriver driver : drivers) {
                        userInterfaceDrivers.add((AbstractUserInterfaceDriver) driver);
                    }
                    instance = new UserInterfaceManager(service, userInterfaceDrivers);
                } catch (DriverFailedToLoadException ex) {
                    LOG.error("Could not load driver", ex);
                    throw new ManagerNotInstantiatedException(ex.toString());
                }
            }
            return instance;
        }
    }
    
    public static UserInterfaceManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (UserInterfaceManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("UserInterfaceManager is not started");
            }
            return instance;
        }
    }
    
    UserInterfaceManager(StargateService service, Collection<AbstractUserInterfaceDriver> drivers) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(drivers == null || drivers.isEmpty()) {
            throw new IllegalArgumentException("drivers is null or empty");
        }
        
        this.setService(service);
        
        for(AbstractUserInterfaceDriver driver : drivers) {
            this.drivers.add(driver);
        }
    }
    
    public AbstractUserInterfaceDriver getDriver() {
        if(this.drivers.size() > 0) {
            return this.drivers.get(0);
        }
        return null;
    }
    
    @Override
    public synchronized void start() throws IOException {
        super.start();
        
        for(AbstractUserInterfaceDriver driver : drivers) {
            try {
                driver.startServer();
            } catch (DriverNotInitializedException ex) {
                throw new IOException(ex);
            }
        }
    }
    
    @Override
    public synchronized void stop() throws IOException {
        for(AbstractUserInterfaceDriver driver : drivers) {
            try {
                driver.stopServer();
            } catch (DriverNotInitializedException ex) {
                throw new IOException(ex);
            }
        }
        
        super.stop();
    }

    public UserInterfaceServiceInfo getServiceInfo() throws IOException, DriverNotInitializedException {
        AbstractUserInterfaceDriver driver = getDriver();
        URI serviceURI = driver.getServiceURI();
        return new UserInterfaceServiceInfo(driver.getClass().getName(), serviceURI);
    }
}
