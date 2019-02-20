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
package stargate.managers.datastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.driver.AbstractDriver;
import stargate.commons.driver.DriverFailedToLoadException;
import stargate.commons.datastore.AbstractDataStoreDriver;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerConfig;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class DataStoreManager extends AbstractManager<AbstractDataStoreDriver> {
    
    private static final Log LOG = LogFactory.getLog(DataStoreManager.class);
    
    private static DataStoreManager instance;
    
    public static DataStoreManager getInstance(StargateService service, Collection<AbstractDataStoreDriver> drivers) throws ManagerNotInstantiatedException {
        synchronized (DataStoreManager.class) {
            if(instance == null) {
                instance = new DataStoreManager(service, drivers);
            }
            return instance;
        }
    }
    
    public static DataStoreManager getInstance(StargateService service, ManagerConfig config) throws ManagerNotInstantiatedException {
        synchronized (DataStoreManager.class) {
            if(instance == null) {
                if(config == null) {
                    throw new IllegalArgumentException("config is null");
                }
                
                try {
                    // type cast
                    Collection<AbstractDriver> drivers = (Collection<AbstractDriver>) config.getDrivers();
                    List<AbstractDataStoreDriver> dataStoreDrivers = new ArrayList<AbstractDataStoreDriver>();
                    for(AbstractDriver driver : drivers) {
                        dataStoreDrivers.add((AbstractDataStoreDriver) driver);
                    }
                    instance = new DataStoreManager(service, dataStoreDrivers);
                } catch (DriverFailedToLoadException ex) {
                    LOG.error("Could not load driver", ex);
                    throw new ManagerNotInstantiatedException(ex.toString());
                }
            }
            return instance;
        }
    }
    
    public static DataStoreManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (DataStoreManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("PersistentStorageManager is not started");
            }
            return instance;
        }
    }
    
    DataStoreManager(StargateService service, Collection<AbstractDataStoreDriver> drivers) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(drivers == null || drivers.isEmpty()) {
            throw new IllegalArgumentException("drivers is null or empty");
        }
        
        this.setService(service);
        
        for(AbstractDataStoreDriver driver : drivers) {
            this.drivers.add(driver);
        }
    }
    
    @Override
    public synchronized void start() throws IOException {
        super.start();
    }
    
    @Override
    public synchronized void stop() throws IOException {
        super.stop();
    }
    
    public AbstractDataStoreDriver getDriver() {
        if(this.drivers.size() > 0) {
            return this.drivers.get(0);
        }
        return null;
    }
}
