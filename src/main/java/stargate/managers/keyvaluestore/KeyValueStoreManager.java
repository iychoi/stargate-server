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
package stargate.managers.keyvaluestore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.driver.AbstractDriver;
import stargate.commons.driver.DriverFailedToLoadException;
import stargate.commons.keyvaluestore.AbstractKeyValueStoreDriver;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerConfig;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class KeyValueStoreManager extends AbstractManager<AbstractKeyValueStoreDriver> {
    
    private static final Log LOG = LogFactory.getLog(KeyValueStoreManager.class);
    
    private static KeyValueStoreManager instance;
    

    public static KeyValueStoreManager getInstance(StargateService service, Collection<AbstractKeyValueStoreDriver> drivers) throws ManagerNotInstantiatedException {
        synchronized (KeyValueStoreManager.class) {
            if(instance == null) {
                instance = new KeyValueStoreManager(service, drivers);
            }
            return instance;
        }
    }
    
    public static KeyValueStoreManager getInstance(StargateService service, ManagerConfig config) throws ManagerNotInstantiatedException {
        synchronized (KeyValueStoreManager.class) {
            if(instance == null) {
                if(config == null) {
                    throw new IllegalArgumentException("config is null");
                }
                
                try {
                    // type cast
                    Collection<AbstractDriver> drivers = (Collection<AbstractDriver>) config.getDrivers();
                    List<AbstractKeyValueStoreDriver> keyValueStoreDrivers = new ArrayList<AbstractKeyValueStoreDriver>();
                    for(AbstractDriver driver : drivers) {
                        keyValueStoreDrivers.add((AbstractKeyValueStoreDriver) driver);
                    }
                    instance = new KeyValueStoreManager(service, keyValueStoreDrivers);
                } catch (DriverFailedToLoadException ex) {
                    LOG.error(ex);
                    throw new ManagerNotInstantiatedException(ex.toString());
                }
            }
            return instance;
        }
    }
    
    public static KeyValueStoreManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (KeyValueStoreManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("PersistentStorageManager is not started");
            }
            return instance;
        }
    }
    
    KeyValueStoreManager(StargateService service, Collection<AbstractKeyValueStoreDriver> drivers) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(drivers == null || drivers.isEmpty()) {
            throw new IllegalArgumentException("drivers is null or empty");
        }
        
        this.setService(service);
        
        for(AbstractKeyValueStoreDriver driver : drivers) {
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
    
    public AbstractKeyValueStoreDriver getDriver() {
        if(this.drivers.size() > 0) {
            return this.drivers.get(0);
        }
        return null;
    }
}
