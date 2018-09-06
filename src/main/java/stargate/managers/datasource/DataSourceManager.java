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
package stargate.managers.datasource;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import stargate.service.StargateService;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.datasource.AbstractDataSourceDriver;
import stargate.commons.driver.AbstractDriver;
import stargate.commons.driver.DriverFailedToLoadException;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerConfig;
import stargate.commons.manager.ManagerNotInstantiatedException;

/**
 *
 * @author iychoi
 */
public class DataSourceManager extends AbstractManager<AbstractDataSourceDriver> {
    
    private static final Log LOG = LogFactory.getLog(DataSourceManager.class);
    
    private static DataSourceManager instance;

    public static DataSourceManager getInstance(StargateService service, Collection<AbstractDataSourceDriver> drivers) throws ManagerNotInstantiatedException {
        synchronized (DataSourceManager.class) {
            if(instance == null) {
                instance = new DataSourceManager(service, drivers);
            }
            return instance;
        }
    }
    
    public static DataSourceManager getInstance(StargateService service, ManagerConfig config) throws ManagerNotInstantiatedException {
        synchronized (DataSourceManager.class) {
            if(instance == null) {
                if(config == null) {
                    throw new IllegalArgumentException("config is null");
                }
                
                try {
                    // type cast
                    Collection<AbstractDriver> drivers = (Collection<AbstractDriver>) config.getDrivers();
                    List<AbstractDataSourceDriver> dataSourceDrivers = new ArrayList<AbstractDataSourceDriver>();
                    for(AbstractDriver driver : drivers) {
                        dataSourceDrivers.add((AbstractDataSourceDriver) driver);
                    }
                    instance = new DataSourceManager(service, dataSourceDrivers);
                } catch (DriverFailedToLoadException ex) {
                    LOG.error(ex);
                    throw new ManagerNotInstantiatedException(ex.toString());
                }
            }
            return instance;
        }
    }
    
    public static DataSourceManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (DataSourceManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("DataSourceManager is not started");
            }
            return instance;
        }
    }
    
    DataSourceManager(StargateService service, Collection<AbstractDataSourceDriver> drivers) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(drivers == null || drivers.isEmpty()) {
            throw new IllegalArgumentException("drivers is null or empty");
        }
        
        this.setService(service);
        
        for(AbstractDataSourceDriver driver : drivers) {
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
    
    public AbstractDataSourceDriver getDriver(URI sourceURI) {
        if(sourceURI == null) {
            throw new IllegalArgumentException("sourceURI is null");
        }
        
        return getDriver(sourceURI.getScheme());
    }
    
    public AbstractDataSourceDriver getDriver(String scheme) {
        if(scheme == null || scheme.isEmpty()) {
            throw new IllegalArgumentException("scheme is null or empty");
        }
        
        for(AbstractDataSourceDriver driver : this.drivers) {
            if(scheme.equalsIgnoreCase(driver.getScheme())) {
                return driver;
            }
        }
        return null;
    }
}
