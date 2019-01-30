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
package stargate.service;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.service.AbstractService;
import stargate.commons.service.ServiceNotStartedException;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.dataexport.DataExportManager;
import stargate.managers.datasource.DataSourceManager;
import stargate.managers.datastore.DataStoreManager;
import stargate.managers.policy.PolicyManager;
import stargate.managers.recipe.RecipeManager;
import stargate.managers.recipe.RecipeManagerException;
import stargate.managers.schedule.ScheduleManager;
import stargate.managers.transport.TransportManager;
import stargate.managers.userinterface.UserInterfaceManager;
import stargate.managers.volume.VolumeManager;
import stargate.managers.dataexport.DataExportUpdateEventHandler;

/**
 *
 * @author iychoi
 */
public class StargateService extends AbstractService {
    
    private static final Log LOG = LogFactory.getLog(StargateService.class);

    private static StargateService instance;
    
    private StargateServiceConfig config;
    private boolean started = false;
    
    private ClusterManager clusterManager;
    private DataSourceManager dataSourceManager;
    private DataExportManager dataExportManager;
    private RecipeManager recipeManager;
    private DataStoreManager dataStoreManager;
    private ScheduleManager scheduleManager;
    private TransportManager transportManager;
    private UserInterfaceManager userInterfaceManager;
    private PolicyManager policyManager;
    private VolumeManager volumeManager;
    
    private DataExportUpdateEventHandler dataExportUpdateEventHandler;
    
    public static StargateService getInstance(StargateServiceConfig config) throws ServiceNotStartedException {
        synchronized (DataStoreManager.class) {
            if(instance == null) {
                if(config == null) {
                    throw new IllegalArgumentException("config is null");
                }
                
                instance = new StargateService(config);
            }
            return instance;
        }
    }
    
    public static StargateService getInstance() throws ServiceNotStartedException {
        synchronized (StargateService.class) {
            if(instance == null) {
                throw new ServiceNotStartedException("StargateService is not started");
            }
            return instance;
        }
    }
    
    StargateService(StargateServiceConfig config) throws ServiceNotStartedException {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void start() throws IOException {
        if(this.started) {
            throw new IllegalStateException("Service is already started");
        }
        
        LOG.info("Starting service...");
        
        // init managers
        LOG.debug("Initializing managers");
        try {
            this.clusterManager = ClusterManager.getInstance(this, this.config.getClusterConfig());
            this.dataSourceManager = DataSourceManager.getInstance(this, this.config.getDataSourceConfig());
            this.dataExportManager = DataExportManager.getInstance(this);
            this.recipeManager = RecipeManager.getInstance(this, this.config.getRecipeConfig());
            this.dataStoreManager = DataStoreManager.getInstance(this, this.config.getDataStoreConfig());
            this.scheduleManager = ScheduleManager.getInstance(this, this.config.getScheduleConfig());
            this.transportManager = TransportManager.getInstance(this, this.config.getTransportConfig());
            this.userInterfaceManager = UserInterfaceManager.getInstance(this, this.config.getUserInterfaceConfig());
            this.policyManager = PolicyManager.getInstance(this);
            this.volumeManager = VolumeManager.getInstance(this);
        } catch (ManagerNotInstantiatedException ex) {
            throw new IOException(ex);
        }
        LOG.debug("Managers are initialized");
        
        LOG.info("Starting managers");
        this.clusterManager.start();
        this.dataStoreManager.start();
        this.dataSourceManager.start();
        this.policyManager.start();
        this.dataExportManager.start();
        this.scheduleManager.start();
        this.recipeManager.start();
        this.transportManager.start();
        this.userInterfaceManager.start();
        this.volumeManager.start();
        LOG.info("Managers are started");
        
        LOG.debug("Registering event handlers");
        this.dataExportUpdateEventHandler = new DataExportUpdateEventHandler(this.recipeManager, this.volumeManager);
        this.dataExportManager.addDataExportEventHandler(this.dataExportUpdateEventHandler);
        LOG.debug("Event handlers are registered");
        
        LOG.info("Synchronizing states");
        try {
            this.recipeManager.syncRecipes();
            this.volumeManager.buildLocalDirectoryHierarchy();
        } catch (ManagerNotInstantiatedException ex) {
            throw new IOException(ex);
        } catch (RecipeManagerException ex) {
            throw new IOException(ex);
        }
        LOG.info("States are synchronized");
        
        this.started = true;
        
        LOG.info("Service is started...");
    }
    
    @Override
    public synchronized void stop() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Service is already stopped");
        }
        
        LOG.info("Stopping service...");
        
        LOG.debug("Unregistering event handlers");
        this.dataExportManager.removeDataExportEventHandler(this.dataExportUpdateEventHandler);
        this.dataExportUpdateEventHandler = null;
        LOG.debug("Event handlers are unregistered");
        
        LOG.info("Stopping managers");
        this.volumeManager.stop();
        this.userInterfaceManager.stop();
        this.transportManager.stop();
        this.recipeManager.stop();
        this.scheduleManager.stop();
        this.dataExportManager.stop();
        this.policyManager.stop();
        this.dataSourceManager.stop();
        this.dataStoreManager.stop();
        this.clusterManager.stop();
        LOG.info("Managers are stopped");
        
        this.started = false;
        
        LOG.info("Service is stopped...");
    }
    
    @Override
    public boolean isStarted() {
        return this.started;
    }
    
    public StargateServiceConfig getConfig() {
        return this.config;
    }
    
    public ClusterManager getClusterManager() throws ManagerNotInstantiatedException {
        if(this.clusterManager == null || !this.clusterManager.isStarted()) {
            throw new ManagerNotInstantiatedException("ClusterManager is not started");
        }
        
        return this.clusterManager;
    }
    
    public DataSourceManager getDataSourceManager() throws ManagerNotInstantiatedException {
        if(this.dataSourceManager == null || !this.dataSourceManager.isStarted()) {
            throw new ManagerNotInstantiatedException("DataSourceManager is not started");
        }
        
        return this.dataSourceManager;
    }
    
    public DataExportManager getDataExportManager() throws ManagerNotInstantiatedException {
        if(this.dataExportManager == null || !this.dataExportManager.isStarted()) {
            throw new ManagerNotInstantiatedException("DataExportManager is not started");
        }
        
        return this.dataExportManager;
    }
    
    public RecipeManager getRecipeManager() throws ManagerNotInstantiatedException {
        if(this.recipeManager == null || !this.recipeManager.isStarted()) {
            throw new ManagerNotInstantiatedException("RecipeManager is not started");
        }
        
        return this.recipeManager;
    }
    
    public DataStoreManager getDataStoreManager() throws ManagerNotInstantiatedException {
        if(this.dataStoreManager == null || !this.dataStoreManager.isStarted()) {
            throw new ManagerNotInstantiatedException("DataStoreManager is not started");
        }
        
        return this.dataStoreManager;
    }
    
    public ScheduleManager getScheduleManager() throws ManagerNotInstantiatedException {
        if(this.scheduleManager == null || !this.scheduleManager.isStarted()) {
            throw new ManagerNotInstantiatedException("ScheduleManager is not started");
        }
        
        return this.scheduleManager;
    }
    
    public TransportManager getTransportManager() throws ManagerNotInstantiatedException {
        if(this.transportManager == null || !this.transportManager.isStarted()) {
            throw new ManagerNotInstantiatedException("TransportManager is not started");
        }
        
        return this.transportManager;
    }
    
    public UserInterfaceManager getUserInterfaceManager() throws ManagerNotInstantiatedException {
        if(this.userInterfaceManager == null || !this.userInterfaceManager.isStarted()) {
            throw new ManagerNotInstantiatedException("UserInterfaceManager is not started");
        }
        
        return this.userInterfaceManager;
    }

    public PolicyManager getPolicyManager() throws ManagerNotInstantiatedException {
        if(this.policyManager == null || !this.policyManager.isStarted()) {
            throw new ManagerNotInstantiatedException("PolicyManager is not started");
        }
        
        return this.policyManager;
    }
    
    public VolumeManager getVolumeManager() throws ManagerNotInstantiatedException {
        if(this.volumeManager == null || !this.volumeManager.isStarted()) {
            throw new ManagerNotInstantiatedException("VolumeManager is not started");
        }
        
        return this.volumeManager;
    }
}
