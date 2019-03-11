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

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.config.AbstractImmutableConfig;
import stargate.commons.driver.DriverInjection;
import stargate.commons.manager.ManagerConfig;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.utils.PathUtils;
import stargate.drivers.cluster.ignite.IgniteClusterDriver;
import stargate.drivers.cluster.ignite.IgniteClusterDriverConfig;
import stargate.drivers.datasource.localfs.LocalFSDataSourceDriver;
import stargate.drivers.datasource.localfs.LocalFSDataSourceDriverConfig;
import stargate.drivers.datastore.ignite.IgniteDataStoreDriver;
import stargate.drivers.datastore.ignite.IgniteDataStoreDriverConfig;
import stargate.drivers.event.ignite.IgniteEventDriver;
import stargate.drivers.event.ignite.IgniteEventDriverConfig;
import stargate.drivers.recipe.fixedsize.FixedSizeChunkRecipeDriver;
import stargate.drivers.recipe.fixedsize.FixedSizeChunkRecipeDriverConfig;
import stargate.drivers.schedule.ignite.IgniteScheduleDriver;
import stargate.drivers.schedule.ignite.IgniteScheduleDriverConfig;
import stargate.drivers.transport.http.HTTPTransportDriver;
import stargate.drivers.transport.http.HTTPTransportDriverConfig;
import stargate.drivers.userinterface.http.HTTPUserInterfaceDriver;
import stargate.drivers.userinterface.http.HTTPUserInterfaceDriverConfig;
import stargate.managers.transport.TransportManagerConfig;


/**
 *
 * @author iychoi
 */
public class StargateServiceConfig extends AbstractImmutableConfig {
    
    protected ManagerConfig clusterManagerConfig;
    protected ManagerConfig eventManagerConfig;
    protected ManagerConfig dataSourceManagerConfig;
    protected ManagerConfig dataStoreManagerConfig;
    protected ManagerConfig recipeManagerConfig;
    protected TransportManagerConfig transportManagerConfig;
    protected ManagerConfig userInterfaceManagerConfig;
    protected ManagerConfig scheduleManagerConfig;
    
    private UserConfig userConfig;
    
    
    public static StargateServiceConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (StargateServiceConfig) JsonSerializer.fromJsonFile(file, StargateServiceConfig.class);
    }
    
    public static StargateServiceConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (StargateServiceConfig) JsonSerializer.fromJson(json, StargateServiceConfig.class);
    }
    
    public StargateServiceConfig () {
        initDefaults();
    }
    
    private void initDefaults() {
        this.clusterManagerConfig = getDefaultClusterConfig();
        this.eventManagerConfig = getDefaultEventConfig();
        this.dataSourceManagerConfig = getDefaultDataSourceConfig();
        this.dataStoreManagerConfig = getDefaultDataStoreConfig();
        this.recipeManagerConfig = getDefaultRecipeConfig();
        this.transportManagerConfig = getDefaultTransportConfig();
        this.userInterfaceManagerConfig = getDefaultUserInterfaceConfig();
        this.scheduleManagerConfig = getDefaultScheduleConfig();
        this.userConfig = new UserConfig();
    }
    
    @Override
    public void setImmutable() {
        super.setImmutable();
        
        if(this.clusterManagerConfig != null) {
            this.clusterManagerConfig.setImmutable();
        }
        
        if(this.eventManagerConfig != null) {
            this.eventManagerConfig.setImmutable();
        }
        
        if(this.dataSourceManagerConfig != null) {
            this.dataSourceManagerConfig.setImmutable();
        }
        
        if(this.dataStoreManagerConfig != null) {
            this.dataStoreManagerConfig.setImmutable();
        }
    
        if(this.recipeManagerConfig != null) {
            this.recipeManagerConfig.setImmutable();
        }
        
        if(this.transportManagerConfig != null) {
            this.transportManagerConfig.setImmutable();
        }
        
        if(this.userInterfaceManagerConfig != null) {
            this.userInterfaceManagerConfig.setImmutable();
        }
        
        if(this.scheduleManagerConfig != null) {
            this.scheduleManagerConfig.setImmutable();
        }
        
        if(this.userConfig != null) {
            this.userConfig.setImmutable();
        }
    }
    
    @JsonProperty("cluster")
    public void setClusterConfig(ManagerConfig clusterConfig) {
        if(clusterConfig == null) {
            throw new IllegalArgumentException("clusterConfig is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.clusterManagerConfig = clusterConfig;
    }
    
    @JsonProperty("cluster")
    public ManagerConfig getClusterConfig() {
        return this.clusterManagerConfig;
    }
    
    @JsonProperty("event")
    public void setEventConfig(ManagerConfig eventConfig) {
        if(eventConfig == null) {
            throw new IllegalArgumentException("eventConfig is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.eventManagerConfig = eventConfig;
    }
    
    @JsonProperty("event")
    public ManagerConfig getEventConfig() {
        return this.eventManagerConfig;
    }
    
    @JsonProperty("data_source")
    public void setDataSourceConfig(ManagerConfig dataSourceConfig) {
        if(dataSourceConfig == null) {
            throw new IllegalArgumentException("dataSourceConfig is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.dataSourceManagerConfig = dataSourceConfig;
    }
    
    @JsonProperty("data_source")
    public ManagerConfig getDataSourceConfig() {
        return this.dataSourceManagerConfig;
    }
    
    @JsonProperty("data_store")
    public void setDataStoreConfig(ManagerConfig dataStoreConfig) {
        if(dataStoreConfig == null) {
            throw new IllegalArgumentException("dataStoreConfig is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.dataStoreManagerConfig = dataStoreConfig;
    }
    
    @JsonProperty("data_store")
    public ManagerConfig getDataStoreConfig() {
        return this.dataStoreManagerConfig;
    }
    
    @JsonProperty("recipe")
    public void setRecipeConfig(ManagerConfig recipeConfig) {
        if(recipeConfig == null) {
            throw new IllegalArgumentException("recipeConfig is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.recipeManagerConfig = recipeConfig;
    }
    
    @JsonProperty("recipe")
    public ManagerConfig getRecipeConfig() {
        return this.recipeManagerConfig;
    }
    
    @JsonProperty("transport")
    public void setTransportConfig(TransportManagerConfig transportConfig) {
        if(transportConfig == null) {
            throw new IllegalArgumentException("transportConfig is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.transportManagerConfig = transportConfig;
    }
    
    @JsonProperty("transport")
    public TransportManagerConfig getTransportConfig() {
        return this.transportManagerConfig;
    }
    
    @JsonProperty("user_interface")
    public void setUserInterfaceConfig(ManagerConfig userInterfaceConfig) {
        if(userInterfaceConfig == null) {
            throw new IllegalArgumentException("userInterfaceConfig is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.userInterfaceManagerConfig = userInterfaceConfig;
    }
    
    @JsonProperty("user_interface")
    public ManagerConfig getUserInterfaceConfig() {
        return this.userInterfaceManagerConfig;
    }
    
    @JsonProperty("schedule")
    public void setScheduleConfig(ManagerConfig scheduleConfig) {
        if(scheduleConfig == null) {
            throw new IllegalArgumentException("scheduleConfig is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.scheduleManagerConfig = scheduleConfig;
    }
    
    @JsonProperty("schedule")
    public ManagerConfig getScheduleConfig() {
        return this.scheduleManagerConfig;
    }
    
    @JsonProperty("user_config")
    public void setUserConfig(UserConfig userConfig) {
        if(userConfig == null) {
            throw new IllegalArgumentException("userConfig is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.userConfig = userConfig;
    }
    
    @JsonProperty("user_config")
    public UserConfig getUserConfig() {
        return this.userConfig;
    }
    
    @JsonIgnore
    public ManagerConfig getDefaultClusterConfig() {
        DriverInjection driverInjection = new DriverInjection();
        driverInjection.setDriverClass(IgniteClusterDriver.class);
        
        IgniteClusterDriverConfig driverConfiguration = new IgniteClusterDriverConfig();
        driverInjection.setDriverConfig(driverConfiguration);
        
        ManagerConfig managerConfig = new ManagerConfig();
        managerConfig.addDriverSetting(driverInjection);
        return managerConfig;
    }
    
    @JsonIgnore
    public ManagerConfig getDefaultEventConfig() {
        DriverInjection driverInjection = new DriverInjection();
        driverInjection.setDriverClass(IgniteEventDriver.class);
        
        IgniteEventDriverConfig driverConfiguration = new IgniteEventDriverConfig();
        driverInjection.setDriverConfig(driverConfiguration);
        
        ManagerConfig managerConfig = new ManagerConfig();
        managerConfig.addDriverSetting(driverInjection);
        return managerConfig;
    }
    
    @JsonIgnore
    public ManagerConfig getDefaultDataSourceConfig() {
        DriverInjection driverInjection = new DriverInjection();
        driverInjection.setDriverClass(LocalFSDataSourceDriver.class);
        
        LocalFSDataSourceDriverConfig driverConfiguration = new LocalFSDataSourceDriverConfig();
        
        String workingDir = PathUtils.getWorkingDir();
        File dataSourceRootDir = new File(workingDir, "data_source");
        driverConfiguration.setRootPath(dataSourceRootDir);
        
        driverInjection.setDriverConfig(driverConfiguration);
        
        ManagerConfig managerConfig = new ManagerConfig();
        managerConfig.addDriverSetting(driverInjection);
        return managerConfig;
    }

    @JsonIgnore
    public ManagerConfig getDefaultDataStoreConfig() {
        DriverInjection driverInjection = new DriverInjection();
        driverInjection.setDriverClass(IgniteDataStoreDriver.class);
        
        IgniteDataStoreDriverConfig driverConfiguration = new IgniteDataStoreDriverConfig();
        
        driverInjection.setDriverConfig(driverConfiguration);
        
        ManagerConfig managerConfig = new ManagerConfig();
        managerConfig.addDriverSetting(driverInjection);
        return managerConfig;
    }

    @JsonIgnore
    public ManagerConfig getDefaultRecipeConfig() {
        DriverInjection driverInjection = new DriverInjection();
        driverInjection.setDriverClass(FixedSizeChunkRecipeDriver.class);
        
        FixedSizeChunkRecipeDriverConfig driverConfiguration = new FixedSizeChunkRecipeDriverConfig();
        driverInjection.setDriverConfig(driverConfiguration);
        
        ManagerConfig managerConfig = new ManagerConfig();
        managerConfig.addDriverSetting(driverInjection);
        return managerConfig;
    }

    @JsonIgnore
    public TransportManagerConfig getDefaultTransportConfig() {
        DriverInjection driverInjection = new DriverInjection();
        driverInjection.setDriverClass(HTTPTransportDriver.class);
        
        HTTPTransportDriverConfig driverConfiguration = new HTTPTransportDriverConfig();
        driverInjection.setDriverConfig(driverConfiguration);
        
        TransportManagerConfig managerConfig = new TransportManagerConfig();
        managerConfig.addDriverSetting(driverInjection);
        return managerConfig;
    }

    @JsonIgnore
    public ManagerConfig getDefaultUserInterfaceConfig() {
        DriverInjection driverInjection = new DriverInjection();
        driverInjection.setDriverClass(HTTPUserInterfaceDriver.class);
        
        HTTPUserInterfaceDriverConfig driverConfiguration = new HTTPUserInterfaceDriverConfig();
        driverInjection.setDriverConfig(driverConfiguration);
        
        ManagerConfig managerConfig = new ManagerConfig();
        managerConfig.addDriverSetting(driverInjection);
        return managerConfig;
    }
    
    @JsonIgnore
    public ManagerConfig getDefaultScheduleConfig() {
        DriverInjection driverInjection = new DriverInjection();
        driverInjection.setDriverClass(IgniteScheduleDriver.class);
        
        IgniteScheduleDriverConfig driverConfiguration = new IgniteScheduleDriverConfig();
        driverInjection.setDriverConfig(driverConfiguration);
        
        ManagerConfig managerConfig = new ManagerConfig();
        managerConfig.addDriverSetting(driverInjection);
        return managerConfig;
    }
}
