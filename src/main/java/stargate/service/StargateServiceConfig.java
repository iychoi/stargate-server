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
import stargate.commons.driver.DriverInjection;
import stargate.commons.manager.ManagerConfig;
import stargate.commons.service.ServiceConfig;
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


/**
 *
 * @author iychoi
 */
public class StargateServiceConfig extends ServiceConfig {
    
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
    @JsonIgnore
    public void setImmutable() {
        super.setImmutable();
        
        this.userConfig.setImmutable();
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
    public ManagerConfig getDefaultTransportConfig() {
        DriverInjection driverInjection = new DriverInjection();
        driverInjection.setDriverClass(HTTPTransportDriver.class);
        
        HTTPTransportDriverConfig driverConfiguration = new HTTPTransportDriverConfig();
        driverInjection.setDriverConfig(driverConfiguration);
        
        ManagerConfig managerConfig = new ManagerConfig();
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

    @JsonIgnore
    public ServiceConfig toServiceConfig() {
        ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setClusterConfig(this.clusterManagerConfig);
        serviceConfig.setEventConfig(this.eventManagerConfig);
        serviceConfig.setDataSourceConfig(this.dataSourceManagerConfig);
        serviceConfig.setDataStoreConfig(this.dataStoreManagerConfig);
        serviceConfig.setRecipeConfig(this.recipeManagerConfig);
        serviceConfig.setTransportConfig(this.transportManagerConfig);
        serviceConfig.setUserInterfaceConfig(this.userInterfaceManagerConfig);
        serviceConfig.setScheduleConfig(this.scheduleManagerConfig);
        return serviceConfig;
    }
}
