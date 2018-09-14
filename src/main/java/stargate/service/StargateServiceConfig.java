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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import stargate.drivers.keyvaluestore.ignite.IgniteKeyValueStoreDriver;
import stargate.drivers.keyvaluestore.ignite.IgniteKeyValueStoreDriverConfig;
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
public class StargateServiceConfig extends AbstractImmutableConfig {
    
    private static final Log LOG = LogFactory.getLog(StargateServiceConfig.class);
    
    private ManagerConfig clusterManagerConfig;
    private ManagerConfig dataSourceManagerConfig;
    private ManagerConfig keyValueStoreManagerConfig;
    private ManagerConfig recipeManagerConfig;
    private ManagerConfig transportManagerConfig;
    private ManagerConfig userInterfaceManagerConfig;
    private ManagerConfig scheduleManagerConfig;
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
    
    public StargateServiceConfig() {
        initDefaults();
    }
    
    private void initDefaults() {
        this.clusterManagerConfig = getDefaultClusterConfig();
        this.dataSourceManagerConfig = getDefaultDataSourceConfig();
        this.keyValueStoreManagerConfig = getDefaultKeyValueStoreConfig();
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
        
        if(this.dataSourceManagerConfig != null) {
            this.dataSourceManagerConfig.setImmutable();
        }
        
        if(this.keyValueStoreManagerConfig != null) {
            this.keyValueStoreManagerConfig.setImmutable();
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
    
    @JsonProperty("keyvalue_store")
    public void setKeyValueStoreConfig(ManagerConfig keyValueStoreConfig) {
        if(keyValueStoreConfig == null) {
            throw new IllegalArgumentException("keyValueStoreConfig is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.keyValueStoreManagerConfig = keyValueStoreConfig;
    }
    
    @JsonProperty("keyvalue_store")
    public ManagerConfig getKeyValueStoreConfig() {
        return this.keyValueStoreManagerConfig;
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
    public void setTransportConfig(ManagerConfig transportConfig) {
        if(transportConfig == null) {
            throw new IllegalArgumentException("transportConfig is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.transportManagerConfig = transportConfig;
    }
    
    @JsonProperty("transport")
    public ManagerConfig getTransportConfig() {
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
    
    @JsonProperty("schedule")
    public ManagerConfig getScheduleConfig() {
        return this.scheduleManagerConfig;
    }
    
    @JsonProperty("schedule")
    public void setScheduleConfig(ManagerConfig scheduleConfig) {
        if(scheduleConfig == null) {
            throw new IllegalArgumentException("scheduleConfig is null");
        }
        
        super.checkMutableAndRaiseException();
        
        this.scheduleManagerConfig = scheduleConfig;
    }
    
    @JsonProperty("user_interface")
    public ManagerConfig getUserInterfaceConfig() {
        return this.userInterfaceManagerConfig;
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
    public ManagerConfig getDefaultKeyValueStoreConfig() {
        DriverInjection driverInjection = new DriverInjection();
        driverInjection.setDriverClass(IgniteKeyValueStoreDriver.class);
        
        IgniteKeyValueStoreDriverConfig driverConfiguration = new IgniteKeyValueStoreDriverConfig();
        
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
}
