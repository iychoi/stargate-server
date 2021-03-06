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
package stargate.drivers.datastore.ignite;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.datastore.AbstractBigKeyValueStore;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.AbstractDataStoreDriver;
import stargate.commons.datastore.AbstractDataStoreDriverConfig;
import stargate.commons.datastore.AbstractLock;
import stargate.commons.datastore.AbstractQueue;
import stargate.commons.datastore.DataStoreProperties;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.drivers.ignite.IgniteDriver;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.datastore.DataStoreManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class IgniteDataStoreDriver extends AbstractDataStoreDriver {

    private static final Log LOG = LogFactory.getLog(IgniteDataStoreDriver.class);

    private IgniteDataStoreDriverConfig config;
    private IgniteDriver igniteDriver;
    private int partSize;
    private Map<String, AbstractKeyValueStore> kvStores = new HashMap<String, AbstractKeyValueStore>();
    private Map<String, AbstractBigKeyValueStore> bigKVStores = new HashMap<String, AbstractBigKeyValueStore>();
    private Map<String, IgniteQueue> queueStores = new HashMap<String, IgniteQueue>();
    private Map<String, IgniteLock> lockStores = new HashMap<String, IgniteLock>();

    public IgniteDataStoreDriver(AbstractDriverConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config is null");
        }

        if (!(config instanceof IgniteDataStoreDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of IgniteDataStoreDriverConfig");
        }

        this.config = (IgniteDataStoreDriverConfig) config;
        this.partSize = this.config.getPartSize();
    }

    public IgniteDataStoreDriver(AbstractDataStoreDriverConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config is null");
        }

        if (!(config instanceof IgniteDataStoreDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of IgniteDataStoreDriverConfig");
        }

        this.config = (IgniteDataStoreDriverConfig) config;
        this.partSize = this.config.getPartSize();
    }

    public IgniteDataStoreDriver(IgniteDataStoreDriverConfig config) {
        if (config == null) {
            throw new IllegalArgumentException("config is null");
        }

        this.config = config;
        this.partSize = this.config.getPartSize();
    }

    @Override
    public synchronized void init() throws IOException {
        super.init();

        LOG.debug("Initializing Ignite Key Value Store Driver");

        this.igniteDriver = IgniteDriver.getInstance();
        this.igniteDriver.init();
    }

    @Override
    public synchronized void uninit() throws IOException {
        if (this.igniteDriver != null && this.igniteDriver.isStarted()) {
            this.igniteDriver.uninit();
        }

        if (this.igniteDriver != null) {
            this.igniteDriver = null;
        }

        super.uninit();
    }

    private DataStoreManager getDataStoreManager() {
        if (this.manager == null) {
            throw new IllegalStateException("manager is not initialized");
        }

        return (DataStoreManager) this.manager;
    }

    private StargateService getStargateService() {
        DataStoreManager dataStoreManager = getDataStoreManager();
        StargateService stargateService = (StargateService) dataStoreManager.getService();
        return stargateService;
    }

    @Override
    public int getPartSize() {
        return this.partSize;
    }

    @Override
    public synchronized AbstractKeyValueStore getKeyValueStore(String name, Class valueClass, DataStoreProperties properties) throws IOException, DriverNotInitializedException {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }

        if (valueClass == null) {
            throw new IllegalArgumentException("valueClass is null");
        }

        if (!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }

        AbstractKeyValueStore store = this.kvStores.get(name);
        if (store == null) {
            store = new IgniteKeyValueStore(this, this.config, this.igniteDriver, name, valueClass, properties);
            this.kvStores.put(name, store);
        }

        return store;
    }

    @Override
    public AbstractBigKeyValueStore getBigKeyValueStore(String name, DataStoreProperties properties) throws IOException, DriverNotInitializedException {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }

        if (!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }

        AbstractBigKeyValueStore store = this.bigKVStores.get(name);
        if (store == null) {
            try {
                StargateService service = getStargateService();
                ClusterManager clusterManager = service.getClusterManager();
                Cluster localCluster = clusterManager.getLocalCluster();
                Collection<String> dataNodeNames = localCluster.getDataNodeNames();
                store = new IgniteDiskBackedBigKeyValueStore(this, this.config, this.igniteDriver, name, dataNodeNames, properties);
                this.bigKVStores.put(name, store);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error("Manager not instantiated");
                throw new DriverNotInitializedException(ex);
            }
        }

        return store;
    }

    @Override
    public synchronized AbstractQueue getQueue(String name, Class valueClass, DataStoreProperties properties) throws IOException, DriverNotInitializedException {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }

        if (valueClass == null) {
            throw new IllegalArgumentException("valueClass is null");
        }

        if (!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }

        IgniteQueue queue = this.queueStores.get(name);
        if (queue == null) {
            queue = new IgniteQueue(this, this.config, this.igniteDriver, name, valueClass, properties);
            this.queueStores.put(name, queue);
        }

        return queue;
    }

    @Override
    public AbstractLock getLock(String name) throws IOException, DriverNotInitializedException {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }

        IgniteLock lock = this.lockStores.get(name);
        if (lock == null) {
            lock = new IgniteLock(this, this.config, this.igniteDriver, name);
            this.lockStores.put(name, lock);
        }

        return lock;
    }
}
