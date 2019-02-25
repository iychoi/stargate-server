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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.AbstractDataStoreDriver;
import stargate.commons.datastore.AbstractDataStoreDriverConfig;
import stargate.commons.datastore.AbstractQueue;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteDataStoreDriver extends AbstractDataStoreDriver {

    private static final Log LOG = LogFactory.getLog(IgniteDataStoreDriver.class);
    
    private IgniteDataStoreDriverConfig config;
    private IgniteDriver igniteDriver;
    private Map<String, IgniteKeyValueStore> kvStores = new HashMap<String, IgniteKeyValueStore>();
    private Map<String, IgniteQueue> queueStores = new HashMap<String, IgniteQueue>();
    
    public IgniteDataStoreDriver(AbstractDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof IgniteDataStoreDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of IgniteDataStoreDriverConfig");
        }
        
        this.config = (IgniteDataStoreDriverConfig) config;
    }
    
    public IgniteDataStoreDriver(AbstractDataStoreDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof IgniteDataStoreDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of IgniteDataStoreDriverConfig");
        }
        
        this.config = (IgniteDataStoreDriverConfig) config;
    }
    
    public IgniteDataStoreDriver(IgniteDataStoreDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
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
        if(this.igniteDriver != null && this.igniteDriver.isStarted()) {
            this.igniteDriver.uninit();
        }
        
        if(this.igniteDriver != null) {
            this.igniteDriver = null;
        }
        
        super.uninit();
    }
    
    @Override
    public synchronized AbstractKeyValueStore getKeyValueStore(String name, Class valueClass, EnumDataStoreProperty property) throws IOException, DriverNotInitializedException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(valueClass == null) {
            throw new IllegalArgumentException("valueClass is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        IgniteKeyValueStore store = this.kvStores.get(name);
        if(store == null) {
            Ignite ignite = this.igniteDriver.getIgnite();
            store = new IgniteKeyValueStore(this, ignite, name, valueClass, property);
            this.kvStores.put(name, store);
        }
        
        return store;
    }
    
    @Override
    public synchronized AbstractKeyValueStore getExpiringKeyValueStore(String name, Class valueClass, EnumDataStoreProperty property, TimeUnit timeunit, long timeval) throws IOException, DriverNotInitializedException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(valueClass == null) {
            throw new IllegalArgumentException("valueClass is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        IgniteKeyValueStore store = this.kvStores.get(name);
        if(store == null) {
            Ignite ignite = this.igniteDriver.getIgnite();
            store = new IgniteKeyValueStore(this, ignite, name, valueClass, property, timeunit, timeval);
            this.kvStores.put(name, store);
        }
        
        return store;
    }

    @Override
    public synchronized AbstractQueue getQueue(String name, Class valueClass, EnumDataStoreProperty property) throws IOException, DriverNotInitializedException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(valueClass == null) {
            throw new IllegalArgumentException("valueClass is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        IgniteQueue queue = this.queueStores.get(name);
        if(queue == null) {
            Ignite ignite = this.igniteDriver.getIgnite();
            queue = new IgniteQueue(this, ignite, name, valueClass, property);
            this.queueStores.put(name, queue);
        }
        
        return queue;
    }
}
