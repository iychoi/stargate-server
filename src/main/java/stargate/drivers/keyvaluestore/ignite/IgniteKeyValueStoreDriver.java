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
package stargate.drivers.keyvaluestore.ignite;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.keyvaluestore.AbstractKeyValueStore;
import stargate.commons.keyvaluestore.AbstractKeyValueStoreDriver;
import stargate.commons.keyvaluestore.AbstractKeyValueStoreDriverConfig;
import stargate.commons.keyvaluestore.EnumKeyValueStoreProperty;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteKeyValueStoreDriver extends AbstractKeyValueStoreDriver {

    private static final Log LOG = LogFactory.getLog(IgniteKeyValueStoreDriver.class);
    
    private IgniteKeyValueStoreDriverConfig config;
    private IgniteDriver igniteDriver;
    private Map<String, IgniteKeyValueStore> stores = new HashMap<String, IgniteKeyValueStore>();
    
    public IgniteKeyValueStoreDriver(AbstractDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof IgniteKeyValueStoreDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of IgniteKeyValueStoreDriverConfig");
        }
        
        this.config = (IgniteKeyValueStoreDriverConfig) config;
    }
    
    public IgniteKeyValueStoreDriver(AbstractKeyValueStoreDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof IgniteKeyValueStoreDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of IgniteKeyValueStoreDriverConfig");
        }
        
        this.config = (IgniteKeyValueStoreDriverConfig) config;
    }
    
    public IgniteKeyValueStoreDriver(IgniteKeyValueStoreDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void init() throws IOException {
        super.init();
        
        LOG.info("Initializing Ignite Key Value Store Driver");
        
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
    public AbstractKeyValueStore getKeyValueStore(String name, Class valueClass, EnumKeyValueStoreProperty property) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(valueClass == null) {
            throw new IllegalArgumentException("valueClass is null");
        }
        
        IgniteKeyValueStore store = this.stores.get(name);
        if(store == null) {
            Ignite ignite = this.igniteDriver.getIgnite();
            store = new IgniteKeyValueStore(this, ignite, name, valueClass, property);
            this.stores.put(name, store);
        }
        
        return store;
    }
}
