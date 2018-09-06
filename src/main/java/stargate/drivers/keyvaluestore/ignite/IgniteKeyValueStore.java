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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import javax.cache.Cache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import stargate.commons.keyvaluestore.AbstractKeyValueStore;
import stargate.commons.keyvaluestore.EnumKeyValueStoreProperty;
import stargate.commons.utils.ClassUtils;
import stargate.commons.utils.JsonSerializer;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteKeyValueStore extends AbstractKeyValueStore {

    private static final Log LOG = LogFactory.getLog(IgniteKeyValueStore.class);
    
    private IgniteKeyValueStoreDriver driver;
    private Ignite ignite;
    private String name;
    private Class valueClass;
    private EnumKeyValueStoreProperty property;
    
    private IgniteCache<String, byte[]> store;
    
    IgniteKeyValueStore(IgniteKeyValueStoreDriver driver, Ignite ignite, String name, Class valueClass, EnumKeyValueStoreProperty property) {
        this.driver = driver;
        this.ignite = ignite;
        this.name = name;
        this.valueClass = valueClass;
        this.property = property;
        
        CacheConfiguration<String, byte[]> cc = new CacheConfiguration<String, byte[]>();
        if(EnumKeyValueStoreProperty.isDistributed(property)) {
            cc.setCacheMode(CacheMode.PARTITIONED);
        } else if(EnumKeyValueStoreProperty.isReplciated(property)) {
            cc.setCacheMode(CacheMode.REPLICATED);
        }
        
        if(EnumKeyValueStoreProperty.isPersistent(property)) {
            cc.setDataRegionName(IgniteDriver.PERSISTENT_REGION_NAME);
        } else if(EnumKeyValueStoreProperty.isVolatile(property)) {
            cc.setDataRegionName(IgniteDriver.VOLATILE_REGION_NAME);
        }
        
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cc.setEvictionPolicy(null);
        cc.setCopyOnRead(false);
        cc.setOnheapCacheEnabled(true);
        cc.setReadFromBackup(true);
        cc.setName(name);
        
        this.store = this.ignite.getOrCreateCache(cc);
    }
    
    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public EnumKeyValueStoreProperty getProperty() {
        return this.property;
    }
    
    @Override
    public Class getValueClass() {
        return this.valueClass;
    }
    
    @Override
    public int size() {
        CachePeekMode[] cpms = { CachePeekMode.ALL };
        return this.store.size(cpms);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(String key) {
        return this.store.containsKey(key);
    }

    @Override
    public Object get(String key) throws IOException {
        if(this.valueClass == byte[].class) {
            return this.store.get(key);
        } else if(this.valueClass == String.class) {
            byte[] bytes = this.store.get(key);
            if(bytes == null) {
                return null;
            }
            return new String(bytes);
        } else {
            // other complex classes
            byte[] bytes = this.store.get(key);
            if(bytes == null) {
                return null;
            }
            String json = new String(bytes);
            
            // cast
            try {
                return ClassUtils.invokeCreateInstance(this.valueClass, json);
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }
    }

    @Override
    public void put(String key, Object value) throws IOException {
        if(this.valueClass == byte[].class) {
            byte[] valueBytes = (byte[]) value;
            this.store.put(key, valueBytes);
        } else if(this.valueClass == String.class) {
            String valueString = (String) value;
            this.store.put(key, valueString.getBytes());
        } else {
            // other complex classes
            JsonSerializer serializer = new JsonSerializer();
            String json = serializer.toJson(value);
            this.store.put(key, json.getBytes());
        }
    }

    @Override
    public boolean putIfAbsent(String key, Object value) throws IOException {
        if(!this.store.containsKey(key)) {
            put(key, value);
            return true;
        }
        return false;
    }

    @Override
    public void remove(String key) throws IOException {
        this.store.remove(key);
    }

    @Override
    public void clear() {
        this.store.clear();
    }

    @Override
    public Map<String, Object> toMap() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        
        Iterator<Cache.Entry<String, byte[]>> iterator = this.store.iterator();
        while(iterator.hasNext()) {
            Cache.Entry<String, byte[]> entry = iterator.next();
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    @Override
    public Collection<String> keys() throws IOException {
        List<String> keys = new ArrayList<String>();
        
        Iterator<Cache.Entry<String, byte[]>> iterator = this.store.iterator();
        while(iterator.hasNext()) {
            Cache.Entry<String, byte[]> entry = iterator.next();
            keys.add(entry.getKey());
        }
        return keys;
    }
}
