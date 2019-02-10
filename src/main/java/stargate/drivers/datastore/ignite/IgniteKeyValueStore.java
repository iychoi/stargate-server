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
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.utils.ObjectSerializer;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteKeyValueStore extends AbstractKeyValueStore {

    private static final Log LOG = LogFactory.getLog(IgniteKeyValueStore.class);
    
    private IgniteDataStoreDriver driver;
    private Ignite ignite;
    private String name;
    private Class valueClass;
    private EnumDataStoreProperty property;
    private TimeUnit expiryTimeUnit;
    private long expiryTimeValue;
    
    private IgniteCache<String, byte[]> store;
    
    public IgniteKeyValueStore(IgniteDataStoreDriver driver, Ignite ignite, String name, Class valueClass, EnumDataStoreProperty property) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        if(ignite == null) {
            throw new IllegalArgumentException("ignite is null");
        }
        
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(valueClass == null) {
            throw new IllegalArgumentException("valueClass is null");
        }
        
        if(property == null) {
            throw new IllegalArgumentException("property is null");
        }
        
        this.driver = driver;
        this.ignite = ignite;
        this.name = name;
        this.valueClass = valueClass;
        this.property = property;
        this.expiryTimeUnit = TimeUnit.SECONDS;
        this.expiryTimeValue = 0;
        
        CacheConfiguration<String, byte[]> cc = new CacheConfiguration<String, byte[]>();
        if(EnumDataStoreProperty.isDistributed(property)) {
            cc.setCacheMode(CacheMode.PARTITIONED);
        } else if(EnumDataStoreProperty.isReplciated(property)) {
            cc.setCacheMode(CacheMode.REPLICATED);
        }
        
        if(EnumDataStoreProperty.isPersistent(property)) {
            cc.setDataRegionName(IgniteDriver.PERSISTENT_REGION_NAME);
        } else if(EnumDataStoreProperty.isVolatile(property)) {
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
    
    public IgniteKeyValueStore(IgniteDataStoreDriver driver, Ignite ignite, String name, Class valueClass, EnumDataStoreProperty property, TimeUnit timeunit, long timeval) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        if(ignite == null) {
            throw new IllegalArgumentException("ignite is null");
        }
        
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(valueClass == null) {
            throw new IllegalArgumentException("valueClass is null");
        }
        
        if(property == null) {
            throw new IllegalArgumentException("property is null");
        }
        
        if(timeunit == null) {
            throw new IllegalArgumentException("timeunit is null");
        }
        
        if(timeval < 0) {
            throw new IllegalArgumentException("timeval is negative");
        }
                
        this.driver = driver;
        this.ignite = ignite;
        this.name = name;
        this.valueClass = valueClass;
        this.property = property;
        this.expiryTimeUnit = timeunit;
        this.expiryTimeValue = timeval;
        
        CacheConfiguration<String, byte[]> cc = new CacheConfiguration<String, byte[]>();
        if(EnumDataStoreProperty.isDistributed(property)) {
            cc.setCacheMode(CacheMode.PARTITIONED);
        } else if(EnumDataStoreProperty.isReplciated(property)) {
            cc.setCacheMode(CacheMode.REPLICATED);
        }
        
        if(EnumDataStoreProperty.isPersistent(property)) {
            cc.setDataRegionName(IgniteDriver.PERSISTENT_REGION_NAME);
        } else if(EnumDataStoreProperty.isVolatile(property)) {
            cc.setDataRegionName(IgniteDriver.VOLATILE_REGION_NAME);
        }
        
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cc.setEvictionPolicy(null);
        cc.setCopyOnRead(false);
        cc.setOnheapCacheEnabled(true);
        cc.setReadFromBackup(true);
        cc.setName(name);
        
        Duration duration = new Duration(timeunit, timeval);
        CreatedExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(duration);
        this.store = this.ignite.getOrCreateCache(cc).withExpiryPolicy(expiryPolicy);
    }
    
    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public EnumDataStoreProperty getProperty() {
        return this.property;
    }
    
    @Override
    public Class getValueClass() {
        return this.valueClass;
    }
    
    @Override
    public synchronized int size() {
        CachePeekMode[] cpms = { CachePeekMode.ALL };
        return this.store.size(cpms);
    }

    @Override
    public synchronized boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public synchronized boolean containsKey(String key) {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        return this.store.containsKey(key);
    }

    @Override
    public synchronized Object get(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        byte[] bytes = this.store.get(key);
        if(bytes == null) {
            return null;
        }
        
        return ObjectSerializer.fromByteArray(bytes, this.valueClass);
    }

    @Override
    public synchronized void put(String key, Object value) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(value == null) {
            throw new IllegalArgumentException("value is null");
        }
        
        byte[] valueBytes = ObjectSerializer.toByteArray(value);
        this.store.put(key, valueBytes);
    }

    @Override
    public synchronized boolean putIfAbsent(String key, Object value) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(value == null) {
            throw new IllegalArgumentException("value is null");
        }
        
        if(!this.store.containsKey(key)) {
            put(key, value);
            return true;
        }
        return false;
    }

    @Override
    public synchronized void remove(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        this.store.remove(key);
    }

    @Override
    public synchronized void clear() {
        this.store.clear();
    }

    @Override
    public synchronized Map<String, Object> toMap() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        
        Iterator<Cache.Entry<String, byte[]>> iterator = this.store.iterator();
        while(iterator.hasNext()) {
            Cache.Entry<String, byte[]> entry = iterator.next();
            if(entry.getValue() == null) {
                map.put(entry.getKey(), null);
            } else {
                Object objValue = ObjectSerializer.fromByteArray(entry.getValue(), this.valueClass);
                map.put(entry.getKey(), objValue);
            }
        }
        return map;
    }

    @Override
    public synchronized Collection<String> keys() throws IOException {
        List<String> keys = new ArrayList<String>();
        
        Iterator<Cache.Entry<String, byte[]>> iterator = this.store.iterator();
        while(iterator.hasNext()) {
            Cache.Entry<String, byte[]> entry = iterator.next();
            keys.add(entry.getKey());
        }
        return keys;
    }
}
