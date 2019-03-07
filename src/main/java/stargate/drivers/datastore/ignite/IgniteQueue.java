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
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import stargate.commons.datastore.AbstractQueue;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.utils.ObjectSerializer;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteQueue extends AbstractQueue {

    private IgniteDataStoreDriver driver;
    private IgniteDriver igniteDriver;
    private Ignite ignite;
    private String name;
    private Class valueClass;
    private EnumDataStoreProperty property;
    
    private org.apache.ignite.IgniteQueue<byte[]> store;
    
    public IgniteQueue(IgniteDataStoreDriver driver, IgniteDriver igniteDriver, String name, Class valueClass, EnumDataStoreProperty property) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        if(igniteDriver == null) {
            throw new IllegalArgumentException("igniteDriver is null");
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
        this.igniteDriver = igniteDriver;
        this.ignite = igniteDriver.getIgnite();
        this.name = name;
        this.valueClass = valueClass;
        this.property = property;
        
        CollectionConfiguration cc = new CollectionConfiguration();
        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        
        this.store = this.ignite.queue(name, 0, cc);
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
        return this.store.size();
    }

    @Override
    public synchronized boolean isEmpty() {
        return this.store.isEmpty();
    }

    
    @Override
    public synchronized Object dequeue() throws IOException {
        byte[] bytes = this.store.take();
        if(bytes == null) {
            return null;
        }
        
        return ObjectSerializer.fromByteArray(bytes, this.valueClass);
    }

    @Override
    public synchronized void enqueue(Object value) throws IOException {
        if(value == null) {
            throw new IllegalArgumentException("value is null");
        }
        
        byte[] valueBytes = ObjectSerializer.toByteArray(value);
        this.store.put(valueBytes);
    }
    
    @Override
    public synchronized void clear() {
        this.store.clear();
    }

    @Override
    public synchronized List<Object> toList() throws IOException {
        List<Object> list = new ArrayList<Object>();
        
        Iterator<byte[]> iterator = this.store.iterator();
        while(iterator.hasNext()) {
            byte[] entry = iterator.next();
            if(entry == null) {
                list.add(null);
            } else {
                Object objValue = ObjectSerializer.fromByteArray(entry, this.valueClass);
                list.add(objValue);
            }
        }
        return list;
    }
}
