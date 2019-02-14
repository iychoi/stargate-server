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
package stargate.drivers.datastore.localfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.datastore.AbstractDataStoreLayoutEventHandler;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.utils.ObjectSerializer;

/**
 *
 * @author iychoi
 */
public class LocalFSKeyValueStore extends AbstractKeyValueStore {

    private static final Log LOG = LogFactory.getLog(LocalFSKeyValueStore.class);
    
    private LocalFSDataStoreDriver driver;
    private String name;
    private Class valueClass;
    private EnumDataStoreProperty property;
    private TimeUnit expiryTimeUnit;
    private long expiryTimeVal;
    private boolean allowKeyLock;
    private List<AbstractDataStoreLayoutEventHandler> layoutEventHandlers = new ArrayList<AbstractDataStoreLayoutEventHandler>();
    private final Object layoutEventHandlersSyncObj = new Object();
    
    public LocalFSKeyValueStore(LocalFSDataStoreDriver driver, String name, Class valueClass, EnumDataStoreProperty property) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
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
        this.name = name;
        this.valueClass = valueClass;
        this.property = property;
        this.expiryTimeUnit = TimeUnit.SECONDS;
        this.expiryTimeVal = 0;
        this.allowKeyLock = false;
    }

    public LocalFSKeyValueStore(LocalFSDataStoreDriver driver, String name, Class valueClass, EnumDataStoreProperty property, TimeUnit timeunit, long timeval, boolean allowKeyLock) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
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
        this.name = name;
        this.valueClass = valueClass;
        this.property = property;
        this.expiryTimeUnit = timeunit;
        this.expiryTimeVal = timeval;
        this.allowKeyLock = allowKeyLock;
    }
    
    @Override
    public String getName() {
        return this.name;
    }
    
    @Override
    public Class getValueClass() {
        return this.valueClass;
    }
    
    @Override
    public EnumDataStoreProperty getProperty() {
        return this.property;
    }
    
    private void cleanUp() throws IOException {
        Collection<String> keys = this.driver.listKeys(this.name);
        long currentTimestamp = DateTimeUtils.getTimestamp();
        long expirationMS = DateTimeUtils.getMilliseconds(this.expiryTimeUnit, this.expiryTimeVal);
        
        for(String key : keys) {
            byte[] bytes = this.driver.getBytes(this.name, key);
            if(bytes == null) {
                this.driver.remove(this.name, key);
            }
            
            if(bytes.length > 8) {
                long timestamp = byteArrayToLong(bytes);
                
                if(DateTimeUtils.timeElapsed(timestamp, currentTimestamp, expirationMS)) {
                    // expired
                    this.driver.remove(this.name, key);
                }
            } else {
                this.driver.remove(this.name, key);
            }
        }
    }
    
    @Override
    public int size() {
        try {
            cleanUp();
            return this.driver.listKeys(this.name).size();
        } catch (IOException ex) {
            LOG.error(ex);
            return 0;
        }
    }

    @Override
    public boolean isEmpty() {
        try {
            cleanUp();
            return (this.driver.listKeys(this.name).isEmpty());
        } catch (IOException ex) {
            LOG.error(ex);
            return true;
        }
    }

    @Override
    public boolean containsKey(String key) {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        try {
            cleanUp();
            return this.driver.existKey(this.name, key);
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }
    
    private void longToByteArray(long data, byte[] bytes) {
        bytes[0] = (byte) ((data >> 56) & 0x000000FF);
        bytes[1] = (byte) ((data >> 48) & 0x000000FF);
        bytes[2] = (byte) ((data >> 40) & 0x000000FF);
        bytes[3] = (byte) ((data >> 32) & 0x000000FF);
        bytes[4] = (byte) ((data >> 24) & 0x000000FF);
        bytes[5] = (byte) ((data >> 16) & 0x000000FF);
        bytes[6] = (byte) ((data >> 8) & 0x000000FF);
        bytes[7] = (byte) ((data >> 0) & 0x000000FF);
    }
    
    private long byteArrayToLong(byte[] bytes) {
        long value = 0;
        value += (long) (bytes[0] & 0x000000FF) << 56;
        value += (long) (bytes[1] & 0x000000FF) << 48;
        value += (long) (bytes[2] & 0x000000FF) << 40;
        value += (long) (bytes[3] & 0x000000FF) << 32;
        value += (bytes[4] & 0x000000FF) << 24;
        value += (bytes[5] & 0x000000FF) << 16;
        value += (bytes[6] & 0x000000FF) << 8;
        value += (bytes[7] & 0x000000FF);
        return value;
    }

    @Override
    public Object get(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        byte[] bytes = this.driver.getBytes(this.name, key);
        if(bytes == null) {
            this.driver.remove(this.name, key);
            return null;
        }
        
        if(bytes.length > 8) {
            long timestamp = byteArrayToLong(bytes);
            long currentTimestamp = DateTimeUtils.getTimestamp();
            long expirationMS = DateTimeUtils.getMilliseconds(this.expiryTimeUnit, this.expiryTimeVal);
            
            if(DateTimeUtils.timeElapsed(timestamp, currentTimestamp, expirationMS)) {
                // expired
                this.driver.remove(this.name, key);
                return null;
            } else {
                byte[] data = new byte[bytes.length - 8];
                System.arraycopy(bytes, 8, data, 0, bytes.length - 8);
                return ObjectSerializer.fromByteArray(data, this.valueClass);
            }
        } else {
            return null;
        }
        
    }

    @Override
    public void put(String key, Object value) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(value == null) {
            throw new IllegalArgumentException("value is null");
        }
        
        byte[] data = ObjectSerializer.toByteArray(value);
        byte[] bytes = new byte[data.length + 8];
        long currentTimestamp = DateTimeUtils.getTimestamp();
        
        longToByteArray(currentTimestamp, bytes);
        System.arraycopy(data, 0, bytes, 8, data.length);
        
        this.driver.putBytes(this.name, key, bytes);
        raiseEventForLayoutEventDataAdded(key, "*");
    }

    @Override
    public boolean putIfAbsent(String key, Object value) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(value == null) {
            throw new IllegalArgumentException("value is null");
        }
        
        this.cleanUp();
        
        if(!this.driver.existKey(this.name, key)) {
            put(key, value);
            return true;
        }
        
        return false;
    }
    
    @Override
    public String getNodeForData(String key) throws IOException {
        return "*";
    }

    @Override
    public void remove(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        this.driver.remove(this.name, key);
    }

    @Override
    public Collection<String> keys() throws IOException {
        this.cleanUp();
        
        return this.driver.listKeys(this.name);
    }

    @Override
    public void clear() throws IOException {
        this.driver.clearStore(this.name);
    }

    @Override
    public Map<String, Object> toMap() throws IOException {
        this.cleanUp();
        
        Map<String, Object> map = new HashMap<String, Object>();
        Collection<String> keys = this.driver.listKeys(this.name);
        for(String key : keys) {
            Object value = get(key);
            map.put(key, value);
        }
        return map;
    }
    
    @Override
    public Lock getKeyLock(String key) {
        //TODO: Implement this
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public void addLayoutEventHandler(AbstractDataStoreLayoutEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        synchronized(this.layoutEventHandlersSyncObj) {
            this.layoutEventHandlers.add(eventHandler);
        }
    }

    @Override
    public void removeLayoutEventHandler(AbstractDataStoreLayoutEventHandler eventHandler) {
        if(eventHandler == null) {
            throw new IllegalArgumentException("eventHandler is null");
        }
        
        synchronized(this.layoutEventHandlersSyncObj) {
            this.layoutEventHandlers.remove(eventHandler);
        }
    }
    
    private void raiseEventForLayoutEventDataAdded(String key, String node) {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is null or empty");
        }
        
        synchronized(this.layoutEventHandlersSyncObj) {
            for(AbstractDataStoreLayoutEventHandler handler: this.layoutEventHandlers) {
                handler.added(this, key, node);
            }
        }
    }
}
