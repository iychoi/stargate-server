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
package stargate.drivers.keyvaluestore.localfs;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.keyvaluestore.AbstractKeyValueStore;
import stargate.commons.keyvaluestore.EnumKeyValueStoreProperty;
import stargate.commons.utils.ClassUtils;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class LocalFSKeyValueStore extends AbstractKeyValueStore {

    private static final Log LOG = LogFactory.getLog(LocalFSKeyValueStore.class);
    
    private LocalFSKeyValueStoreDriver driver;
    private String name;
    private Class valueClass;
    private EnumKeyValueStoreProperty property;
    
    LocalFSKeyValueStore(LocalFSKeyValueStoreDriver dirver, String name, Class valueClass, EnumKeyValueStoreProperty property) {
        this.driver = driver;
        this.name = name;
        this.valueClass = valueClass;
        this.property = property;
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
    public EnumKeyValueStoreProperty getProperty() {
        return this.property;
    }
    
    @Override
    public int size() {
        try {
            return this.driver.listKeys(this.name).size();
        } catch (IOException ex) {
            LOG.error(ex);
            return 0;
        }
    }

    @Override
    public boolean isEmpty() {
        try {
            return (this.driver.listKeys(this.name).size() == 0);
        } catch (IOException ex) {
            LOG.error(ex);
            return true;
        }
    }

    @Override
    public boolean containsKey(String key) {
        try {
            return this.driver.existKey(this.name, key);
        } catch (IOException ex) {
            LOG.error(ex);
            return false;
        }
    }

    @Override
    public Object get(String key) throws IOException {
        if(this.valueClass == byte[].class) {
            return this.driver.getBytes(this.name, key);
        } else if(this.valueClass == String.class) {
            return this.driver.getString(this.name, key);
        } else {
            // other complex classes
            String json = this.driver.getString(this.name, key);
            if(json == null) {
                return null;
            }
            
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
            this.driver.putBytes(this.name, key, (byte[]) value);
        } else if(this.valueClass == String.class) {
            this.driver.putString(this.name, key, (String) value);
        } else {
            // other complex classes
            JsonSerializer serializer = new JsonSerializer();
            String json = serializer.toJson(value);
            this.driver.putString(this.name, key, json);
        }
    }

    @Override
    public boolean putIfAbsent(String key, Object value) throws IOException {
        if(!this.driver.existKey(name, key)) {
            put(key, value);
            return true;
        }
        return false;
    }

    @Override
    public void remove(String key) throws IOException {
        this.driver.remove(this.name, key);
    }

    @Override
    public Collection<String> keys() throws IOException {
        return this.driver.listKeys(this.name);
    }

    @Override
    public void clear() throws IOException {
        this.driver.clearStore(this.name);
    }

    @Override
    public Map<String, Object> toMap() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        Collection<String> keys = this.driver.listKeys(this.name);
        for(String key : keys) {
            Object value = this.driver.getString(this.name, key);
            map.put(key, value);
        }
        return map;
    }
}
