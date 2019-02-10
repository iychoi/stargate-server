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
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.datastore.AbstractQueue;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.utils.ObjectSerializer;

/**
 *
 * @author iychoi
 */
public class LocalFSQueue extends AbstractQueue {

    private static final Log LOG = LogFactory.getLog(LocalFSQueue.class);
    
    private LocalFSDataStoreDriver driver;
    private String name;
    private Class valueClass;
    private EnumDataStoreProperty property;
    
    public LocalFSQueue(LocalFSDataStoreDriver driver, String name, Class valueClass, EnumDataStoreProperty property) {
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
    public Object dequeue() throws IOException {
        List<String> orderedKeys = new ArrayList<String>();
        Collection<String> keys = this.driver.listKeys(this.name);
        Collections.addAll(keys);
        Collections.sort(orderedKeys);

        String key = orderedKeys.get(0);
        byte[] bytes = this.driver.getBytes(this.name, key);
        if(bytes == null) {
            return null;
        }
        return ObjectSerializer.fromByteArray(bytes, this.valueClass);
    }

    @Override
    public void enqueue(Object value) throws IOException {
        if(value == null) {
            throw new IllegalArgumentException("value is null");
        }
        
        List<String> orderedKeys = new ArrayList<String>();
        Collection<String> keys = this.driver.listKeys(this.name);
        Collections.addAll(keys);
        Collections.sort(orderedKeys);

        String lastKey = orderedKeys.get(orderedKeys.size() - 1);
        long newKeyNum = Long.parseLong(lastKey) + 1;
        String key = "" + newKeyNum;
        byte[] bytes = ObjectSerializer.toByteArray(value);
        this.driver.putBytes(this.name, key, bytes);
    }

    @Override
    public void clear() throws IOException {
        this.driver.clearStore(this.name);
    }

    @Override
    public List<Object> toList() throws IOException {
        List<Object> list = new ArrayList<Object>();
        List<String> orderedKeys = new ArrayList<String>();
        Collection<String> keys = this.driver.listKeys(this.name);
        Collections.addAll(keys);
        Collections.sort(orderedKeys);

        for(String key : orderedKeys) {
            byte[] bytes = this.driver.getBytes(this.name, key);
            if(bytes == null) {
                return null;
            }
            Object value = ObjectSerializer.fromByteArray(bytes, this.valueClass);
            list.add(value);
        }
        return list;
    }
}
