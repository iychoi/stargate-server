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
package stargate.managers.policy;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import stargate.commons.config.AbstractImmutableConfig;
import stargate.commons.utils.ConfigSerializer;

/**
 *
 * @author iychoi
 */
public abstract class AbstractPolicy extends AbstractImmutableConfig {
    
    private static final Log LOG = LogFactory.getLog(AbstractPolicy.class);
    
    protected PolicyManager manager;
    protected Map<String, Object> store = new HashMap<String, Object>();
    
    @JsonIgnore
    public void setManager(PolicyManager manager) {
        this.manager = manager;
    }
    
    @JsonIgnore
    public PolicyManager getManager() {
        return this.manager;
    }
    
    @JsonIgnore
    public abstract String getGroupName();
    
    @JsonIgnore
    public abstract Collection<String> getKeyList();
    
    @JsonIgnore
    public abstract AbstractPolicy clone();
    
    @JsonIgnore
    public void makeLocal() {
        if(this.manager == null) {
            return;
        }
        
        Collection<String> keys = getKeyList();
        
        for(String key : keys) {
            try {
                Object val = this.manager.get(getKey(key));
                this.store.put(key, val);
            } catch (IOException ex) {
                LOG.error(ex);
            }
        }
        
        this.manager = null;
    }
    
    @JsonIgnore
    protected String getKey(String key) {
        return this.getGroupName() + "." + key;
    }
    
    @JsonIgnore
    public Object get(String key) throws IOException {
        if(this.manager == null) {
            return this.store.get(key);
        } else {
            return this.manager.get(getKey(key));
        }
    }
    
    @JsonIgnore
    public Object get(String key, Object defaultValue) {
        Object val = null;
        if(this.manager == null) {
            val = this.store.get(key);
        } else {
            try {
                val = this.manager.get(getKey(key));
            } catch (IOException ex) {
                LOG.error(val);
                val = null;
            }
        }
        
        if(val == null) {
            val = defaultValue;
        }
        return val;
    }
    
    @JsonIgnore
    public void put(String key, Object value) throws IOException {
        if(this.manager == null) {
            this.store.put(key, value);
        } else {
            this.manager.put(getKey(key), value);
        }
    }
    
    @JsonIgnore
    public void saveTo(Map map) throws IOException {
        if(map == null) {
            throw new IllegalArgumentException("map is null");
        }
        
        ConfigSerializer serializer = new ConfigSerializer();
        Map<String, Object> mymap = serializer.toMap(this);
        for(Entry<String, Object> entry: mymap.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
    }
}
