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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class TransportPolicy extends AbstractPolicy {

    // scan interval for data updates
    public static final int DEFAULT_PREFETCH_THREAD_NUM = 5;
    public static final String PREFETCH_THREAD_NUM = "prefetch_thread_num";
    
    public static TransportPolicy createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        return (TransportPolicy) JsonSerializer.fromJsonFile(file, TransportPolicy.class);
    }
    
    public static TransportPolicy createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (TransportPolicy) JsonSerializer.fromJson(json, TransportPolicy.class);
    }
    
    TransportPolicy() {
    }
    
    @JsonProperty("prefetch_thread_num")
    public int getPrefetchThreadNum() {
        return (int) this.get(PREFETCH_THREAD_NUM, DEFAULT_PREFETCH_THREAD_NUM);
    }
    
    @JsonProperty("prefetch_thread_num")
    public void setPrefetchThreadNum(int num) throws IOException {
        long val = Math.max(1, num);
        this.put(PREFETCH_THREAD_NUM, val);
    }
    
    @Override
    @JsonIgnore
    public Collection<String> getKeyList() {
        List<String> keys = new ArrayList<String>();
        keys.add(PREFETCH_THREAD_NUM);
        return Collections.unmodifiableCollection(keys);
    }
    
    @JsonIgnore
    public String getGroupName() {
        return "transport";
    }
    
    @JsonIgnore
    public TransportPolicy clone() {
        TransportPolicy newPolicy = new TransportPolicy();
        newPolicy.manager = this.manager;
        newPolicy.store.putAll(this.store);
        return newPolicy;
    }
}
