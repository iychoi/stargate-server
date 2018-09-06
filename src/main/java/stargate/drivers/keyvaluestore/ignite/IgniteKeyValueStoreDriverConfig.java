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

import java.io.File;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.keyvaluestore.AbstractKeyValueStoreDriverConfig;

/**
 *
 * @author iychoi
 */
public class IgniteKeyValueStoreDriverConfig extends AbstractKeyValueStoreDriverConfig {
    
    private static final Log LOG = LogFactory.getLog(IgniteKeyValueStoreDriverConfig.class);
    
    public static IgniteKeyValueStoreDriverConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        JsonSerializer serializer = new JsonSerializer();
        return (IgniteKeyValueStoreDriverConfig) serializer.fromJsonFile(file, IgniteKeyValueStoreDriverConfig.class);
    }
    
    public static IgniteKeyValueStoreDriverConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        JsonSerializer serializer = new JsonSerializer();
        return (IgniteKeyValueStoreDriverConfig) serializer.fromJson(json, IgniteKeyValueStoreDriverConfig.class);
    }
    
    public IgniteKeyValueStoreDriverConfig() {
    }
}
