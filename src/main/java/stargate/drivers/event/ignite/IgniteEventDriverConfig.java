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
package stargate.drivers.event.ignite;

import java.io.File;
import java.io.IOException;
import stargate.commons.event.AbstractEventDriverConfig;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class IgniteEventDriverConfig extends AbstractEventDriverConfig {
    
    public static IgniteEventDriverConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (IgniteEventDriverConfig) JsonSerializer.fromJsonFile(file, IgniteEventDriverConfig.class);
    }
    
    public static IgniteEventDriverConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (IgniteEventDriverConfig) JsonSerializer.fromJson(json, IgniteEventDriverConfig.class);
    }
    
    public IgniteEventDriverConfig() {
    }
}
