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
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.event.AbstractEventDriverConfig;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class IgniteEventDriverConfig extends AbstractEventDriverConfig {
    
    private static final int DEFAULT_EVENT_HANDLER_THREADS = 3;
    private static final int DEFAULT_EVENT_SENDER_THREADS = 1;
    
    private int eventHandlerThreads = DEFAULT_EVENT_HANDLER_THREADS;
    private int eventSenderThreads = DEFAULT_EVENT_SENDER_THREADS;
    
    
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
    
    @JsonProperty("event_handler_threads")
    public void setEventHandlerThreads(int threads) {
        super.checkMutableAndRaiseException();
        
        if(threads <= 0) {
            this.eventHandlerThreads = DEFAULT_EVENT_HANDLER_THREADS;
        } else {
            this.eventHandlerThreads = threads;
        }
    }
    
    @JsonProperty("event_handler_threads")
    public int getEventHandlerThreads() {
        return this.eventHandlerThreads;
    }
    
    @JsonProperty("event_sender_threads")
    public void setEventSenderThreads(int threads) {
        super.checkMutableAndRaiseException();
        
        if(threads <= 0) {
            this.eventSenderThreads = DEFAULT_EVENT_SENDER_THREADS;
        } else {
            this.eventSenderThreads = threads;
        }
    }
    
    @JsonProperty("event_sender_threads")
    public int getEventSenderThreads() {
        return this.eventSenderThreads;
    }
}
