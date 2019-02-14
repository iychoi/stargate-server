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
package stargate.managers.transport;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class TransferEvent {
    private TransferEventType eventType;
    private DataObjectURI uri;
    private String hash;
    
    public static TransferEvent createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (TransferEvent) JsonSerializer.fromJsonFile(file, TransferEvent.class);
    }
    
    public static TransferEvent createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (TransferEvent) JsonSerializer.fromJson(json, TransferEvent.class);
    }
    
    TransferEvent() {
    }
    
    public TransferEvent(TransferEventType eventType, DataObjectURI uri, String hash) {
        if(eventType == null) {
            throw new IllegalArgumentException("eventType is null");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        this.eventType = eventType;
        this.uri = uri;
        this.hash = hash;
    }
    
    @JsonProperty("event_type")
    public TransferEventType getEventType() {
        return this.eventType;
    }
    
    @JsonProperty("event_type")
    public void setEventType(TransferEventType eventType) {
        if(eventType == null) {
            throw new IllegalArgumentException("eventType is null");
        }
        
        this.eventType = eventType;
    }
    
    @JsonProperty("uri")
    public DataObjectURI getURI() {
        return this.uri;
    }
    
    @JsonProperty("uri")
    public void setURI(DataObjectURI uri) {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        this.uri = uri;
    }
    
    @JsonProperty("hash")
    public String getHash() {
        return this.hash;
    }
    
    @JsonProperty("hash")
    public void setHash(String hash) {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        this.hash = hash;
    }
    
    @JsonIgnore
    public String toJson() throws IOException {
        return JsonSerializer.toJson(this);
    }
    
    @JsonIgnore
    public void saveTo(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer.toJsonFile(file, this);
    }
}
