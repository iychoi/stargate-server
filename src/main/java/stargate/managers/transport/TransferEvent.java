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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.utils.JsonSerializer;


/**
 *
 * @author iychoi
 */
public class TransferEvent {

    public static final String ANY_NODES = "ANY";
    
    private TransferEventType eventType;
    private DataObjectURI uri;
    private long offset;
    private int length;
    private String hash;
    private List<String> dataSourceNodeNames = new ArrayList<String>();
    private String targetNodeName;
    
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
    
    TransferEvent(TransferEventType eventType, DataObjectURI uri, long offset, int length, String hash, Collection<String> dataSourceNodeNames, String targetNodeName) {
        if(eventType == null) {
            throw new IllegalArgumentException("eventType is null");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(length < 0) {
            throw new IllegalArgumentException("length is negative");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(dataSourceNodeNames == null || dataSourceNodeNames.isEmpty()) {
            throw new IllegalArgumentException("dataSourceNodeNames is null or empty");
        }
        
        this.eventType = eventType;
        this.uri = uri;
        this.offset = offset;
        this.length = length;
        this.hash = hash.toLowerCase();
        this.dataSourceNodeNames.addAll(dataSourceNodeNames);
        if(targetNodeName == null) {
            this.targetNodeName = ANY_NODES;
        } else {
            this.targetNodeName = targetNodeName;
        }
    }
    
    @JsonProperty("event_type")
    public TransferEventType getEventType() {
        return this.eventType;
    }
    
    @JsonProperty("event_type")
    public void setEventType(TransferEventType eventType) {
        this.eventType = eventType;
    }
    
    @JsonProperty("uri")
    public DataObjectURI getDataObjectURI() {
        return uri;
    }
    
    @JsonProperty("uri")
    public void setDataObjectURI(DataObjectURI uri) {
        this.uri = uri;
    }

    @JsonProperty("offset")
    public long getOffset() {
        return offset;
    }
    
    @JsonProperty("offset")
    public void setOffset(long offset) {
        this.offset = offset;
    }

    @JsonProperty("length")
    public int getLength() {
        return length;
    }
    
    @JsonProperty("length")
    public void setLength(int length) {
        this.length = length;
    }

    @JsonProperty("hash")
    public String getHash() {
        if(this.hash == null) {
            return null;
        }
        return this.hash.toLowerCase();
    }
    
    @JsonProperty("hash")
    public void setHash(String hash) {
        if(hash == null) {
            this.hash = null;
        } else {
            this.hash = hash.toLowerCase();
        }
    }
    
    @JsonProperty("data_source_node_names")
    public Collection<String> getDataSourceNodeNames() {
        return Collections.unmodifiableCollection(this.dataSourceNodeNames);
    }
    
    @JsonProperty("data_source_node_names")
    public void addDataSourceNodeNames(Collection<String> dataSourceNodeNames) {
        this.dataSourceNodeNames.addAll(dataSourceNodeNames);
    }
    
    @JsonIgnore
    public void addDataSourceNodeName(String dataSourceNodeName) {
        this.dataSourceNodeNames.add(dataSourceNodeName);
    }
    
    @JsonProperty("target_node_name")
    public String getTargetNodeName() {
        return this.targetNodeName;
    }
    
    @JsonProperty("target_node_names")
    public void setTargetNodeName(String targetNodeName) {
        this.targetNodeName = targetNodeName;
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        return "TransferEvent{" + "eventType=" + eventType + ", uri=" + uri + ", offset=" + offset + ", length=" + length + ", hash=" + hash + '}';
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
