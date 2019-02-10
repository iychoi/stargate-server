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
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class TransferResult {
    private TransferEvent event;
    private String dataSourceNodeName;
    private long transferTime;
    private boolean success;
    
    
    public static TransferResult createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (TransferResult) JsonSerializer.fromJsonFile(file, TransferResult.class);
    }
    
    public static TransferResult createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (TransferResult) JsonSerializer.fromJson(json, TransferResult.class);
    }
    
    TransferResult() {
    }
    
    public TransferResult(TransferEvent event, String dataSourceNodeName, long transferTime, boolean success) {
        if(event == null) {
            throw new IllegalArgumentException("event is null");
        }
        
        if(dataSourceNodeName == null || dataSourceNodeName.isEmpty()) {
            throw new IllegalArgumentException("dataSourceNodeName is null or empty");
        }
        
        if(transferTime < 0) {
            throw new IllegalArgumentException("transferTime is negative");
        }
        
        this.event = event;
        this.dataSourceNodeName = dataSourceNodeName;
        this.transferTime = transferTime;
        this.success = success;
    }
    
    @JsonProperty("event")
    public TransferEvent getEvent() {
        return this.event;
    }
    
    @JsonProperty("event")
    public void setEvent(TransferEvent event) {
        this.event = event;
    }
    
    @JsonProperty("data_source_node_name")
    public String getDataSourceNodeName() {
        return this.dataSourceNodeName;
    }
    
    @JsonProperty("data_source_node_name")
    public void setDataSourceNodeName(String dataSourceNodeName) {
        this.dataSourceNodeName = dataSourceNodeName;
    }
    
    @JsonProperty("transfer_time")
    public long getTransferTime() {
        return this.transferTime;
    }
    
    @JsonProperty("transfer_time")
    public void setTransferTime(long transferTime) {
        this.transferTime = transferTime;
    }
    
    @JsonProperty("success")
    public boolean getSuccess() {
        return this.success;
    }
    
    @JsonProperty("success")
    public void setSuccess(boolean success) {
        this.success = success;
    }
    
    @Override
    @JsonIgnore
    public int hashCode() {
        int hash = 5;
        hash = 17 * hash + Objects.hashCode(this.event);
        hash = 17 * hash + Objects.hashCode(this.dataSourceNodeName);
        hash = 17 * hash + Objects.hashCode(this.transferTime);
        hash = 17 * hash + Objects.hashCode(this.success);
        return hash;
    }

    @Override
    @JsonIgnore
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TransferResult other = (TransferResult) obj;
        if (!Objects.equals(this.event, other.event)) {
            return false;
        }
        if (!this.dataSourceNodeName.equals(other.dataSourceNodeName)) {
            return false;
        }
        if (this.transferTime != other.transferTime) {
            return false;
        }
        if (this.success != other.success) {
            return false;
        }
        return true;
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        return "TransferResult{" + "event=" + event + ", dataSourceNodeName=" + dataSourceNodeName + ", transferTime=" + transferTime + '}';
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
