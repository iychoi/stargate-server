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
package stargate.managers.dataexport;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class DataExportEvent {
    private DataExportEventType eventType;
    private DataExportEntry dataExportEntry;
    
    public static DataExportEvent createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (DataExportEvent) JsonSerializer.fromJsonFile(file, DataExportEvent.class);
    }
    
    public static DataExportEvent createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (DataExportEvent) JsonSerializer.fromJson(json, DataExportEvent.class);
    }
    
    DataExportEvent() {
    }
    
    public DataExportEvent(DataExportEventType eventType, DataExportEntry entry) {
        if(eventType == null) {
            throw new IllegalArgumentException("eventType is null");
        }
        
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        this.eventType = eventType;
        this.dataExportEntry = entry;
    }
    
    @JsonProperty("event_type")
    public DataExportEventType getEventType() {
        return this.eventType;
    }
    
    @JsonProperty("event_type")
    public void setEventType(DataExportEventType eventType) {
        if(eventType == null) {
            throw new IllegalArgumentException("eventType is null");
        }
        
        this.eventType = eventType;
    }
    
    @JsonProperty("data_export_entry")
    public DataExportEntry getDataExportEntry() {
        return this.dataExportEntry;
    }
    
    @JsonProperty("data_export_entry")
    public void setDataExportEntry(DataExportEntry dataExportEntry) {
        if(dataExportEntry == null) {
            throw new IllegalArgumentException("dataExportEntry is null");
        }
        
        this.dataExportEntry = dataExportEntry;
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
