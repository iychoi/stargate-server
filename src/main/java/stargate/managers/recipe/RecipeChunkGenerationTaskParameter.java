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
package stargate.managers.recipe;

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.datasource.SourceFileMetadata;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class RecipeChunkGenerationTaskParameter {
    private DataExportEntry dataExportEntry;
    private SourceFileMetadata sourceMetadata;
    private long offset;
    private int length;
    
    public static RecipeChunkGenerationTaskParameter createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (RecipeChunkGenerationTaskParameter) JsonSerializer.fromJsonFile(file, RecipeChunkGenerationTaskParameter.class);
    }
    
    public static RecipeChunkGenerationTaskParameter createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (RecipeChunkGenerationTaskParameter) JsonSerializer.fromJson(json, RecipeChunkGenerationTaskParameter.class);
    }
    
    RecipeChunkGenerationTaskParameter() {
    }
    
    public RecipeChunkGenerationTaskParameter(DataExportEntry dataExportEntry, SourceFileMetadata sourceMetadata, long offset, int length) {
        if(dataExportEntry == null) {
            throw new IllegalArgumentException("dataExportEntry is null");
        }
        
        if(sourceMetadata == null) {
            throw new IllegalArgumentException("sourceMetadata is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(length < 0) {
            throw new IllegalArgumentException("length is negative");
        }
        
        this.dataExportEntry = dataExportEntry;
        this.sourceMetadata = sourceMetadata;
        this.offset = offset;
        this.length = length;
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
    
    @JsonProperty("source_file_metadata")
    public SourceFileMetadata getSourceFileMetadata() {
        return this.sourceMetadata;
    }
    
    @JsonProperty("source_file_metadata")
    public void setSourceFileMetadata(SourceFileMetadata sourceMetadata) {
        if(sourceMetadata == null) {
            throw new IllegalArgumentException("sourceMetadata is null");
        }
        
        this.sourceMetadata = sourceMetadata;
    }
    
    @JsonProperty("offset")
    public long getOffset() {
        return this.offset;
    }
    
    @JsonProperty("offset")
    public void setOffset(long offset) {
        this.offset = offset;
    }
    
    @JsonProperty("length")
    public int getLength() {
        return this.length;
    }
    
    @JsonProperty("length")
    public void setLength(int length) {
        this.length = length;
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        return String.format("%s (Offset: %d, Length %d)", this.dataExportEntry.getSourceURI().toASCIIString(), this.offset, this.length);
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
