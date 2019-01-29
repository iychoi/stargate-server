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

import stargate.commons.datasource.DataExportEntry;
import stargate.commons.datasource.SourceFileMetadata;

/**
 *
 * @author iychoi
 */
public class RecipeChunkGenerateEvent {
    private DataExportEntry dataExportEntry;
    private SourceFileMetadata sourceMetadata;
    private long offset;
    private int length;
    
    RecipeChunkGenerateEvent(DataExportEntry dataExportEntry, SourceFileMetadata sourceMetadata, long offset, int length) {
        this.dataExportEntry = dataExportEntry;
        this.sourceMetadata = sourceMetadata;
        this.offset = offset;
        this.length = length;
    }
    
    public DataExportEntry getDataExportEntry() {
        return this.dataExportEntry;
    }
    
    public SourceFileMetadata getSourceFileMetadata() {
        return this.sourceMetadata;
    }
    
    public long getOffset() {
        return this.offset;
    }
    
    public int getLength() {
        return this.length;
    }
    
    @Override
    public String toString() {
        return String.format("%s (Offset: %d, Length %d)", this.dataExportEntry.getSourceURI().toASCIIString(), this.offset, this.length);
    }
}
