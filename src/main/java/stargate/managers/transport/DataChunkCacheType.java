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

/**
 *
 * @author iychoi
 */
public enum DataChunkCacheType {
    DATA_CHUNK_CACHE_PENDING (0),
    DATA_CHUNK_CACHE_PRESENT (1);
    
    private int numVal;
    
    DataChunkCacheType(int numVal) {
        this.numVal = numVal;
    }
    
    public int getNumVal() {
        return this.numVal;
    }
    
    public static DataChunkCacheType fromNumVal(int numVal) {
        for(DataChunkCacheType type : DataChunkCacheType.values()) {
            if(type.getNumVal() == numVal) {
                return type;
            }
        }
        return null;
    }
}
