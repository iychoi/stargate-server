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
package stargate.drivers.userinterface.http;

import java.io.Closeable;
import stargate.commons.io.DiskBufferInputStream;

/**
 *
 * @author iychoi
 */
public class ChunkData implements Comparable<ChunkData>, Closeable {

    private DiskBufferInputStream is;
    private long offset;
    private long size;

    public ChunkData(DiskBufferInputStream is, long offset, long size) {
        if(is == null) {
            throw new IllegalArgumentException("is is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        
        this.is = is;
        this.offset = offset;
        this.size = size;
    }

    public DiskBufferInputStream getInputStream() {
        return this.is;
    }

    public void setInputStream(DiskBufferInputStream is) {
        if(is == null) {
            throw new IllegalArgumentException("is is null");
        }
        
        this.is = is;
    }

    public long getOffset() {
        return this.offset;
    }

    public void setOffset(long offset) {
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        this.offset = offset;
    }

    public long getSize() {
        return this.size;
    }

    public void setSize(long size) {
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        
        this.size = size;
    }

    @Override
    public int compareTo(ChunkData other) {
        if(other == null) {
            return -1;
        }
        
        return (int)(this.offset - other.offset);
    }
    
    @Override
    public void close() {
        if(this.is != null) {
            this.is.close();
            this.is = null;
        }
    }
}
