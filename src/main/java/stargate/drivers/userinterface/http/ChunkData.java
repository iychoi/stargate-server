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
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author iychoi
 */
public class ChunkData implements Comparable<ChunkData>, Closeable {

    private InputStream is;
    private int currentOffsetInChunk;
    private long chunkStartOffset;
    private long chunkSize;
    
    public ChunkData(InputStream is, int currentOffsetInChunk, long chunkStartOffset, long chunkSize) {
        if(is == null) {
            throw new IllegalArgumentException("is is null");
        }
        
        if(currentOffsetInChunk < 0) {
            throw new IllegalArgumentException("currentOffsetInChunk is negative");
        }
        
        if(chunkStartOffset < 0) {
            throw new IllegalArgumentException("chunkStartOffset is negative");
        }
        
        if(chunkSize < 0) {
            throw new IllegalArgumentException("chunkSize is negative");
        }
        
        this.is = is;
        this.currentOffsetInChunk = currentOffsetInChunk;
        this.chunkStartOffset = chunkStartOffset;
        this.chunkSize = chunkSize;
    }

    public InputStream getInputStream() {
        return this.is;
    }

    public void setInputStream(InputStream is) {
        if(is == null) {
            throw new IllegalArgumentException("is is null");
        }
        
        this.is = is;
    }
    
    public int getCurrentOffsetInChunk() {
        return this.currentOffsetInChunk;
    }

    public void setCurrentOffsetInChunk(int currentOffsetInChunk) {
        if(currentOffsetInChunk < 0) {
            throw new IllegalArgumentException("currentOffsetInChunk is negative");
        }
        
        this.currentOffsetInChunk = currentOffsetInChunk;
    }
    
    public void increaseCurrentOffset(int skip) {
        if(skip < 0) {
            throw new IllegalArgumentException("skip is negative");
        }
        
        this.currentOffsetInChunk += skip;
    }

    public long getChunkStartOffset() {
        return this.chunkStartOffset;
    }

    public void setChunkStartOffset(long chunkStartOffset) {
        if(chunkStartOffset < 0) {
            throw new IllegalArgumentException("chunkStartOffset is negative");
        }
        
        this.chunkStartOffset = chunkStartOffset;
    }

    public long getChunkSize() {
        return this.chunkSize;
    }

    public void setChunkSize(long chunkSize) {
        if(chunkSize < 0) {
            throw new IllegalArgumentException("chunkSize is negative");
        }
        
        this.chunkSize = chunkSize;
    }
    
    public boolean containsOffset(long offset) {
        if (this.chunkStartOffset <= offset &&
            (this.chunkStartOffset + this.chunkSize) > offset) {
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(ChunkData other) {
        if(other == null) {
            return -1;
        }
        
        return (int)(this.chunkStartOffset - other.chunkStartOffset);
    }
    
    @Override
    public void close() {
        if(this.is != null) {
            try {
                this.is.close();
            } catch (IOException ex) {
            } finally {
                this.is = null;
            }
        }
    }
}
