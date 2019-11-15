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
package stargate.drivers.datastore.ignite;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;
import stargate.commons.datastore.BigKeyValueStoreMetadata;
import stargate.commons.utils.ByteArray;

/**
 *
 * @author iychoi
 */
public class IgniteCacheInputStream extends InputStream {

    private IgniteBigKeyValueStore store;
    private IgniteDataStoreDriverConfig config;
    private IgniteCache<String, ByteArray> cache;
    private BigKeyValueStoreMetadata metadata;
    private Lock lock;
    private int currentPartNo;
    private byte[] chunkData;
    private long offset;
    private long size;
    private int chunkSize;
    
    IgniteCacheInputStream(IgniteBigKeyValueStore store, IgniteDataStoreDriverConfig config, IgniteCache<String, ByteArray> cache, BigKeyValueStoreMetadata metadata) {
        if(store == null) {
            throw new IllegalArgumentException("store is null");
        }
        
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(cache == null) {
            throw new IllegalArgumentException("cache is null");
        }
        
        if(metadata == null) {
            throw new IllegalArgumentException("metadata is null");
        }
        
        //if(partKeys == null) {
        //    throw new IllegalArgumentException("partKeys is null");
        //}
        
        this.store = store;
        this.config = config;
        this.cache = cache;
        this.metadata = metadata;
        this.lock = null;
        
        this.chunkData = null;
        this.currentPartNo = 0;
        this.offset = 0;
        
        this.size = metadata.getEntrySize();
        this.chunkSize = config.getChunkSize();
    }
    
    IgniteCacheInputStream(IgniteBigKeyValueStore store, IgniteDataStoreDriverConfig config, IgniteCache<String, ByteArray> cache, BigKeyValueStoreMetadata metadata, Lock lock) {
        if(store == null) {
            throw new IllegalArgumentException("store is null");
        }
        
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(cache == null) {
            throw new IllegalArgumentException("cache is null");
        }
        
        if(metadata == null) {
            throw new IllegalArgumentException("metadata is null");
        }
        
        //if(partKeys == null) {
        //    throw new IllegalArgumentException("partKeys is null");
        //}
        
        //if(lock == null) {
        //    throw new IllegalArgumentException("lock is null");
        //}
        
        this.store = store;
        this.config = config;
        this.cache = cache;
        this.metadata = metadata;
        this.lock = lock;
        
        this.chunkData = null;
        this.currentPartNo = 0;
        this.offset = 0;
        
        this.size = metadata.getEntrySize();
        this.chunkSize = config.getChunkSize();
    }

    @Override
    public synchronized int available() throws IOException {
        
        if(this.chunkData != null) {
            long currentStartOffset = this.chunkSize * this.currentPartNo;
            if(currentStartOffset <= this.offset && this.offset < currentStartOffset + this.chunkData.length) {
                return this.chunkData.length - (int)(this.offset - currentStartOffset);
            }
        }
        
        return 0;
    }
    
    @Override
    public synchronized long skip(long size) throws IOException {
        if(size <= 0) {
            return 0;
        }
        
        if(this.offset >= this.size) {
            return 0;
        }
        
        long lavailable = this.size - this.offset;
        if(size >= lavailable) {
            this.offset = this.size;
            return lavailable;
        } else {
            this.offset += size;
            return size;
        }
    }
    
    private synchronized void loadChunkData() throws IOException {
        if(this.chunkData != null) {
            long currentStartOffset = this.chunkSize * this.currentPartNo;
            if(currentStartOffset <= this.offset && this.offset < currentStartOffset + this.chunkData.length) {
                // safe to reuse 
                return;
            }
        }
        
        if(this.offset >= this.size) {
            this.chunkData = null;
            this.currentPartNo = 0;
            return;
        }
        
        int partNo = (int)(this.offset / this.chunkSize);
        String partKey = IgniteBigKeyValueStore.makePartkey(this.metadata.getKey(), partNo);
        
        ByteArray chunkDataByteArr = this.cache.get(partKey);
        if(chunkDataByteArr == null) {
            throw new IOException(String.format("Cannot read chunk data for %s", partKey));
        }
        
        this.chunkData = chunkDataByteArr.getArray();
        this.currentPartNo = partNo;
    }
    
    @Override
    public synchronized int read() throws IOException {
        if(this.offset >= this.size) {
            return -1;
        }
        
        loadChunkData();
        
        long currentStartOffset = this.chunkSize * this.currentPartNo;
        int bufferOffset = (int)(this.offset - currentStartOffset);
        if(bufferOffset < this.chunkData.length) {
            byte ch = this.chunkData[bufferOffset];
            this.offset++;
            return ch;
        } else {
            return -1;
        }
    }
    
    @Override
    public synchronized int read(byte[] bytes, int off, int len) throws IOException {
        if(this.offset >= this.size) {
            return -1;
        }
        
        if(bytes == null) {
            throw new IllegalArgumentException("bytes is null");
        }
        
        if(off < 0) {
            throw new IllegalArgumentException("off is negative");
        }
        
        if(len < 0) {
            throw new IllegalArgumentException("len is negative");
        }
            
        long lavailable = this.size - this.offset;
        int remain = len;
        if(len > lavailable) {
            remain = (int) lavailable;
        }
        
        int copied = remain;
        int outputBufferOffset = off;
        
        while(remain > 0) {
            loadChunkData();

            long currentStartOffset = this.chunkSize * this.currentPartNo;
            int bufferOffset = (int)(this.offset - currentStartOffset);
            int bufferLength = (int) Math.min(this.chunkData.length - bufferOffset, remain);
            
            System.arraycopy(this.chunkData, bufferOffset, bytes, outputBufferOffset, bufferLength);
            this.offset += bufferLength;
            outputBufferOffset += bufferLength;
            remain -= bufferLength;
        }
        
        return copied;
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(this.lock != null) {
            this.lock.unlock();
        }
    }
    
    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(int readLimit) {
        // Do nothing
    }

    @Override
    public void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
}
