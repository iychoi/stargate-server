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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.IgniteCache;
import stargate.commons.datastore.BigKeyValueStoreMetadata;
import stargate.commons.utils.ByteArray;
import stargate.commons.utils.DateTimeUtils;

/**
 *
 * @author iychoi
 */
public class IgniteCacheInputStream extends InputStream {

    private static final Log LOG = LogFactory.getLog(IgniteCacheInputStream.class);
    
    private IgniteBigKeyValueStore store;
    private IgniteDataStoreDriverConfig config;
    private IgniteCache<String, ByteArray> cache;
    private BigKeyValueStoreMetadata metadata;
    private int currentPartNo;
    private byte[] partData;
    private long offset;
    private long size;
    private int partSize;
    private int lastCompletedPartNo;
    private int currentWaitingPartNo;
    private Object partCompletionSyncObject = new Object();
    
    
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
        
        this.store = store;
        this.config = config;
        this.cache = cache;
        this.metadata = metadata;
        
        this.partData = null;
        this.currentPartNo = 0;
        this.offset = 0;
        
        this.size = metadata.getEntrySize();
        this.partSize = config.getPartSize();
        
        this.lastCompletedPartNo = -1;
        this.currentWaitingPartNo = -1;
    }
    
    @Override
    public synchronized int available() throws IOException {
        
        if(this.partData != null) {
            long currentStartOffset = this.partSize * this.currentPartNo;
            if(currentStartOffset <= this.offset && this.offset < currentStartOffset + this.partData.length) {
                return this.partData.length - (int)(this.offset - currentStartOffset);
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
    
    public void notifyPartCompletion(int partNo) {
        synchronized(this.partCompletionSyncObject) {
            if(this.lastCompletedPartNo < partNo) {
                this.lastCompletedPartNo = partNo;
            }
            
            if(this.currentWaitingPartNo <= partNo && this.currentWaitingPartNo >= 0) {
                this.partCompletionSyncObject.notifyAll();
            }
        }
    }
    
    private synchronized void loadPartData() throws IOException {
        if(this.partData != null) {
            long currentStartOffset = this.partSize * this.currentPartNo;
            if(currentStartOffset <= this.offset && this.offset < currentStartOffset + this.partData.length) {
                // safe to reuse 
                return;
            }
        }
        
        if(this.offset >= this.size) {
            this.partData = null;
            this.currentPartNo = 0;
            return;
        }
        
        int partNo = (int)(this.offset / this.partSize);
        if(partNo >= this.metadata.getPartNum()) {
            throw new IOException(String.format("partNo %d does not exist in %d parts", partNo, this.metadata.getPartNum()));
        }
        
        String partKey = IgniteBigKeyValueStore.makePartkey(this.metadata.getKey(), partNo);
        long beginTime = DateTimeUtils.getTimestamp();
        
        try {
            while(true) {
                ByteArray partDataByteArr = this.cache.get(partKey);
                if(partDataByteArr == null) {
                    // pending
                    long curTime = DateTimeUtils.getTimestamp();
                    if(DateTimeUtils.timeElapsedSec(beginTime, curTime, this.config.getPartWaitTimeoutSec())) {
                        // timeout
                        LOG.error(String.format("Timeout occurred while reading a part %s", partKey));
                        throw new IOException(String.format("cannot read part key - %s after %d sec waiting", partKey, this.config.getPartWaitTimeoutSec()));
                    } else {
                        // pending
                        //LOG.error(String.format("Sleep for waiting a part %s", partKey));
                        synchronized(this.partCompletionSyncObject) {
                            if(this.lastCompletedPartNo < partNo) {
                                LOG.info(String.format("Wait a part %s", partKey));
                                this.currentWaitingPartNo = partNo;
                                this.partCompletionSyncObject.wait(this.config.getPartWaitSleepMsec());
                            }
                        }
                    }
                } else {
                    this.partData = partDataByteArr.getArray();
                    this.currentPartNo = partNo;
                    synchronized(this.partCompletionSyncObject) {
                        if(this.lastCompletedPartNo < partNo) {
                            this.lastCompletedPartNo = partNo;
                        }
                        this.currentWaitingPartNo = -1;
                    }
                    break;
                }
            }
        } catch(InterruptedException ex) {
        }
    }
    
    @Override
    public synchronized int read() throws IOException {
        if(this.offset >= this.size) {
            return -1;
        }
        
        loadPartData();
        if(this.partData == null) {
            throw new IOException("part data is null");
        }
        
        long currentStartOffset = this.partSize * this.currentPartNo;
        int bufferOffset = (int)(this.offset - currentStartOffset);
        if(bufferOffset < this.partData.length) {
            byte ch = this.partData[bufferOffset];
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
            loadPartData();
            if(this.partData == null) {
                throw new IOException("part data is null");
            }

            long currentStartOffset = this.partSize * this.currentPartNo;
            int bufferOffset = (int)(this.offset - currentStartOffset);
            int bufferLength = (int) Math.min(this.partData.length - bufferOffset, remain);
            
            System.arraycopy(this.partData, bufferOffset, bytes, outputBufferOffset, bufferLength);
            this.offset += bufferLength;
            outputBufferOffset += bufferLength;
            remain -= bufferLength;
        }
        
        return copied;
    }
    
    @Override
    public synchronized void close() throws IOException {
        this.store.notifyInputStreamClosed(this.metadata.getKey());
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
