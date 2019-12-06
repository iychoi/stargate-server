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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
public class IgniteCachePartInputStream extends InputStream {

    private static final Log LOG = LogFactory.getLog(IgniteCachePartInputStream.class);
    
    private IgniteBigKeyValueStore store;
    private IgniteDataStoreDriverConfig config;
    private IgniteCache<String, ByteArray> cache;
    private BigKeyValueStoreMetadata metadata;
    private int partNo;
    private byte[] partData;
    private int offset;
    private int size;
    private CountDownLatch latch;
    
    
    IgniteCachePartInputStream(IgniteBigKeyValueStore store, IgniteDataStoreDriverConfig config, IgniteCache<String, ByteArray> cache, BigKeyValueStoreMetadata metadata, int partNo) {
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
        
        if(partNo < 0) {
            throw new IllegalArgumentException("partNo is negative");
        }
        
        if(partNo >= metadata.getPartNum()) {
            throw new IllegalArgumentException(String.format("partNo %d does not exist in %d parts", partNo, metadata.getPartNum()));
        }
        
        this.store = store;
        this.config = config;
        this.cache = cache;
        this.metadata = metadata;
        
        this.partData = null;
        this.partNo = partNo;
        this.offset = 0;
        
        this.size = Math.min((int)(metadata.getEntrySize() - (partNo * config.getPartSize())), config.getPartSize());
        this.latch = new CountDownLatch(1);
    }
    
    @Override
    public synchronized int available() throws IOException {
        
        if(this.partData != null) {
            return (int)(this.size - this.offset);
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
    
    public void notifyPartCompletion() {
        this.latch.countDown();
    }
    
    private synchronized void loadPartData() throws IOException {
        if(this.partData != null) {
            // safe to reuse 
            return;
        }
        
        if(this.offset >= this.size) {
            this.partData = null;
            return;
        }
        
        String partKey = IgniteBigKeyValueStore.makePartkey(this.metadata.getKey(), this.partNo);
        long beginTime = DateTimeUtils.getTimestamp();
        
        try {
            while(true) {
                ByteArray partDataByteArr = this.cache.get(partKey);
                if(partDataByteArr == null) {
                    long curTime = DateTimeUtils.getTimestamp();
                    if(DateTimeUtils.timeElapsedSec(beginTime, curTime, this.config.getPartWaitTimeoutSec())) {
                        // timeout
                        LOG.error(String.format("Timeout occurred while reading a part %s", partKey));
                        throw new IOException(String.format("cannot read part key - %s after %d sec waiting", partKey, this.config.getPartWaitTimeoutSec()));
                    } else {
                        // pending
                        //LOG.error(String.format("Sleep for waiting a part %s", partKey));
                        LOG.info(String.format("Wait a part %s", partKey));
                        boolean wokeup = this.latch.await(this.config.getPartWaitSleepMsec(), TimeUnit.MILLISECONDS);
                        if(wokeup) {
                            LOG.info(String.format("Woke up. Found a part %s", partKey));
                        }
                    }
                } else {
                    this.partData = partDataByteArr.getArray();
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
        
        if(this.offset < this.partData.length) {
            byte ch = this.partData[this.offset];
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

            int bufferLength = (int) Math.min(this.partData.length - this.offset, remain);
            
            System.arraycopy(this.partData, this.offset, bytes, outputBufferOffset, bufferLength);
            this.offset += bufferLength;
            outputBufferOffset += bufferLength;
            remain -= bufferLength;
        }
        
        return copied;
    }
    
    @Override
    public synchronized void close() throws IOException {
        this.store.notifyPartInputStreamClosed(this.metadata.getKey(), this.partNo);
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
