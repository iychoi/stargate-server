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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.datastore.BigKeyValueStoreMetadata;
import stargate.commons.io.AbstractSeekableInputStream;
import stargate.commons.utils.DateTimeUtils;

/**
 *
 * @author iychoi
 */
public class IgniteCacheInputStream extends AbstractSeekableInputStream {

    private static final Log LOG = LogFactory.getLog(IgniteCacheInputStream.class);
    
    private static final int BUFFER_SIZE = 64 * 1024;
    
    private IgniteBigKeyValueStore store;
    private IgniteDataStoreDriverConfig config;
    private BigKeyValueStoreMetadata metadata;
    private long beginOffset;
    private long offset;
    private long size;
    private File cacheFile;
    private long lastCacheFileLength;
    private long lastCompletedOffset;
    private long currentWaitingOffset;
    private BufferedInputStream cacheInputStream;
    private Object waitingObject = new Object();
    
    IgniteCacheInputStream(IgniteBigKeyValueStore store, IgniteDataStoreDriverConfig config, BigKeyValueStoreMetadata metadata) throws IOException {
        if(store == null) {
            throw new IllegalArgumentException("store is null");
        }
        
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(metadata == null) {
            throw new IllegalArgumentException("metadata is null");
        }
        
        this.store = store;
        this.config = config;
        this.metadata = metadata;
        
        this.beginOffset = 0;
        this.offset = 0;
        this.size = metadata.getEntrySize();
        
        this.lastCompletedOffset = -1;
        this.currentWaitingOffset = -1;
        
        this.cacheFile = this.store.getCacheFilePath(metadata.getKey());
        this.lastCacheFileLength = 0;
        this.cacheInputStream = null;
    }
    
    IgniteCacheInputStream(IgniteBigKeyValueStore store, IgniteDataStoreDriverConfig config, BigKeyValueStoreMetadata metadata, long beginOffset, long size) throws IOException {
        if(store == null) {
            throw new IllegalArgumentException("store is null");
        }
        
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(metadata == null) {
            throw new IllegalArgumentException("metadata is null");
        }
        
        this.store = store;
        this.config = config;
        this.metadata = metadata;
        
        this.beginOffset = beginOffset;
        this.offset = 0;
        this.size = Math.min(metadata.getEntrySize(), size);
        
        this.lastCompletedOffset = -1;
        this.currentWaitingOffset = -1;
        
        this.cacheFile = this.store.getCacheFilePath(metadata.getKey());
        this.lastCacheFileLength = -1;
        this.cacheInputStream = null;
    }
    
    private synchronized void safeInitCacheFileInputStream() throws IOException {
        if(this.cacheInputStream == null) {
            // wait
            waitData(this.beginOffset);
            
            try {
                FileInputStream is = new FileInputStream(this.cacheFile);
                if(this.beginOffset > 0) {
                    is.getChannel().position(this.beginOffset);
                }
                this.cacheInputStream = new BufferedInputStream(is, BUFFER_SIZE);
            } catch (FileNotFoundException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
    
    private synchronized void waitData(long offset) throws IOException {
        if(this.lastCacheFileLength >= offset) {
            return;
        }
        
        long beginTime = DateTimeUtils.getTimestamp();
        long curTime = beginTime;
        
        // check file existance
        if(this.lastCacheFileLength < 0) {
            while(!this.cacheFile.exists()) {
                if(DateTimeUtils.timeElapsedSec(beginTime, curTime, this.config.getDataWaitTimeoutSec())) {
                    // timeout
                    LOG.error("Timeout occurred while waiting data");
                    throw new IOException(String.format("cannot open data after %d sec waiting", this.config.getDataWaitTimeoutSec()));
                } else {
                    LOG.info("Wait data");
                    try {
                        Thread.sleep(this.config.getDataWaitPollingIntervalMsec());
                    } catch (InterruptedException ex) {
                        throw new IOException(ex);
                    }
                    
                    curTime = DateTimeUtils.getTimestamp();
                }
            }
        }
        
        this.lastCacheFileLength = this.cacheFile.length();
        if(this.lastCacheFileLength >= offset) {
            return;
        }
        
        while(this.lastCacheFileLength < offset) {
            if(DateTimeUtils.timeElapsedSec(beginTime, curTime, this.config.getDataWaitTimeoutSec())) {
                // timeout
                LOG.error(String.format("Timeout occurred while reading data at offset %d", offset));
                throw new IOException(String.format("cannot read data at offset - %d after %d sec waiting", offset, this.config.getDataWaitTimeoutSec()));
            } else {
                synchronized(this.waitingObject) {
                    if(this.lastCompletedOffset < offset) {
                        LOG.info(String.format("Wait data at offset %d", offset));
                        this.currentWaitingOffset = offset;
                        try {
                            this.waitingObject.wait(this.config.getDataWaitPollingIntervalMsec());
                        } catch (InterruptedException ex) {
                            throw new IOException(ex);
                        }
                    }

                    this.lastCacheFileLength = this.cacheFile.length();
                    curTime = DateTimeUtils.getTimestamp();
                }
            }
        }
        
        synchronized(this.waitingObject) {
            if(this.lastCompletedOffset < offset) {
                this.lastCompletedOffset = offset;
            }
            this.currentWaitingOffset = -1;
        }
    }
    
    @Override
    public synchronized int available() throws IOException {
        if(this.cacheInputStream == null) {
            return 0;
        } else {
            return (int) Math.min(this.cacheInputStream.available(), this.size - this.offset);
        }
    }
    
    @Override
    public synchronized long skip(long size) throws IOException {
        if(size <= 0) {
            return 0;
        }
        
        if(this.offset >= this.size) {
            return 0;
        }
        
        safeInitCacheFileInputStream();
        
        long lavailable = this.size - this.offset;
        if(size >= lavailable) {
            this.offset = this.size;
            waitData(this.beginOffset + this.offset);
            this.cacheInputStream.skip(lavailable);
            return lavailable;
        } else {
            this.offset += size;
            waitData(this.beginOffset + this.offset);
            this.cacheInputStream.skip(size);
            return size;
        }
    }
    
    @Override
    public synchronized long getOffset() throws IOException {
        return this.offset;
    }
    
    @Override
    public synchronized void seek(long offset) throws IOException {
        if(offset < 0) {
            return;
        }
        
        if(offset == this.offset) {
            return;
        }
        
        safeInitCacheFileInputStream();
        
        long seekable = (int) Math.min(this.size, offset);
        // wait
        waitData(this.beginOffset + seekable);
        
        if(this.cacheInputStream != null) {
            this.cacheInputStream.close();
        
            try {
                FileInputStream is = new FileInputStream(this.cacheFile);
                is.getChannel().position(this.beginOffset + seekable);
                this.cacheInputStream = new BufferedInputStream(is, BUFFER_SIZE);
            } catch (FileNotFoundException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
        this.offset = seekable;
    }
    
    public void notifyDataAvailability(long offset) {
        synchronized(this.waitingObject) {
            if(this.lastCompletedOffset < offset) {
                this.lastCompletedOffset = offset;
            }
            
            if(this.currentWaitingOffset <= offset && this.currentWaitingOffset >= 0) {
                this.waitingObject.notifyAll();
            }
        }
    }
    
    @Override
    public synchronized int read() throws IOException {
        if(this.offset >= this.size) {
            return -1;
        }

        safeInitCacheFileInputStream();
        
        waitData(this.beginOffset + this.offset + 1);
        
        int ch = this.cacheInputStream.read();
        this.offset++;
        return ch;
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
        
        safeInitCacheFileInputStream();
        
        int available = (int) Math.min(this.size - this.offset, len);
        
        waitData(this.beginOffset + this.offset + available);
        int readLen = this.cacheInputStream.read(bytes, off, available);
        if(readLen >= 0) {
            this.offset += readLen;
        }
        
        return readLen;
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(this.cacheInputStream != null) {
            this.cacheInputStream.close();
            this.cacheInputStream = null;
        }
        
        this.store.notifyInputStreamClosed(this.metadata.getKey(), this);
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
