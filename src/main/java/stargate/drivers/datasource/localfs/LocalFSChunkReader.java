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
package stargate.drivers.datasource.localfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class LocalFSChunkReader extends InputStream {

    private static final Log LOG = LogFactory.getLog(LocalFSChunkReader.class);
    
    private InputStream is;
    private long offset;
    private int size;
    private long currentOffset;
    
    LocalFSChunkReader(File resourcePath, long offset, int size) throws IOException {
        if(resourcePath == null) {
            throw new IllegalArgumentException("resourcePath is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is invalid");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is invalid");
        }
        
        FileInputStream is = new FileInputStream(resourcePath);
        
        initialize(is, offset, size);
    }
    
    LocalFSChunkReader(FileInputStream is, long offset, int size) throws IOException {
        if(is == null) {
            throw new IllegalArgumentException("is is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is invalid");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is invalid");
        }
        
        initialize(is, offset, size);
    }
    
    private void initialize(FileInputStream is, long offset, int size) throws IOException {
        this.is = is;
        this.offset = offset;
        this.size = size;
        // do skip because there's no seek api
        long skippedBytes = is.skip(offset);
        if(skippedBytes != this.offset) {
            throw new IOException(String.format("failed to move offset to %d (moved to %d)", this.offset, skippedBytes));
        }
        this.currentOffset = offset;
    }
    
    private int availableBytes(int toRead) {
        long offsetMove = (this.currentOffset - this.offset) + toRead;
        if(offsetMove > this.size) {
            return (int) (this.size - (this.currentOffset - this.offset));
        } else {
            return toRead;
        }
    }
    
    @Override
    public int read() throws IOException {
        if(availableBytes(1) >= 1) {
            int read = is.read();
            if(read >= 0) {
                this.currentOffset++;
            }
            return read;
        }
        return -1;
    }

    @Override
    public int read(byte[] bytes) throws IOException {
        int availableBytes = availableBytes(bytes.length);
        if(availableBytes <= 0) {
            return -1;
        }
        
        int read = this.is.read(bytes, 0, availableBytes);
        if(read >= 0) {
            this.currentOffset += read;
        }
        return read;
    }

    @Override
    public int read(byte[] bytes, int offset, int len) throws IOException {
        int availableBytes = availableBytes(len);
        if(availableBytes <= 0) {
            return -1;
        }
        
        int read = this.is.read(bytes, offset, availableBytes);
        if(read >= 0) {
            this.currentOffset += read;
        }
        return read;
    }

    @Override
    public long skip(long len) throws IOException {
        int availableBytes = availableBytes((int) len);
        if(availableBytes <= 0) {
            return -1;
        }
        
        long skipped = this.is.skip(availableBytes);
        if(skipped >= 0) {
            this.currentOffset += skipped;
        }
        return skipped;
    }

    @Override
    public int available() throws IOException {
        int availableBytes = availableBytes((int) this.is.available());
        if(availableBytes <= 0) {
            return -1;
        }
        
        return availableBytes;
    }

    @Override
    public void close() throws IOException {
        this.is.close();
    }

    @Override
    public synchronized void mark(int i) {
    }

    @Override
    public synchronized void reset() throws IOException {
        this.is.reset();
        long skip = this.is.skip(this.offset);
        if(skip != this.offset) {
            throw new IOException("failed to move offset to " + skip);
        }
    }

    @Override
    public boolean markSupported() {
        return false;
    }
}
