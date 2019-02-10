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
package stargate.drivers.datasource.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public class HDFSChunkReader extends InputStream {

    private static final int BUFFER_SIZE = 1024*1024;
    
    private InputStream is;
    private long offset;
    private int size;
    private long currentOffset;
    
    public HDFSChunkReader(URI resourceUri, long offset, int size) throws IOException {
        if(resourceUri == null) {
            throw new IllegalArgumentException("resourceUri is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        
        Path path = new Path(resourceUri.normalize());
        FileSystem fs = path.getFileSystem(new Configuration());
        FSDataInputStream is = fs.open(path, BUFFER_SIZE);
        
        initialize(is, offset, size);
    }
    
    public HDFSChunkReader(FileSystem fs, Path resourcePath, long offset, int size) throws IOException {
        if(fs == null) {
            throw new IllegalArgumentException("fs is null");
        }
        
        if(resourcePath == null) {
            throw new IllegalArgumentException("resourcePath is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        
        FSDataInputStream is = fs.open(resourcePath, BUFFER_SIZE);
        
        initialize(is, offset, size);
    }
    
    public HDFSChunkReader(Configuration conf, Path resourcePath, long offset, int size) throws IOException {
        if(conf == null) {
            throw new IllegalArgumentException("conf is null");
        }
        
        if(resourcePath == null) {
            throw new IllegalArgumentException("resourcePath is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        
        FileSystem fs = resourcePath.getFileSystem(conf);
        FSDataInputStream is = fs.open(resourcePath, BUFFER_SIZE);
        
        initialize(is, offset, size);
    }
    
    public HDFSChunkReader(Path resourcePath, long offset, int size) throws IOException {
        if(resourcePath == null) {
            throw new IllegalArgumentException("resourcePath is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
                
        FileSystem fs = resourcePath.getFileSystem(new Configuration());
        FSDataInputStream is = fs.open(resourcePath, BUFFER_SIZE);
        
        initialize(is, offset, size);
    }
    
    public HDFSChunkReader(FSDataInputStream is, long offset, int size) throws IOException {
        if(is == null) {
            throw new IllegalArgumentException("is is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        
        initialize(is, offset, size);
    }
    
    private void initialize(FSDataInputStream is, long offset, int size) throws IOException {
        this.is = is;
        this.offset = offset;
        this.size = size;
        
        is.seek(offset);
        
        if(is.getPos() != this.offset) {
            throw new IOException(String.format("failed to move offset to %d (moved to %d)", this.offset, is.getPos()));
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
    public int read(byte[] buf) throws IOException {
        if(buf == null) {
            throw new IllegalArgumentException("buf is null");
        }
        
        int availableBytes = availableBytes(buf.length);
        if(availableBytes <= 0) {
            return -1;
        }
        
        int read = this.is.read(buf, 0, availableBytes);
        if(read >= 0) {
            this.currentOffset += read;
        }
        return read;
    }

    @Override
    public int read(byte[] buf, int offset, int len) throws IOException {
        if(buf == null) {
            throw new IllegalArgumentException("buf is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(len < 0) {
            throw new IllegalArgumentException("len is negative");
        }
        
        int availableBytes = availableBytes(len);
        if(availableBytes <= 0) {
            return -1;
        }
        
        int read = this.is.read(buf, offset, availableBytes);
        if(read >= 0) {
            this.currentOffset += read;
        }
        return read;
    }

    @Override
    public long skip(long len) throws IOException {
        if(len < 0) {
            throw new IllegalArgumentException("len is negative");
        }
        
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
    public void mark(int i) {
    }

    @Override
    public void reset() throws IOException {
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
