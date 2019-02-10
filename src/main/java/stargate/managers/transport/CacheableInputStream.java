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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author iychoi
 */
public class CacheableInputStream extends InputStream {

    private TransportManager manager;
    private InputStream inputStream;
    private String hash;
    
    private ByteArrayOutputStream cacheData = new ByteArrayOutputStream();
    
    CacheableInputStream(TransportManager manager, InputStream inputStream, String hash) {
        this.manager = manager;
        this.inputStream = inputStream;
        this.hash = hash;
    }

    @Override
    public int read() throws IOException {
        int b = this.inputStream.read();
        this.cacheData.write(b);
        return b;
    }
    
    @Override
    public int read(byte[] buf) throws IOException {
        if(buf == null) {
            throw new IllegalArgumentException("buf is null");
        }
        
        int b = this.inputStream.read(buf);
        this.cacheData.write(buf, 0, b);
        return b;
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
        
        int b = this.inputStream.read(buf, offset, len);
        this.cacheData.write(buf, offset, b);
        return b;
    }

    @Override
    public long skip(long len) throws IOException {
        if(len < 0) {
            throw new IllegalArgumentException("len is negative");
        }
        
        // don't skip
        //return this.inputStream.skip(l);
        byte[] buffer = new byte[4096];
        long remaining = len;
        long skipped = 0;
        while(remaining > 0) {
            int toRead = (int) Math.min(remaining, 4096);
            int b = this.inputStream.read(buffer, 0, toRead);
            if(b <= 0) {
                // EOF
                break;
            }
            this.cacheData.write(buffer, 0, b);
            
            remaining -= b;
            skipped += b;
        }
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return this.inputStream.available();
    }

    @Override
    public void close() throws IOException {
        // check if there is unaccessed data
        // if so, read fully and cache
        byte[] buffer = new byte[4096];
        int read = 0;
        while((read = this.inputStream.read(buffer, 0, 4096)) > 0) {
            this.cacheData.write(buffer, 0, read);
        }
        
        this.cacheData.close();
        byte[] cacheDataBytes = this.cacheData.toByteArray();
        this.inputStream.close();
        
        // cache
        this.manager.addDataChunkCache(this.hash, cacheDataBytes);
        this.cacheData.close();
    }

    @Override
    public void mark(int i) {
        this.inputStream.mark(i);
    }

    @Override
    public void reset() throws IOException {
        this.inputStream.reset();
        this.cacheData.reset();
    }

    @Override
    public boolean markSupported() {
        return this.inputStream.markSupported();
    }
}
