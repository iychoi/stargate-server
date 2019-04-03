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
package stargate.drivers.recipe.overlappedfixedsize;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.recipe.AbstractRecipeDriver;
import stargate.commons.recipe.AbstractRecipeDriverConfig;
import stargate.commons.recipe.RecipeChunk;

/**
 *
 * @author iychoi
 */
public class OverlappedFixedSizeChunkRecipeDriver extends AbstractRecipeDriver {

    private OverlappedFixedSizeChunkRecipeDriverConfig config;
    private int chunkSize;
    private int overlapSize;
    private String hashAlgorithm;
    private byte[] buffer;
    
    public OverlappedFixedSizeChunkRecipeDriver(AbstractDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof OverlappedFixedSizeChunkRecipeDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of FixedSizeChunkRecipeDriver");
        }
        
        this.config = (OverlappedFixedSizeChunkRecipeDriverConfig) config;
        this.chunkSize = this.config.getChunkSize();
        this.overlapSize = this.config.getOverlapSize();
        this.hashAlgorithm = this.config.getHashAlgorithm();
        int bufferSize = Math.min(this.chunkSize, this.config.getBufferSize());
        this.buffer = new byte[bufferSize];
    }
    
    public OverlappedFixedSizeChunkRecipeDriver(AbstractRecipeDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof OverlappedFixedSizeChunkRecipeDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of FixedSizeChunkRecipeDriver");
        }
        
        this.config = (OverlappedFixedSizeChunkRecipeDriverConfig) config;
        this.chunkSize = this.config.getChunkSize();
        this.overlapSize = this.config.getOverlapSize();
        this.hashAlgorithm = this.config.getHashAlgorithm();
        int bufferSize = Math.min(this.chunkSize, this.config.getBufferSize());
        this.buffer = new byte[bufferSize];
    }
    
    public OverlappedFixedSizeChunkRecipeDriver(OverlappedFixedSizeChunkRecipeDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
        this.chunkSize = this.config.getChunkSize();
        this.overlapSize = this.config.getOverlapSize();
        this.hashAlgorithm = this.config.getHashAlgorithm();
        int bufferSize = Math.min(this.chunkSize, this.config.getBufferSize());
        this.buffer = new byte[bufferSize];
    }
    
    @Override
    public void init() throws IOException {
        super.init();
    }
    
    @Override
    public void uninit() throws IOException {
        super.uninit();
    }
    
    @Override
    public int getChunkSize() {
        return this.chunkSize;
    }
    
    public int getOverlapSize() {
        return this.overlapSize;
    }
    
    @Override
    public String getHashAlgorithm() {
        return this.hashAlgorithm;
    }
    
    @Override
    public Collection<RecipeChunk> produceRecipeChunks(InputStream is) throws IOException, DriverNotInitializedException {
        if(is == null) {
            throw new IllegalArgumentException("is is null");
        }
        
        if(!isStarted()) {
            throw new DriverNotInitializedException("driver is not initialized");
        }
        
        List<RecipeChunk> chunks = new ArrayList<RecipeChunk>();
        byte[] overlapBuffer = new byte[this.overlapSize];
        int overlapBufferSize = 0;
        
        RecipeChunk chunk = null;
        do {
            chunk = null;
            try {
                MessageDigest messageDigest = MessageDigest.getInstance(this.hashAlgorithm);
                
                int nread = 0;
                int toread = this.chunkSize;
                int chunkLength = 0;
                
                if(overlapBufferSize > 0) {
                    // from previous iteration
                    messageDigest.update(overlapBuffer, 0, overlapBufferSize);
                    
                    chunkLength += overlapBufferSize;
                    toread -= overlapBufferSize;
                    
                    // reset
                    overlapBufferSize = 0;
                }
                
                DigestInputStream dis = new DigestInputStream(is, messageDigest);

                while((nread = dis.read(this.buffer, 0, Math.min(toread, this.buffer.length))) > 0) {
                    chunkLength += nread;
                    toread -= nread;
                    if(toread <= 0) {
                        // read done
                        break;
                    }
                }
                
                if(chunkLength == this.chunkSize) {
                    // not EOF
                    // read more
                    int overlapBufferOffset = 0;
                    while((nread = dis.read(overlapBuffer, overlapBufferOffset, this.overlapSize - overlapBufferOffset)) > 0) {
                        chunkLength += nread;
                        overlapBufferOffset += nread;
                        if(this.overlapSize - overlapBufferOffset <= 0) {
                            // read done
                            break;
                        }
                    }
                    
                    overlapBufferSize = overlapBufferOffset;
                }
                
                // do not close DigestInputStream because it closes inputstream that will be reused
                //dis.close();

                if(chunkLength > 0) {
                    // create a new recipe chunk
                    byte[] hash = messageDigest.digest();
                    chunk = new RecipeChunk(0, chunkLength, hash, null);
                    chunks.add(chunk);
                }
            } catch (NoSuchAlgorithmException ex) {
                throw new IOException(ex);
            }
        } while(chunk != null);
        
        return Collections.unmodifiableCollection(chunks);
    }
    
    @Override
    public RecipeChunk produceRecipeChunk(InputStream is) throws IOException, DriverNotInitializedException {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(this.hashAlgorithm);
            DigestInputStream dis = new DigestInputStream(is, messageDigest);
            
            int nread = 0;
            int toread = this.chunkSize + this.overlapSize;
            int chunkLength = 0;
            
            while((nread = dis.read(this.buffer, 0, Math.min(toread, this.buffer.length))) > 0) {
                chunkLength += nread;
                toread -= nread;
                if(toread <= 0) {
                    // read done
                    break;
                }
            }
            // do not close DigestInputStream because it closes inputstream that will be reused
            //dis.close();
            
            if(chunkLength > 0) {
                // create a new recipe chunk
                byte[] hash = messageDigest.digest();
                return new RecipeChunk(0, chunkLength, hash, null);
            }
            
            return null;
        } catch (NoSuchAlgorithmException ex) {
            throw new IOException(ex);
        } 
    }
}
