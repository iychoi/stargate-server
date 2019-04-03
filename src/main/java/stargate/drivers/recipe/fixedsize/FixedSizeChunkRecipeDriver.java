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
package stargate.drivers.recipe.fixedsize;

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
public class FixedSizeChunkRecipeDriver extends AbstractRecipeDriver {

    private FixedSizeChunkRecipeDriverConfig config;
    private int chunkSize;
    private String hashAlgorithm;
    private byte[] buffer;
    
    public FixedSizeChunkRecipeDriver(AbstractDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof FixedSizeChunkRecipeDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of FixedSizeChunkRecipeDriver");
        }
        
        this.config = (FixedSizeChunkRecipeDriverConfig) config;
        this.chunkSize = this.config.getChunkSize();
        this.hashAlgorithm = this.config.getHashAlgorithm();
        int bufferSize = Math.min(this.chunkSize, this.config.getBufferSize());
        this.buffer = new byte[bufferSize];
    }
    
    public FixedSizeChunkRecipeDriver(AbstractRecipeDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof FixedSizeChunkRecipeDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of FixedSizeChunkRecipeDriver");
        }
        
        this.config = (FixedSizeChunkRecipeDriverConfig) config;
        this.chunkSize = this.config.getChunkSize();
        this.hashAlgorithm = this.config.getHashAlgorithm();
        int bufferSize = Math.min(this.chunkSize, this.config.getBufferSize());
        this.buffer = new byte[bufferSize];
    }
    
    public FixedSizeChunkRecipeDriver(FixedSizeChunkRecipeDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
        this.chunkSize = this.config.getChunkSize();
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
        
        RecipeChunk chunk = null;
        do {
            chunk = produceRecipeChunk(is);
            chunks.add(chunk);
        } while(chunk != null);
        
        return Collections.unmodifiableCollection(chunks);
    }
    
    @Override
    public RecipeChunk produceRecipeChunk(InputStream is) throws IOException, DriverNotInitializedException {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(this.hashAlgorithm);
            DigestInputStream dis = new DigestInputStream(is, messageDigest);
            
            int nread = 0;
            int toread = this.chunkSize;
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
