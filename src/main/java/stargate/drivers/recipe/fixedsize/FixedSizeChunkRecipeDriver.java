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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.recipe.AbstractRecipeDriver;
import stargate.commons.recipe.AbstractRecipeDriverConfig;
import stargate.commons.recipe.RecipeChunk;

/**
 *
 * @author iychoi
 */
public class FixedSizeChunkRecipeDriver extends AbstractRecipeDriver {

    private static final Log LOG = LogFactory.getLog(FixedSizeChunkRecipeDriver.class);
    
    private FixedSizeChunkRecipeDriverConfig config;
    private int chunkSize;
    private String hashAlgorithm;
    private static final int BUFFER_SIZE = 64*1024; // 64KB
    private int bufferSize = BUFFER_SIZE;
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
        this.bufferSize = Math.min(this.chunkSize, BUFFER_SIZE);
        this.buffer = new byte[this.bufferSize];
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
        this.bufferSize = Math.min(this.chunkSize, BUFFER_SIZE);
        this.buffer = new byte[this.bufferSize];
    }
    
    public FixedSizeChunkRecipeDriver(FixedSizeChunkRecipeDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
        this.chunkSize = this.config.getChunkSize();
        this.hashAlgorithm = this.config.getHashAlgorithm();
        this.bufferSize = Math.min(this.chunkSize, BUFFER_SIZE);
        this.buffer = new byte[this.bufferSize];
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
    public byte[] calculateHash(byte[] buf) throws IOException {
        if(buf == null) {
            throw new IllegalArgumentException("buf is null");
        }
        
        try {    
            MessageDigest messageDigest = MessageDigest.getInstance(this.hashAlgorithm);
            messageDigest.update(buf);
            
            byte[] digest = messageDigest.digest();
            return digest;
        } catch (NoSuchAlgorithmException ex) {
            throw new IOException(ex);
        }
    }
    
    @Override
    public RecipeChunk produceRecipeChunk(InputStream is, long offset) throws IOException {
        if(is == null) {
            throw new IllegalArgumentException("is is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(this.hashAlgorithm);
            DigestInputStream dis = new DigestInputStream(is, messageDigest);
            
            int nread = 0;
            int toread = this.chunkSize;
            int chunkLength = 0;
            
            while((nread = dis.read(this.buffer, 0, Math.min(toread, this.bufferSize))) > 0) {
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
                return new RecipeChunk(offset, chunkLength, hash, null);
            }
            
            return null;
        } catch (NoSuchAlgorithmException ex) {
            throw new IOException(ex);
        } 
    }

    @Override
    public RecipeChunk produceRecipeChunk(byte[] buf, long offset, int len) throws IOException {
        if(buf == null) {
            throw new IllegalArgumentException("buf is null");
        }
        
        if(offset < 0) {
            throw new IllegalArgumentException("offset is negative");
        }
        
        if(len < 0) {
            throw new IllegalArgumentException("len is negative");
        }
        
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(this.hashAlgorithm);
            messageDigest.update(buf);
            
            byte[] digest = messageDigest.digest();
            
            if(len > 0) {
                // create a new recipe chunk
                byte[] hash = messageDigest.digest();
                return new RecipeChunk(offset, len, hash, null);
            }
            
            return null;
        } catch (NoSuchAlgorithmException ex) {
            throw new IOException(ex);
        } 
    }
}
