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
import stargate.commons.utils.HexUtils;

/**
 *
 * @author iychoi
 */
public class FixedSizeChunkRecipeDriver extends AbstractRecipeDriver {

    private static final Log LOG = LogFactory.getLog(FixedSizeChunkRecipeDriver.class);
    
    private FixedSizeChunkRecipeDriverConfig config;
    private int chunkSize;
    private String hashAlgorithm;
    private static final int BUFFER_SIZE = 100*1024; // 100KB
    
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
    }
    
    public FixedSizeChunkRecipeDriver(FixedSizeChunkRecipeDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
        this.chunkSize = this.config.getChunkSize();
        this.hashAlgorithm = this.config.getHashAlgorithm();
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
    public String calculateHash(byte[] buffer) throws IOException {
        if(buffer == null) {
            throw new IllegalArgumentException("buffer is null");
        }
        
        try {    
            MessageDigest messageDigest = MessageDigest.getInstance(this.hashAlgorithm);
            messageDigest.update(buffer);
            
            byte[] digest = messageDigest.digest();
            return HexUtils.toHexString(digest).toLowerCase();
        } catch (NoSuchAlgorithmException ex) {
            throw new IOException(ex);
        }
    }
    
    @Override
    public RecipeChunk produceRecipeChunk(InputStream is) throws IOException {
        if(is == null) {
            throw new IllegalArgumentException("is is null");
        }
        
        int bufferSize = Math.min(this.chunkSize, BUFFER_SIZE);
        byte[] buffer = new byte[bufferSize];
        
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(this.hashAlgorithm);
            DigestInputStream dis = new DigestInputStream(is, messageDigest);
            
            int nread = 0;
            int toread = this.chunkSize;
            int chunkLength = 0;
            
            while((nread = dis.read(buffer, 0, Math.min(toread, bufferSize))) > 0) {
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
