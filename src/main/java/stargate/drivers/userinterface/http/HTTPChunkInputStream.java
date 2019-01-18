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
package stargate.drivers.userinterface.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSInputStream;
import stargate.commons.utils.IOUtils;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.utils.IPUtils;

/**
 *
 * @author iychoi
 */
public class HTTPChunkInputStream extends FSInputStream {

    private static final Log LOG = LogFactory.getLog(HTTPChunkInputStream.class);
    
    public static final String DEFAULT_NODE_NAME = "DEFAULT_NODE";
    public static final String LOCAL_NODE_NAME = "LOCAL_NODE";
    
    // node-name to client mapping
    private Map<String, HTTPUserInterfaceClient> clients = new HashMap<String, HTTPUserInterfaceClient>();
    private Recipe recipe;
    private long offset;
    private long size;
    private ChunkData chunkData;
    
    public HTTPChunkInputStream(HTTPUserInterfaceClient client, Recipe recipe) {
        if(client == null) {
            throw new IllegalArgumentException("client is null");
        }
        
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        Map<String, HTTPUserInterfaceClient> clients = new HashMap<String, HTTPUserInterfaceClient>();
        clients.put(DEFAULT_NODE_NAME, client);
        
        initialize(clients, recipe);
    }
    
    public HTTPChunkInputStream(Map<String, HTTPUserInterfaceClient> clients, Recipe recipe) {
        if(clients == null) {
            throw new IllegalArgumentException("clients is null");
        }
        
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        initialize(clients, recipe);
    }

    private void initialize(Map<String, HTTPUserInterfaceClient> clients, Recipe recipe) {
        if(clients == null) {
            throw new IllegalArgumentException("client is null");
        }
        
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        this.clients.putAll(clients);
        setLocalClient();
        
        this.recipe = recipe;
        this.offset = 0;
        this.size = recipe.getMetadata().getSize();
        this.chunkData = null;
    }
    
    private void setLocalClient() {
        if(!this.clients.containsKey(LOCAL_NODE_NAME)) {
            Set<Map.Entry<String, HTTPUserInterfaceClient>> entrySet = this.clients.entrySet();
            for(Map.Entry<String, HTTPUserInterfaceClient> entry : entrySet) {
                HTTPUserInterfaceClient client = entry.getValue();
                URI serviceURI = client.getServiceURI();
                try {
                    if(IPUtils.isLocalIPAddress(serviceURI.getHost())) {
                        this.clients.put(LOCAL_NODE_NAME, client);
                        break;
                    }
                } catch (IOException ex) {
                }
            }
        }
    }
    
    @Override
    public synchronized long getPos() throws IOException {
        return this.offset;
    }
    
    @Override
    public synchronized int available() throws IOException {
        if(this.chunkData != null &&
                this.chunkData.getOffset() <= this.offset &&
                (this.chunkData.getOffset() + this.chunkData.getSize()) > this.offset) {
            // safe to reuse
            return (int) (this.chunkData.getSize() - (this.offset - this.chunkData.getOffset()));
        }
        return 0;
    }
    
    @Override
    public synchronized void seek(long l) throws IOException {
        if(l < 0) {
            throw new IOException("cannot seek to negative offset : " + l);
        }
        
        if(l >= this.size) {
            this.offset = this.size;
        } else {
            this.offset = l;
        }
    }
    
    @Override
    public synchronized long skip(long l) throws IOException {
        if(l <= 0) {
            return 0;
        }
        
        if(this.offset >= this.size) {
            return 0;
        }
        
        long lavailable = this.size - this.offset;
        if(l >= lavailable) {
            this.offset = this.size;
            return lavailable;
        } else {
            this.offset += l;
            return l;
        }
    }
    
    @Override
    public synchronized boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
    
    private synchronized void loadChunkData(long offset) throws IOException {
        if(this.chunkData != null &&
                this.chunkData.getOffset() <= offset &&
                (this.chunkData.getOffset() + this.chunkData.getSize()) > offset) {
            // safe to reuse
            return;
        }
        
        this.chunkData = null;
        
        if(offset >= this.size) {
            return;
        }
        
        RecipeChunk chunk = this.recipe.getChunk(offset);
        if(chunk == null) {
            throw new IOException(String.format("cannot find chunk for offset %d", offset));
        }
        
        DataObjectMetadata metadata = this.recipe.getMetadata();
        DataObjectURI uri = metadata.getURI();
        
        Collection<Integer> nodeIDs = chunk.getNodeIDs();
        Collection<String> nodeNames = this.recipe.getNodeNames(nodeIDs);
        
        HTTPUserInterfaceClient localClient = this.clients.get(LOCAL_NODE_NAME);
        HTTPUserInterfaceClient client = null;
        
        // Step1. check if local node has the block
        if(localClient != null) {
            URI localServiceURI = localClient.getServiceURI();
            
            for(String nodeName : nodeNames) {
                HTTPUserInterfaceClient candidateClient = this.clients.get(nodeName);
                if(candidateClient != null) {
                    URI candidateServiceURI = candidateClient.getServiceURI();
                    if(candidateServiceURI.equals(localServiceURI)) {
                        // we found
                        client = candidateClient;
                        break;
                    }
                }
            }
        }
        
        // Step2. use any of nodes having the block
        if(client == null) {
            for(String nodeName : nodeNames) {
                client = this.clients.get(nodeName);
                if(client != null) {
                    // we found
                    break;
                }
            }
        }
        
        // Step3. use wild card
        if(client == null) {
            client = this.clients.get(DEFAULT_NODE_NAME);
        }
        
        if(client == null) {
            throw new IOException("Cannot find responsible remote nodes");
        }
        
        if(!client.isConnected()) {
            client.connect();
        }
        
        InputStream dataChunkIS = client.getDataChunk(uri.getClusterName(), chunk.getHashString());
        this.chunkData = new ChunkData(IOUtils.toByteArray(dataChunkIS), chunk.getOffset(), chunk.getLength());
        dataChunkIS.close();
    }
    
    private synchronized boolean isEOF() {
        if(this.offset >= this.size) {
            return true;
        }
        return false;
    }
    
    @Override
    public synchronized int read() throws IOException {
        if(isEOF()) {
            return -1;
        }
        
        loadChunkData(this.offset);
        
        int bufferOffset = (int) (this.offset - this.chunkData.getOffset());
        byte[] data = this.chunkData.getData();
        byte ch = data[bufferOffset];
        
        this.offset++;
        return ch;
    }
    
    @Override
    public synchronized int read(byte[] bytes, int off, int len) throws IOException {
        if(isEOF()) {
            return -1;
        }
            
        long lavailable = this.size - this.offset;
        int remain = len;
        if(len > lavailable) {
            remain = (int) lavailable;
        }
        
        int copied = remain;
        
        int outputBufferOffset = off;
        while(remain > 0) {
            loadChunkData(this.offset);

            int bufferOffset = (int) (this.offset - this.chunkData.getOffset());
            int bufferLength = (int) Math.min(this.chunkData.getSize() - bufferOffset, remain);
            
            System.arraycopy(this.chunkData.getData(), bufferOffset, bytes, outputBufferOffset, bufferLength);
            this.offset += bufferLength;
            outputBufferOffset += bufferLength;
            remain -= bufferLength;
        }
        return copied;
    }
    
    @Override
    public synchronized void close() throws IOException {
        if(this.clients != null) {
            Collection<HTTPUserInterfaceClient> clients = this.clients.values();
            clients.clear();
        }
        
        this.recipe = null;
        this.offset = 0;
        this.size = 0;
        this.chunkData = null;
    }
    
    @Override
    public synchronized boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void mark(int readLimit) {
        // Do nothing
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("Mark not supported");
    }
}
