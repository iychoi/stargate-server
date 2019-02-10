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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.fs.FSInputStream;
import stargate.commons.utils.IOUtils;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;

/**
 * NEED TO REWORK THIS CODE
 * @author iychoi
 */
public class ParallelHTTPChunkInputStream extends FSInputStream {

    public static final String DEFAULT_NODE_NAME = "*";
    public static final String LOCAL_NODE_NAME = "localhost";
    public static final Integer PARALLEL_REQUESTS = 20;
    public static final Integer PARALLEL_DOWNLOADS = 5;
    
    // node-name to client mapping
    private Map<String, HTTPUserInterfaceClient> clients = new HashMap<String, HTTPUserInterfaceClient>();
    private Recipe recipe;
    private long offset;
    private long size;
    private SortedSet<ChunkData> chunkDataList = Collections.synchronizedSortedSet(new TreeSet<ChunkData>());
    private SortedMap<Long, Future<ChunkData>> chunkRequests = Collections.synchronizedSortedMap(new TreeMap<Long, Future<ChunkData>>());
    private ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_DOWNLOADS);
    
    public ParallelHTTPChunkInputStream(HTTPUserInterfaceClient client, Recipe recipe) {
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
    
    public ParallelHTTPChunkInputStream(Map<String, HTTPUserInterfaceClient> clients, Recipe recipe) {
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
        this.recipe = recipe;
        this.offset = 0;
        this.size = recipe.getMetadata().getSize();
    }
    
    @Override
    public synchronized long getPos() throws IOException {
        return this.offset;
    }
    
    @Override
    public synchronized int available() throws IOException {
        int available = 0;
        boolean foundCurrent = false;
        long nextOffset = -1;
        Iterator<ChunkData> iterator = this.chunkDataList.iterator();
        
        while(iterator.hasNext()) {
            ChunkData chunk = iterator.next();
            if(chunk != null) {
                // find current
                long chunkOffset = chunk.getOffset();
                long chunkSize = chunk.getSize();
                
                if(!foundCurrent) {
                    if(chunkOffset <= this.offset &&
                        (chunkOffset + chunkSize > this.offset)) {
                        // safe to reuse
                        available += (int) (chunkSize - (this.offset - chunkOffset));
                        foundCurrent = true;
                        nextOffset = chunkOffset + chunkSize;
                    }
                } else {
                    // we already found current chunk
                    if(chunkOffset == nextOffset) {
                        available += (int) chunkSize;
                        nextOffset = chunkOffset + chunkSize;
                    } else {
                        break;
                    }
                }
            }
        }
        return available;
    }
    
    @Override
    public synchronized void seek(long l) throws IOException {
        if(l < 0) {
            throw new IOException("cannot seek to negative offset : " + l);
        }
        
        deleteOldChunkData(l);
        
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
        
        deleteOldChunkData(l);
        
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
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
    
    private synchronized void deleteOldChunkData(long offset) throws IOException {
        Iterator<ChunkData> iterator = this.chunkDataList.iterator();
        
        while(iterator.hasNext()) {
            ChunkData chunk = iterator.next();
            if(chunk != null) {
                // find current
                long chunkOffset = chunk.getOffset();
                long chunkSize = chunk.getSize();
                
                if((chunkOffset + chunkSize) <= offset) {
                    //past chunks
                    iterator.remove();
                } else {
                    break;
                }
            }
        }
    }
    
    private synchronized ChunkData findChunkData(long offset) throws IOException {
        Iterator<ChunkData> iterator = this.chunkDataList.iterator();
        while(iterator.hasNext()) {
            ChunkData chunk = iterator.next();
            if(chunk != null) {
                // find
                long chunkOffset = chunk.getOffset();
                long chunkSize = chunk.getSize();
                
                if(chunkOffset <= offset &&
                    (chunkOffset + chunkSize > offset)) {
                    // found
                    return chunk;
                } else if(chunkOffset > offset) {
                    break;
                }
            }
        }
        return null;
    }
    
    private ChunkData downloadChunkData(long offset) throws IOException {
        if(offset >= this.size) {
            return null;
        }
        
        // load
        RecipeChunk chunk = this.recipe.getChunk(offset);
        
        DataObjectMetadata metadata = this.recipe.getMetadata();
        DataObjectURI uri = metadata.getURI();
        
        Collection<Integer> nodeIDs = chunk.getNodeIDs();
        Collection<String> nodeNames = recipe.getNodeNames(nodeIDs);
        
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
        
        InputStream dataChunkIS = client.getDataChunk(uri, chunk.getHash());
        ChunkData newChunkData = new ChunkData(IOUtils.toByteArray(dataChunkIS), chunk.getOffset(), chunk.getLength());
        dataChunkIS.close();
        return newChunkData;
    }
    
    private synchronized void requestDownload(long offset) throws IOException {
        if(!this.chunkRequests.containsKey(offset)) {
            Future<ChunkData> future = (Future) this.executor.submit(new Callable<ChunkData>(){
                @Override
                public ChunkData call() throws IOException {
                    ChunkData chunkData = downloadChunkData(offset);
                    return chunkData;
                }
            });
            this.chunkRequests.put(offset, future);
        }
    }
    
    private synchronized void loadChunkData(long offset) throws IOException {
        if(offset >= this.size) {
            return;
        }
        
        // check if there already is
        ChunkData chunkData = findChunkData(offset);
        if(chunkData != null) {
            // we found
            return;
        }
        
        RecipeChunk chunk = this.recipe.getChunk(offset);
        
        long chunkOffset = chunk.getOffset();
        long chunkSize = chunk.getLength();
        
        // make a new requst
        long requestChunkOffset = chunkOffset;
        long requestChunkSize = chunkSize;
        for(int i=0;i<PARALLEL_REQUESTS;i++) {
            ChunkData requestChunkData = findChunkData(requestChunkOffset);
            if(requestChunkData == null) {
                requestDownload(requestChunkOffset);
                requestChunkOffset += requestChunkSize;

                if(requestChunkOffset >= this.size) {
                    break;
                }
                
                RecipeChunk requestChunk = this.recipe.getChunk(requestChunkOffset);
                requestChunkOffset = requestChunk.getOffset();
                requestChunkSize = requestChunk.getLength();
            }
        }
        
        Future<ChunkData> future = this.chunkRequests.get(chunkOffset);
        try {
            // block until it completes
            chunkData = future.get();
            this.chunkDataList.add(chunkData);
        } catch (Exception ex) {
            throw new IOException(ex);
        }
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
        ChunkData chunkData = findChunkData(this.offset);
        
        int bufferOffset = (int) (this.offset - chunkData.getOffset());
        byte[] data = chunkData.getData();
        byte ch = data[bufferOffset];
        
        this.offset++;
        
        deleteOldChunkData(this.offset);
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
            
            ChunkData chunkData = findChunkData(this.offset);

            int bufferOffset = (int) (this.offset - chunkData.getOffset());
            int bufferLength = (int) Math.min(chunkData.getSize() - bufferOffset, remain);
            
            System.arraycopy(chunkData.getData(), bufferOffset, bytes, outputBufferOffset, bufferLength);
            this.offset += bufferLength;
            outputBufferOffset += bufferLength;
            remain -= bufferLength;
            
            deleteOldChunkData(this.offset);
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
        this.chunkDataList.clear();
        this.chunkRequests.clear();
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
