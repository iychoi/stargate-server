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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import stargate.commons.utils.ByteArray;
import stargate.commons.utils.DateTimeUtils;

/**
 *
 * @author iychoi
 */
public class IgniteCacheClient {

    private static final Log LOG = LogFactory.getLog(IgniteCacheClient.class);
    
    private static final String DATA_CHUNK_CACHE_STORE = "data_chunk_cache";
    private static final long PART_WAIT_TIMEOUT_SEC = 300; // 300 seconds
    
    private IgniteClient igniteClient;
    private ClientCache<String, ByteArray> igniteCache;
    private long connectionEstablishedTime;
    private boolean connected = false;
    
    public static String makePartkey(String key, int part) {
        return key + ":" + part;
    }
    
    public IgniteCacheClient() throws IOException {
        this.connected = false;
    }
    
    public synchronized void connect() throws IOException {
        if(!this.connected) {
            ClientConfiguration clientConfig = new ClientConfiguration();
            clientConfig.setAddresses("127.0.0.1:10800");
            
            this.igniteClient = Ignition.startClient(clientConfig);

            this.connectionEstablishedTime = DateTimeUtils.getTimestamp();
            this.connected = true;
            LOG.debug("Connected to local ignite host");
            
            this.igniteCache = this.igniteClient.cache(DATA_CHUNK_CACHE_STORE);
        }
    }
    
    public synchronized void disconnect() {
        if(this.connected) {
            try {
                this.igniteClient.close();
                this.connected = false;
                LOG.debug("Disconnected from local ignite host");
            } catch (Exception ex) {
                LOG.error(ex);
            }
        }
    }
    
    public synchronized boolean isConnected() {
        return this.connected;
    }
    
    public long getConnectionEstablishedTime() {
        return this.connectionEstablishedTime;
    }
    
    public InputStream getDataChunkPart(String hash, int partNo) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(partNo < 0) {
            throw new IllegalArgumentException("partNo is negative");
        }
        
        LOG.info(String.format("getDataChunkPart: retrieve metadata - %s", hash));
        ByteArray bytes = this.igniteCache.get(hash);
        if(bytes == null) {
            return null;
        }
        
        LOG.info(String.format("getDataChunkPart: return data - %s, %d", hash, partNo));
        
        String partKey = makePartkey(hash, partNo);
        long beginTime = DateTimeUtils.getTimestamp();
        
        try {
            while(true) {
                ByteArray partDataByteArr = this.igniteCache.get(partKey);
                if(partDataByteArr == null) {
                    long curTime = DateTimeUtils.getTimestamp();
                    if(DateTimeUtils.timeElapsedSec(beginTime, curTime, PART_WAIT_TIMEOUT_SEC)) {
                        // timeout
                        LOG.error(String.format("Timeout occurred while reading a part %s", partKey));
                        throw new IOException(String.format("cannot read part key - %s after %d sec waiting", partKey, PART_WAIT_TIMEOUT_SEC));
                    } else {
                        // pending
                        LOG.info(String.format("Sleep for waiting a part %s", partKey));
                        Thread.sleep(1000);
                    }
                } else {
                    return new ByteArrayInputStream(partDataByteArr.getArray());
                }
            }
        } catch(InterruptedException ex) {
            LOG.error(ex);
            return null;
        }
    }
}
