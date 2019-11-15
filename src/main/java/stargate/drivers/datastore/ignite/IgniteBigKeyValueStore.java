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
package stargate.drivers.datastore.ignite;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import stargate.commons.datastore.AbstractBigKeyValueStore;
import stargate.commons.datastore.BigKeyValueStoreMetadata;
import stargate.commons.datastore.DataStoreProperties;
import stargate.commons.utils.ByteArray;
import stargate.commons.utils.IOUtils;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteBigKeyValueStore extends AbstractBigKeyValueStore {

    private static final Log LOG = LogFactory.getLog(IgniteBigKeyValueStore.class);
    
    private IgniteDataStoreDriver driver;
    private IgniteDataStoreDriverConfig config;
    private IgniteDriver igniteDriver;
    private Ignite ignite;
    private String name;
    private DataStoreProperties properties;
    private IgniteCache<String, ByteArray> store;
    private Affinity<String> affinity;
    
    public static String makePartkey(String key, int part) {
        return key + ":" + part;
    }
    
    public static String getPartitionKey(String partkey) {
        int index = partkey.indexOf(":");
        if(index > 0) {
            // ignore parts after ':'
            return partkey.substring(0, index);
        } else {
            return partkey;
        }
    }
    
    public static boolean isPartKey(String key) {
        int index = key.indexOf(":");
        if(index > 0) {
            return true;
        }
        return false;
    }
    
    public IgniteBigKeyValueStore(IgniteDataStoreDriver driver, IgniteDataStoreDriverConfig config, IgniteDriver igniteDriver, String name, DataStoreProperties properties) throws IOException {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(igniteDriver == null) {
            throw new IllegalArgumentException("igniteDriver is null");
        }
        
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(properties == null) {
            throw new IllegalArgumentException("property is null");
        }
        
        this.driver = driver;
        this.config = config;
        this.igniteDriver = igniteDriver;
        this.ignite = igniteDriver.getIgnite();
        this.name = name;
        this.properties = properties;
        
        CacheConfiguration<String, ByteArray> cc = new CacheConfiguration<String, ByteArray>();
        if(properties.isSharded()) {
            cc.setCacheMode(CacheMode.PARTITIONED);
            
            int replicaNum = properties.getReplicaNum();
            cc.setBackups(replicaNum);
        } else {
            cc.setCacheMode(CacheMode.REPLICATED);
        }
            
        IgniteAffinityFunction affinityFunction = new IgniteAffinityFunction();

        // remove non-data node from affinity
        IgniteCluster igniteCluster = this.ignite.cluster();
        Collection<String> nonDataNodes = properties.getNonDataNodes();

        for(ClusterNode node : igniteCluster.nodes()) {
            String nodeName = igniteDriver.getNodeNameFromClusterNode(node);
            if(nonDataNodes.contains(nodeName)) {
                affinityFunction.execludeNode(node);
            }
        }

        cc.setAffinity(affinityFunction);
        
        if(properties.isPersistent()) {
            cc.setDataRegionName(IgniteDriver.PERSISTENT_BIG_REGION_NAME);
        } else {
            cc.setDataRegionName(IgniteDriver.VOLATILE_REGION_NAME);
        }
        
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        //cc.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cc.setCopyOnRead(true);
        cc.setReadFromBackup(true);
        cc.setName(name);
        
        this.store = this.ignite.getOrCreateCache(cc);
        
        if(properties.isExpirable()) {
            TimeUnit expirationTimeUnit = properties.getExpireTimeUnit();
            long expirationTimeValue = properties.getExpireTimeVal();
            
            if(expirationTimeValue > 0) {
                Duration duration = new Duration(expirationTimeUnit, expirationTimeValue);
                CreatedExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(duration);

                this.store = store.withExpiryPolicy(expiryPolicy);
            }
        }
        
        this.affinity = this.ignite.affinity(name);
    }
    
    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public DataStoreProperties getProperties() {
        return this.properties;
    }
    
    @Override
    public boolean containsKey(String key) {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        return this.store.containsKey(key);
    }

    @Override
    public BigKeyValueStoreMetadata getMetadata(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        ByteArray bytes = this.store.get(key);
        if(bytes == null) {
            return null;
        }

        return BigKeyValueStoreMetadata.fromBytes(bytes.getArray());
    }
    
    @Override
    public InputStream getData(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        
        LOG.info(String.format("getData: lock - %s", key));
        //Lock lock = this.store.lock(key);
        //lock.lock();
        
        LOG.info(String.format("getData: retrieve metadata - %s", key));
        ByteArray bytes = this.store.get(key);
        if(bytes == null) {
            return null;
        }
        
        BigKeyValueStoreMetadata metadata = BigKeyValueStoreMetadata.fromBytes(bytes.getArray());

        /*
        LOG.info(String.format("getData: check consistency - %s", key));
        for(int partNo=0;partNo<metadata.getPartNum();partNo++) {
            String partKey = IgniteBigKeyValueStore.makePartkey(key, partNo);
            
            if(!this.store.containsKey(partKey)) {
                //lock.unlock();
                throw new IOException(String.format("Partkey %s does not exist in key-value store", partKey));
            }
        }
        */
        
        LOG.info(String.format("getData: return data - %s", key));
        //return new IgniteCacheInputStream(this.store, metadata, lock);
        return new BufferedInputStream(new IgniteCacheInputStream(this, this.config, this.store, metadata));
    }
    
    @Override
    public void put(String key, BigKeyValueStoreMetadata metadata, InputStream dataIS) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        //if(metadata == null) {
        //    throw new IllegalArgumentException("metadata is null");
        //}
        
        //if(dataIS == null) {
        //    throw new IllegalArgumentException("dataIS is null");
        //}
        
        BigKeyValueStoreMetadata newMetadata = metadata;
        if(newMetadata == null) {
            newMetadata = new BigKeyValueStoreMetadata(key);
        }
        
        //Lock lock = this.store.lock(key);
        //lock.lock();
        
        try {
            long dataSize = 0;
            int partNo = 0;
            if(dataIS != null) {
                boolean cont = true;
                
                int chunkSize = this.config.getChunkSize();
                byte[] buffer  = new byte[chunkSize];
                
                IgniteDataStreamer<String, ByteArray> streamer = this.ignite.dataStreamer(this.name);
                streamer.perNodeBufferSize(this.config.getChunkSize() * 32);
                streamer.perThreadBufferSize(this.config.getChunkSize() * 8);
                
                while(cont) {
                    String partkey = IgniteBigKeyValueStore.makePartkey(key, partNo);
                    int readLen = IOUtils.toByteArray(dataIS, buffer, chunkSize);
                    if(readLen > 0) {
                        //this.store.put(partkey, new ByteArray(buffer, readLen));
                        streamer.addData(partkey, new ByteArray(buffer, readLen));
                        partNo++;
                        dataSize += readLen;
                    } else {
                        // no data
                        cont = false;
                    }
                }
                
                newMetadata.setKey(key);
                newMetadata.setEntrySize(dataSize);
                newMetadata.setPartNum(partNo);

                byte[] metadataBytes = newMetadata.toBytes();
                //this.store.put(key, new ByteArray(metadataBytes));
                streamer.addData(key, new ByteArray(metadataBytes));
                
                streamer.close();
            } else {
                newMetadata.setKey(key);
                newMetadata.setEntrySize(dataSize);
                newMetadata.setPartNum(partNo);

                byte[] metadataBytes = newMetadata.toBytes();
                this.store.put(key, new ByteArray(metadataBytes));
            }
        } finally {
            //lock.unlock();
        }
    }
    
    @Override
    public boolean putIfAbsent(String key, BigKeyValueStoreMetadata metadata, InputStream dataIS) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        //if(metadata == null) {
        //    throw new IllegalArgumentException("value is null");
        //}
        
        //if(dataIS == null) {
        //    throw new IllegalArgumentException("dataIS is null");
        //}
        
        BigKeyValueStoreMetadata newMetadata = metadata;
        if(newMetadata == null) {
            newMetadata = new BigKeyValueStoreMetadata(key);
        }
        
        //Lock lock = this.store.lock(key);
        //lock.lock();
        
        boolean result = false;
        try {
            if(!this.store.containsKey(key)) {
                long dataSize = 0;
                int partNo = 0;
                if(dataIS != null) {
                    boolean cont = true;
                    
                    int chunkSize = this.config.getChunkSize();
                    byte[] buffer  = new byte[chunkSize];
                    
                    IgniteDataStreamer<String, ByteArray> streamer = this.ignite.dataStreamer(this.name);
                    streamer.perNodeBufferSize(this.config.getChunkSize() * 32);
                    streamer.perThreadBufferSize(this.config.getChunkSize() * 8);
                
                    while(cont) {
                        String partkey = IgniteBigKeyValueStore.makePartkey(key, partNo);
                        int readLen = IOUtils.toByteArray(dataIS, buffer, chunkSize);
                        if(readLen > 0) {
                            //this.store.put(partkey, new ByteArray(buffer, readLen));
                            streamer.addData(partkey, new ByteArray(buffer, readLen));
                            partNo++;
                            dataSize += readLen;
                        } else {
                            // no data
                            cont = false;
                        }
                    }
                    
                    newMetadata.setKey(key);
                    newMetadata.setEntrySize(dataSize);
                    newMetadata.setPartNum(partNo);

                    byte[] metadataBytes = newMetadata.toBytes();
                    //this.store.put(key, new ByteArray(metadataBytes));
                    streamer.addData(key, new ByteArray(metadataBytes));
                    
                    streamer.close();
                } else {
                    newMetadata.setKey(key);
                    newMetadata.setEntrySize(dataSize);
                    newMetadata.setPartNum(partNo);

                    byte[] metadataBytes = newMetadata.toBytes();
                    this.store.put(key, new ByteArray(metadataBytes));
                }

                result = true;
            }
        } finally {
            //lock.unlock();
        }
        
        return result;
    }
    
    @Override
    public boolean replace(String key, BigKeyValueStoreMetadata oldMetadata, BigKeyValueStoreMetadata newMetadata, InputStream dataIS) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(oldMetadata == null) {
            throw new IllegalArgumentException("oldMetadata is null");
        }
        
        //if(newMetadata == null) {
        //    throw new IllegalArgumentException("newMetadata is null");
        //}
        
        //if(dataIS == null) {
        //    throw new IllegalArgumentException("dataIS is null");
        //}
        
        BigKeyValueStoreMetadata newMetadata2 = newMetadata;
        if(newMetadata2 == null) {
            newMetadata2 = new BigKeyValueStoreMetadata(key);
        }
        
        //Lock lock = this.store.lock(key);
        //lock.lock();
        
        boolean result = false;
        try {
            ByteArray oldMetadataByteArray = this.store.get(key);
            if(oldMetadataByteArray != null) {
                BigKeyValueStoreMetadata oldMetadataExist = BigKeyValueStoreMetadata.fromBytes(oldMetadataByteArray.getArray());
                if(oldMetadataExist.equals(oldMetadata)) {
                    result = true;
                }
            }
            
            if(result) {
                // replace
                long dataSize = 0;
                int partNo = 0;
                if(dataIS != null) {
                    boolean cont = true;
                    
                    int chunkSize = this.config.getChunkSize();
                    byte[] buffer  = new byte[chunkSize];
                    
                    IgniteDataStreamer<String, ByteArray> streamer = this.ignite.dataStreamer(this.name);
                    streamer.perNodeBufferSize(this.config.getChunkSize() * 32);
                    streamer.perThreadBufferSize(this.config.getChunkSize() * 8);

                    while(cont) {
                        String partkey = IgniteBigKeyValueStore.makePartkey(key, partNo);
                        int readLen = IOUtils.toByteArray(dataIS, buffer, chunkSize);
                        if(readLen > 0) {
                            //this.store.put(partkey, new ByteArray(buffer, readLen));
                            streamer.addData(partkey, new ByteArray(buffer, readLen));
                            partNo++;
                            dataSize += readLen;
                        } else {
                            // no data
                            cont = false;
                        }
                    }
                    
                    newMetadata2.setKey(key);
                    newMetadata2.setEntrySize(dataSize);
                    newMetadata2.setPartNum(partNo);

                    byte[] metadataBytes = newMetadata2.toBytes();
                    //this.store.put(key, new ByteArray(metadataBytes));
                    streamer.addData(key, new ByteArray(metadataBytes));
                
                    streamer.close();
                } else {
                    newMetadata2.setKey(key);
                    newMetadata2.setEntrySize(dataSize);
                    newMetadata2.setPartNum(partNo);

                    byte[] metadataBytes = newMetadata2.toBytes();
                    this.store.put(key, new ByteArray(metadataBytes));
                }
            }
        } finally {
            //lock.unlock();
        }
        
        return result;
    }
    
    @Override
    public String getPrimaryNodeForData(String key) throws IOException {
        ClusterNode primaryNode = this.affinity.mapKeyToNode(key);
        if(primaryNode == null) {
            int partitionID = this.affinity.partition(key);
            LOG.error(String.format("Map key(%s) to partition(%d)", key, partitionID));
            
            ClusterNode partitionNode = this.affinity.mapPartitionToNode(partitionID);
            LOG.error(String.format("Map partition(%d) to node(%s)", partitionID, partitionNode.id()));
            return null;
        }
        return this.igniteDriver.getNodeNameFromClusterNode(primaryNode);
    }
    
    @Override
    public Collection<String> getBackupNodesForData(String key) throws IOException {
        List<String> backupNodes = new ArrayList<String>();
        
        Collection<ClusterNode> primaryAndBackupNodes = this.affinity.mapKeyToPrimaryAndBackups(key);
        for(ClusterNode node : primaryAndBackupNodes) {
            if(this.affinity.isBackup(node, key)) {
                String nodeName = this.igniteDriver.getNodeNameFromClusterNode(node);
                backupNodes.add(nodeName);
            }
        }
        return backupNodes;
    }
    
    @Override
    public Collection<String> getPrimaryAndBackupNodesForData(String key) throws IOException {
        String primaryNode = null;
        List<String> nodes = new ArrayList<String>();
        
        Collection<ClusterNode> primaryAndBackupNodes = this.affinity.mapKeyToPrimaryAndBackups(key);
        for(ClusterNode node : primaryAndBackupNodes) {
            if(this.affinity.isBackup(node, key)) {
                String nodeName = this.igniteDriver.getNodeNameFromClusterNode(node);
                nodes.add(nodeName);
            } else if(this.affinity.isPrimary(node, key)) {
                String nodeName = this.igniteDriver.getNodeNameFromClusterNode(node);
                primaryNode = nodeName;
            }
        }
        
        // put the primary node to front
        if(primaryNode != null) {
            nodes.add(0, primaryNode);
        }
        
        return nodes;
    }

    @Override
    public void remove(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        //Lock lock = this.store.lock(key);
        //lock.lock();
        
        try {
            ByteArray bytes = this.store.get(key);
            if(bytes != null) {
                BigKeyValueStoreMetadata metadata = BigKeyValueStoreMetadata.fromBytes(bytes.getArray());

                IgniteDataStreamer<String, ByteArray> streamer = this.ignite.dataStreamer(this.name);
                
                for(int partNo=0;partNo<metadata.getPartNum();partNo++) {
                    String partKey = IgniteBigKeyValueStore.makePartkey(key, partNo);
                    //this.store.remove(partKey);
                    streamer.removeData(partKey);
                }
                
                //this.store.remove(key);
                streamer.removeData(key);
                
                streamer.close();
            }
        } finally {
            //lock.unlock();
        }
    }
    
    @Override
    public void clear() {
        this.store.clear();
    }
}
