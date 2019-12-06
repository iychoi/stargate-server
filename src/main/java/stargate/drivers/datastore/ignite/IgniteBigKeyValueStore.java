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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
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
    private Map<String, Thread> putThreads = new ConcurrentHashMap<String, Thread>();
    private Map<String, IgniteCacheInputStream> openedInputStreams = new ConcurrentHashMap<String, IgniteCacheInputStream>();
    private Map<String, IgniteCachePartInputStream> openedPartInputStreams = new ConcurrentHashMap<String, IgniteCachePartInputStream>();
    
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
        cc.setCopyOnRead(false);
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
    
    public IgniteDataStoreDriverConfig getConfig() {
        return this.config;
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
        
        LOG.info(String.format("getData: retrieve metadata - %s", key));
        ByteArray bytes = this.store.get(key);
        if(bytes == null) {
            return null;
        }
        
        BigKeyValueStoreMetadata metadata = BigKeyValueStoreMetadata.fromBytes(bytes.getArray());
        
        LOG.info(String.format("getData: return data - %s", key));
        IgniteCacheInputStream inputStream = new IgniteCacheInputStream(this, this.config, this.store, metadata);
        this.openedInputStreams.put(key, inputStream);
        return inputStream;
    }
    
    @Override
    public void warmData(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        ByteArray bytes = this.store.get(key);
        if(bytes == null) {
            return;
        }
        
        BigKeyValueStoreMetadata metadata = BigKeyValueStoreMetadata.fromBytes(bytes.getArray());
        warmData(key, metadata);
    }
    
    @Override
    public void warmData(String key, BigKeyValueStoreMetadata metadata) throws IOException {
        // NOOP
    }
    
    @Override
    public InputStream getDataPart(String key, int partNo) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(partNo < 0) {
            throw new IllegalArgumentException("partNo is negative");
        }
        
        LOG.info(String.format("getDataPart: retrieve metadata - %s", key));
        ByteArray bytes = this.store.get(key);
        if(bytes == null) {
            return null;
        }
        
        BigKeyValueStoreMetadata metadata = BigKeyValueStoreMetadata.fromBytes(bytes.getArray());
        
        LOG.info(String.format("getDataPart: return data - %s, %d", key, partNo));
        IgniteCachePartInputStream inputStream = new IgniteCachePartInputStream(this, this.config, this.store, metadata, partNo);
        String partKey = IgniteBigKeyValueStore.makePartkey(key, partNo);
        this.openedPartInputStreams.put(partKey, inputStream);
        return inputStream;
    }
    
    public void notifyInputStreamClosed(String key) {
        this.openedInputStreams.remove(key);
    }
    
    public void notifyPartInputStreamClosed(String key, int partNo) {
        String partKey = IgniteBigKeyValueStore.makePartkey(key, partNo);
        this.openedPartInputStreams.remove(partKey);
    }
    
    private void putAsync(String key, InputStream dataIS, long size) {
        int partSize = this.driver.getPartSize();
        int parts = (int) (size / partSize);
        if(size % partSize != 0) {
            parts++;
        }
        
        final int partNum = parts;
        
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.info(String.format("Run async data block put - %s, %d", key, partNum));
                    byte[] buffer = new byte[partSize];

                    for(int partNo=0;partNo<partNum;partNo++) {
                        String partkey = IgniteBigKeyValueStore.makePartkey(key, partNo);

                        int toRead = (int) Math.min(size - (partNo * partSize), partSize);
                        int readLen = IOUtils.toByteArray(dataIS, buffer, toRead);
                        
                        if(readLen > 0) {
                            store.put(partkey, new ByteArray(buffer, readLen));
                            
                            // notify
                            IgniteCacheInputStream inputStream = openedInputStreams.get(key);
                            if(inputStream != null) {
                                inputStream.notifyPartCompletion(partNo);
                            }
                            
                            IgniteCachePartInputStream partInputStream = openedPartInputStreams.get(partkey);
                            if(partInputStream != null) {
                                partInputStream.notifyPartCompletion();
                            }
                        }
                    }
                    
                    dataIS.close();
                    
                    putThreads.remove(key);
                    LOG.info(String.format("Finished async data block put - %s, %d", key, partNum));
                } catch (Exception ex) {
                    LOG.error(String.format("An error occurred while putting a data block %s", key), ex);
                }
            }
        });
        
        LOG.info(String.format("Start async data block put for a data block %s", key));
        this.putThreads.put(key, thread);
        thread.start();
    }
    
    @Override
    public void put(String key, InputStream dataIS, long size, byte[] extra) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        
        //if(dataIS == null) {
        //    throw new IllegalArgumentException("dataIS is null");
        //}
        
        //if(extra == null) {
        //    throw new IllegalArgumentException("extra is null");
        //}
        
        int partSize = this.driver.getPartSize();
        int parts = (int) (size / partSize);
        if(size % partSize != 0) {
            parts++;
        }
        
        if(dataIS != null) {
            putAsync(key, dataIS, size);
        }

        BigKeyValueStoreMetadata metadata = new BigKeyValueStoreMetadata(key, parts, size, extra);
        byte[] metadataBytes = metadata.toBytes();
        this.store.put(key, new ByteArray(metadataBytes));
    }
    
    @Override
    public boolean putIfAbsent(String key, InputStream dataIS, long size, byte[] extra) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        
        //if(dataIS == null) {
        //    throw new IllegalArgumentException("dataIS is null");
        //}
        
        //if(extra == null) {
        //    throw new IllegalArgumentException("extra is null");
        //}
        
        int partSize = this.driver.getPartSize();
        int parts = (int) (size / partSize);
        if(size % partSize != 0) {
            parts++;
        }
        
        boolean result = false;
        if(!this.store.containsKey(key)) {
            if(dataIS != null) {
                putAsync(key, dataIS, size);
            }

            BigKeyValueStoreMetadata metadata = new BigKeyValueStoreMetadata(key, parts, size, extra);
            byte[] metadataBytes = metadata.toBytes();
            result = this.store.putIfAbsent(key, new ByteArray(metadataBytes));
        }
        
        return result;
    }
    
    @Override
    public boolean replace(String key, BigKeyValueStoreMetadata oldMetadata, BigKeyValueStoreMetadata newMetadata) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(oldMetadata == null) {
            throw new IllegalArgumentException("oldMetadata is null");
        }
        
        if(newMetadata == null) {
            throw new IllegalArgumentException("newMetadata is null");
        }
        
        //if(dataIS == null) {
        //    throw new IllegalArgumentException("dataIS is null");
        //}
        
        byte[] oldMetadataBytes = oldMetadata.toBytes();
        byte[] newMetadataBytes = newMetadata.toBytes();
        return this.store.replace(key, new ByteArray(oldMetadataBytes), new ByteArray(newMetadataBytes));
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
        
        ByteArray bytes = this.store.get(key);
        if(bytes != null) {
            BigKeyValueStoreMetadata metadata = BigKeyValueStoreMetadata.fromBytes(bytes.getArray());

            for(int partNo=0;partNo<metadata.getPartNum();partNo++) {
                String partKey = IgniteBigKeyValueStore.makePartkey(key, partNo);
                this.store.remove(partKey);
            }

            this.store.remove(key);
        }
    }
    
    @Override
    public void clear() {
        this.store.clear();
    }
}
