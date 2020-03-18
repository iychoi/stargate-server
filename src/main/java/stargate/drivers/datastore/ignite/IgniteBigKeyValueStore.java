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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import stargate.commons.datastore.AbstractBigKeyValueStore;
import stargate.commons.datastore.BigKeyValueStoreMetadata;
import stargate.commons.datastore.BigKeyValueStoreUtils;
import stargate.commons.datastore.DataStoreProperties;
import stargate.commons.io.AbstractSeekableInputStream;
import stargate.commons.utils.ByteArray;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteBigKeyValueStore extends AbstractBigKeyValueStore {

    private static final Log LOG = LogFactory.getLog(IgniteBigKeyValueStore.class);
    
    private static final int BUFFER_SIZE = 64 * 1024;
    
    private IgniteDataStoreDriver driver;
    private IgniteDataStoreDriverConfig config;
    private IgniteDriver igniteDriver;
    private Ignite ignite;
    private String name;
    private List<String> dataNodeNames = new ArrayList<String>();
    private DataStoreProperties properties;
    private IgniteCache<String, ByteArray> store;
    private Affinity<String> affinity;
    private Map<String, Thread> putThreads = new ConcurrentHashMap<String, Thread>();
    private Map<String, List<IgniteCacheInputStream>> openedInputStreams = new HashMap<String, List<IgniteCacheInputStream>>();
    
    public IgniteBigKeyValueStore(IgniteDataStoreDriver driver, IgniteDataStoreDriverConfig config, IgniteDriver igniteDriver, String name, Collection<String> dataNodeNames, DataStoreProperties properties) throws IOException {
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
        
        if(dataNodeNames == null || dataNodeNames.isEmpty()) {
            throw new IllegalArgumentException("dataNodeNames is null or empty");
        }
        
        if(properties == null) {
            throw new IllegalArgumentException("property is null");
        }
        
        this.driver = driver;
        this.config = config;
        this.igniteDriver = igniteDriver;
        this.ignite = igniteDriver.getIgnite();
        this.name = name;
        this.dataNodeNames.addAll(dataNodeNames);
        this.properties = properties;
        
        CacheConfiguration<String, ByteArray> cc = new CacheConfiguration<String, ByteArray>();
        if(properties.isSharded()) {
            cc.setCacheMode(CacheMode.PARTITIONED);
            
            int replicaNum = properties.getReplicaNum();
            cc.setBackups(replicaNum);
        } else {
            cc.setCacheMode(CacheMode.REPLICATED);
        }
            
        RendezvousAffinityFunction affinityFunction = new RendezvousAffinityFunction();
        affinityFunction.setPartitions(2000);
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
        cc.setNodeFilter(new IgniteDataNodeFilter(this.dataNodeNames));
        
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
    public int getPartSize() {
        return this.driver.getPartSize();
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
    public AbstractSeekableInputStream getData(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        ClusterNode keyNode = this.affinity.mapKeyToNode(key);
        if(this.igniteDriver.isLocalNode(keyNode)) {
            // local
            LOG.info(String.format("getData: retrieve metadata - %s", key));
            ByteArray bytes = this.store.get(key);
            if(bytes == null) {
                return null;
            }

            BigKeyValueStoreMetadata metadata = BigKeyValueStoreMetadata.fromBytes(bytes.getArray());

            LOG.info(String.format("getData: return data - %s", key));
            IgniteCacheInputStream inputStream = new IgniteCacheInputStream(this, this.config, metadata);
            synchronized(this.openedInputStreams) {
                List<IgniteCacheInputStream> inputStreams = this.openedInputStreams.get(key);
                if(inputStreams == null) {
                    inputStreams = new ArrayList<IgniteCacheInputStream>();
                }
                inputStreams.add(inputStream);
                this.openedInputStreams.put(key, inputStreams);
            }

            return inputStream;
        } else {
            // remote
            throw new IOException("cannot access data from remote node");
        }
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
    public AbstractSeekableInputStream getDataPart(String key, int partNo) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(partNo < 0) {
            throw new IllegalArgumentException("partNo is negative");
        }
        
        ClusterNode keyNode = this.affinity.mapKeyToNode(key);
        if(this.igniteDriver.isLocalNode(keyNode)) {
            // local
            LOG.info(String.format("getDataPart: retrieve metadata - %s", key));
            ByteArray bytes = this.store.get(key);
            if(bytes == null) {
                return null;
            }

            BigKeyValueStoreMetadata metadata = BigKeyValueStoreMetadata.fromBytes(bytes.getArray());

            LOG.info(String.format("getDataPart: return data - %s, %d", key, partNo));
            int partSize = this.driver.getPartSize();
            IgniteCacheInputStream inputStream = new IgniteCacheInputStream(this, this.config, metadata, BigKeyValueStoreUtils.getPartStartOffset(partSize, partNo), BigKeyValueStoreUtils.getPartSize(metadata.getEntrySize(), partSize, partNo));
            synchronized(this.openedInputStreams) {
                List<IgniteCacheInputStream> partInputStreams = this.openedInputStreams.get(key);
                if(partInputStreams == null) {
                    partInputStreams = new ArrayList<IgniteCacheInputStream>();
                }
                partInputStreams.add(inputStream);
                this.openedInputStreams.put(key, partInputStreams);
            }

            return inputStream;
        } else {
            // remote
            throw new IOException("cannot access data from remote node");
        }
    }
    
    public void notifyInputStreamClosed(String key, IgniteCacheInputStream is) {
        synchronized(this.openedInputStreams) {
            List<IgniteCacheInputStream> inputStreams = this.openedInputStreams.get(key);
            if(inputStreams != null) {
                inputStreams.remove(is);
                this.openedInputStreams.put(key, inputStreams);
            }
        }
    }
    
    private File makeCacheFilePath(String key) {
        return new File(this.igniteDriver.getStorageRootPath(), key);
    }
    
    private void putAsync(String key, InputStream dataIS, long size) throws IOException {
        if(!this.affinity.isPrimary(this.igniteDriver.getLocalNode(), key)) {
            throw new IOException("Writing a data cache is only available via a primary node");
        }
        
        int partSize = this.driver.getPartSize();
        int parts = BigKeyValueStoreUtils.getPartNum(size, partSize);
        
        final int partNum = parts;
        File cacheFilePath = getCacheFilePath(key);
        cacheFilePath.createNewFile();
        
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                FileOutputStream os = null;
                BufferedOutputStream bos = null;
                BufferedInputStream bis = new BufferedInputStream(dataIS);
                try {
                    LOG.info(String.format("Run async data block put - %s, %d", key, partNum));
                    os = new FileOutputStream(cacheFilePath, true);
                    FileChannel channel = os.getChannel();
                    channel.truncate(0);
                    
                    byte[] buffer = new byte[BUFFER_SIZE];
                    long readOffset = 0;
                    for(int partNo=0;partNo<partNum;partNo++) {
                        int toRead = BigKeyValueStoreUtils.getPartSize(size, partSize, partNo);
                        while(toRead > 0) {
                            int readLen = bis.read(buffer, 0, Math.min(toRead, buffer.length));
                            if(readLen < 0) {
                                //EOF
                                throw new IOException("EOF found");
                            }
                            
                            readOffset += readLen;
                            os.write(buffer, 0, readLen);
                            toRead -= readLen;
                            
                            // notify
                            synchronized(openedInputStreams) {
                                List<IgniteCacheInputStream> inputStreams = openedInputStreams.get(key);
                                if(inputStreams != null) {
                                    os.flush();
                                    for(IgniteCacheInputStream is : inputStreams) {
                                        is.notifyDataAvailability(readOffset);
                                    }
                                }
                            }
                        }
                    }
                    os.flush();
                    channel.force(true);
                    os.getFD().sync();
                    os.close();
                } catch (IOException ex) {
                    LOG.error(String.format("An error occurred while putting a data block %s", key), ex);
                } catch (Exception ex) {
                    LOG.error(String.format("An error occurred while putting a data block %s", key), ex);
                } finally {
                    try {
                        dataIS.close();
                    } catch (IOException ex) {
                    }
                    
                    putThreads.remove(key);
                    LOG.info(String.format("Finished async data block put - %s, %d", key, partNum));
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
        int parts = BigKeyValueStoreUtils.getPartNum(size, partSize);
        
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
        int parts = BigKeyValueStoreUtils.getPartNum(size, partSize);
        
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
        return this.igniteDriver.getNodeNameFromClusterNode(primaryNode);
    }
    
    @Override
    public boolean isPrimaryNodeForDataLocal(String key) throws IOException {
        ClusterNode primaryNode = this.affinity.mapKeyToNode(key);
        return primaryNode.isLocal();
    }
    
    @Override
    public File getCacheFilePath(String key) throws IOException {
        return this.makeCacheFilePath(key);
    }
    
    @Override
    public Collection<String> getBackupNodesForData(String key) throws IOException {
        List<String> backupNodes = new ArrayList<String>();
        
        Collection<ClusterNode> primaryAndBackupNodes = this.affinity.mapKeyToPrimaryAndBackups(key);
        Iterator<ClusterNode> iterator = primaryAndBackupNodes.iterator();
        // skip master
        ClusterNode masterNode = iterator.next();
        while(iterator.hasNext()) {
            ClusterNode backupNode = iterator.next();
            String nodeName = this.igniteDriver.getNodeNameFromClusterNode(backupNode);
            backupNodes.add(nodeName);
        }
        return backupNodes;
    }
    
    @Override
    public Collection<String> getPrimaryAndBackupNodesForData(String key) throws IOException {
        List<String> nodes = new ArrayList<String>();
        
        Collection<ClusterNode> primaryAndBackupNodes = this.affinity.mapKeyToPrimaryAndBackups(key);
        Iterator<ClusterNode> iterator = primaryAndBackupNodes.iterator();
        while(iterator.hasNext()) {
            ClusterNode node = iterator.next();
            String nodeName = this.igniteDriver.getNodeNameFromClusterNode(node);
            nodes.add(nodeName);
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
                String partKey = BigKeyValueStoreUtils.makePartkey(key, partNo);
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
