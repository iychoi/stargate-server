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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.cache.Cache;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.DataStoreProperties;
import stargate.commons.utils.ByteArray;
import stargate.commons.utils.ObjectSerializer;
import stargate.drivers.ignite.IgniteDriver;

/**
 *
 * @author iychoi
 */
public class IgniteKeyValueStore extends AbstractKeyValueStore {

    private static final Log LOG = LogFactory.getLog(IgniteKeyValueStore.class);
    
    private IgniteDataStoreDriver driver;
    private IgniteDriver igniteDriver;
    private Ignite ignite;
    private String name;
    private Class valueClass;
    private DataStoreProperties properties;
    private IgniteCache<String, ByteArray> store;
    private Affinity<String> affinity;
    
    public IgniteKeyValueStore(IgniteDataStoreDriver driver, IgniteDriver igniteDriver, String name, Class valueClass, DataStoreProperties properties) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        if(igniteDriver == null) {
            throw new IllegalArgumentException("igniteDriver is null");
        }
        
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        if(valueClass == null) {
            throw new IllegalArgumentException("valueClass is null");
        }
        
        if(properties == null) {
            throw new IllegalArgumentException("property is null");
        }
        
        this.driver = driver;
        this.igniteDriver = igniteDriver;
        this.ignite = igniteDriver.getIgnite();
        this.name = name;
        this.valueClass = valueClass;
        this.properties = properties;
        
        CacheConfiguration<String, ByteArray> cc = new CacheConfiguration<String, ByteArray>();
        if(properties.isSharded()) {
            cc.setCacheMode(CacheMode.PARTITIONED);
            
            int replicaNum = properties.getReplicaNum();
            cc.setBackups(replicaNum);
            
            RendezvousAffinityFunction affinityFunction = new RendezvousAffinityFunction();
            affinityFunction.setExcludeNeighbors(true);
            
            // remove non-data node from affinity
            IgniteCluster igniteCluster = this.ignite.cluster();
            Collection<String> nonDataNodes = properties.getNonDataNodes();

            for(ClusterNode node : igniteCluster.nodes()) {
                UUID nodeId = node.id();

                String nodeName = igniteDriver.getNodeNameFromNodeID(nodeId);
                if(nonDataNodes.contains(nodeName)) {
                    affinityFunction.removeNode(nodeId);
                }
            }
            
            cc.setAffinity(affinityFunction);
        } else {
            cc.setCacheMode(CacheMode.REPLICATED);
        }
        
        if(properties.isPersistent()) {
            cc.setDataRegionName(IgniteDriver.PERSISTENT_REGION_NAME);
        } else {
            cc.setDataRegionName(IgniteDriver.VOLATILE_REGION_NAME);
        }
        
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);
        cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cc.setEvictionPolicy(null);
        cc.setCopyOnRead(true);
        cc.setOnheapCacheEnabled(true);
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
    public Class getValueClass() {
        return this.valueClass;
    }
    
    @Override
    public int size() {
        CachePeekMode[] cpms = { CachePeekMode.ALL };
        return this.store.size(cpms);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(String key) {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        return this.store.containsKey(key);
    }

    @Override
    public Object get(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        ByteArray bytes = this.store.get(key);
        if(bytes == null) {
            return null;
        }
        
        return ObjectSerializer.fromByteArray(bytes.getArray(), this.valueClass);
    }

    @Override
    public void put(String key, Object value) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(value == null) {
            throw new IllegalArgumentException("value is null");
        }
        
        byte[] valueBytes = ObjectSerializer.toByteArray(value);
        this.store.put(key, new ByteArray(valueBytes));
    }
    
    @Override
    public Future<Void> putAsync(String key, Object value) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(value == null) {
            throw new IllegalArgumentException("value is null");
        }
        
        byte[] valueBytes = ObjectSerializer.toByteArray(value);
        return new IgniteFutureWrapper<Void>(this.store.putAsync(key, new ByteArray(valueBytes)));
    }

    @Override
    public boolean putIfAbsent(String key, Object value) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(value == null) {
            throw new IllegalArgumentException("value is null");
        }
        
        byte[] valueBytes = ObjectSerializer.toByteArray(value);
        return this.store.putIfAbsent(key, new ByteArray(valueBytes));
    }
    
    @Override
    public boolean replace(String key, Object oldValue, Object newValue) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        if(oldValue == null) {
            throw new IllegalArgumentException("oldValue is null");
        }
        
        if(newValue == null) {
            throw new IllegalArgumentException("newValue is null");
        }
        
        byte[] oldValueBytes = ObjectSerializer.toByteArray(oldValue);
        byte[] newValueBytes = ObjectSerializer.toByteArray(newValue);
        
        return this.store.replace(key, new ByteArray(oldValueBytes), new ByteArray(newValueBytes));
    }
    
    @Override
    public String getPrimaryNodeForData(String key) throws IOException {
        ClusterNode primaryNode = this.affinity.mapKeyToNode(key);
        if(primaryNode == null) {
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
        
        this.store.remove(key);
    }
    
    @Override
    public Future<Boolean> removeAsync(String key) throws IOException {
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        return new IgniteFutureWrapper<Boolean>(this.store.removeAsync(key));
    }

    @Override
    public void clear() {
        this.store.clear();
    }
    
    @Override
    public Future<Void> clearAsync() throws IOException {
        return new IgniteFutureWrapper<Void>(this.store.clearAsync());
    }

    @Override
    public Map<String, Object> toMap() throws IOException {
        Map<String, Object> map = new HashMap<String, Object>();
        
        Iterator<Cache.Entry<String, ByteArray>> iterator = this.store.iterator();
        while(iterator.hasNext()) {
            Cache.Entry<String, ByteArray> entry = iterator.next();
            if(entry.getValue() == null) {
                map.put(entry.getKey(), null);
            } else {
                Object objValue = ObjectSerializer.fromByteArray(entry.getValue().getArray(), this.valueClass);
                map.put(entry.getKey(), objValue);
            }
        }
        return map;
    }

    @Override
    public Collection<String> keys() throws IOException {
        List<String> keys = new ArrayList<String>();
        
        Iterator<Cache.Entry<String, ByteArray>> iterator = this.store.iterator();
        while(iterator.hasNext()) {
            Cache.Entry<String, ByteArray> entry = iterator.next();
            keys.add(entry.getKey());
        }
        return keys;
    }
}
