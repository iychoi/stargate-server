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
package stargate.managers.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.HexUtils;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class DataChunkCache {

    private DataChunkCacheType type;
    private String hash;
    private long version; // incremental
    private Set<String> waitingNodes = new HashSet<String>();
    private byte[] data;
    
    public static DataChunkCache createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (DataChunkCache) JsonSerializer.fromJsonFile(file, DataChunkCache.class);
    }
    
    public static DataChunkCache createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (DataChunkCache) JsonSerializer.fromJson(json, DataChunkCache.class);
    }
    
    DataChunkCache() {
    }
    
    public DataChunkCache(DataChunkCacheType type, String hash, long version, byte[] data) {
        if(type == null) {
            throw new IllegalArgumentException("type is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(version <= 0) {
            throw new IllegalArgumentException("version is negative");
        }
        
        //if(data == null) {
        //    throw new IllegalArgumentException("data is null");
        //}
        
        this.type = type;
        this.hash = hash.trim().toLowerCase();
        this.version = version;
        this.data = data;
    }
    
    public DataChunkCache(DataChunkCacheType type, String hash, long version, String waitingNode, byte[] data) {
        if(type == null) {
            throw new IllegalArgumentException("type is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(version <= 0) {
            throw new IllegalArgumentException("version is negative");
        }
        
        if(waitingNode == null || waitingNode.isEmpty()) {
            throw new IllegalArgumentException("waitingNode is null or empty");
        }
        
        //if(data == null) {
        //    throw new IllegalArgumentException("data is null");
        //}
        
        this.type = type;
        this.hash = hash.trim().toLowerCase();
        this.version = version;
        this.waitingNodes.add(waitingNode);
        this.data = data;
    }
    
    public DataChunkCache(DataChunkCacheType type, String hash, long version, Collection<String> waitingNodes, byte[] data) {
        if(type == null) {
            throw new IllegalArgumentException("type is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(version <= 0) {
            throw new IllegalArgumentException("version is negative");
        }
        
        if(waitingNodes == null) {
            throw new IllegalArgumentException("waitingNodes is null or empty");
        }
        
        //if(data == null) {
        //    throw new IllegalArgumentException("data is null");
        //}
        
        this.type = type;
        this.hash = hash.trim().toLowerCase();
        this.version = version;
        this.waitingNodes.addAll(waitingNodes);
        this.data = data;
    }
    
    @JsonProperty("type")
    public DataChunkCacheType getType() {
        return this.type;
    }
    
    @JsonProperty("type")
    public void setType(DataChunkCacheType type) {
        if(type == null) {
            throw new IllegalArgumentException("type is null");
        }
        
        this.type = type;
    }
    
    @JsonProperty("hash")
    public String getHash() {
        return this.hash;
    }
    
    @JsonProperty("hash")
    public void setHash(String hash) {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        this.hash = hash.trim().toLowerCase();
    }
    
    @JsonProperty("version")
    public long getVersion() {
        return this.version;
    }
    
    @JsonProperty("version")
    public void setVersion(long version) {
        if(version <= 0) {
            throw new IllegalArgumentException("version is negative");
        }
        
        this.version = version;
    }
    
    @JsonIgnore
    public void increaseVersion() {
        this.version++;
    }
    
    @JsonProperty("waiting_nodes")
    public Collection<String> getWaitingNodes() {
        return Collections.unmodifiableCollection(this.waitingNodes);
    }
    
    @JsonProperty("waiting_nodes")
    public void addWaitingNodes(Collection<String> nodes) {
        if(nodes == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        for(String node : nodes) {
            addWaitingNode(node);
        }
    }
    
    @JsonIgnore
    public void addWaitingNode(String node) {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is null or empty");
        }
        
        this.waitingNodes.add(node);
    }
    
    @JsonIgnore
    boolean hasWaitingNode(String node) {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is null or empty");
        }
        
        return this.waitingNodes.contains(node);
    }
        
    @JsonProperty("data")
    public byte[] getData() {
        return this.data;
    }
    
    @JsonProperty("data")
    public void setData(byte[] data) {
        //if(data == null) {
        //    throw new IllegalArgumentException("data is null");
        //}
        
        this.data = data;
    }
    
    @JsonIgnore
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(output);
        
        oos.writeInt(this.type.getNumVal());
        
        byte[] hashBytes = HexUtils.toBytes(this.hash);
        oos.writeInt(hashBytes.length);
        oos.write(hashBytes);
        
        oos.writeLong(this.version);
        
        oos.writeInt(this.waitingNodes.size());
        for(String node : this.waitingNodes) {
            byte[] nodeBytes = node.getBytes();
            oos.writeInt(nodeBytes.length);
            oos.write(nodeBytes);
        }
        
        if(this.data == null) {
            oos.writeInt(0);
        } else {
            oos.writeInt(this.data.length);
            oos.write(this.data);
        }
        
        oos.close();
        output.close();
        return output.toByteArray();
    }
    
    @JsonIgnore
    public static DataChunkCache fromBytes(byte[] buf) throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(buf);
        ObjectInputStream ois = new ObjectInputStream(input);
        
        int typeVal = ois.readInt();
        DataChunkCacheType type = DataChunkCacheType.fromNumVal(typeVal);
        
        int hashBytesLen = ois.readInt();
        byte[] hashBytes = new byte[hashBytesLen];
        ois.readFully(hashBytes, 0, hashBytesLen);
        String hash = HexUtils.toHexString(hashBytes).toLowerCase();
        
        long version = ois.readLong();
        
        int waitingNodeNum = ois.readInt();
        Set<String> waitingNodes = new HashSet<String>();
        for(int i=0;i<waitingNodeNum;i++) {
            int nodeBytesLen = ois.readInt();
            byte[] nodeBytes = new byte[nodeBytesLen];
            ois.readFully(nodeBytes, 0, nodeBytesLen);
            String waitingNode = new String(nodeBytes);
            waitingNodes.add(waitingNode);
        }
        
        int dataLen = ois.readInt();
        byte[] data = null;
        if(dataLen > 0) {
            data = new byte[dataLen];
            ois.readFully(data, 0, dataLen);
        }
        
        ois.close();
        input.close();
        
        return new DataChunkCache(type, hash, version, waitingNodes, data);
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        return "DataChunkCache{" + "type=" + type + ", hash=" + hash + ", version=" + version + ", waitingNodes=" + waitingNodes + ", data=" + data + '}';
    }
    
    @JsonIgnore
    public String toJson() throws IOException {
        return JsonSerializer.toJson(this);
    }
    
    @JsonIgnore
    public void saveTo(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer.toJsonFile(file, this);
    }
}
