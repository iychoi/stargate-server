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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.HexUtils;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class DataChunkCacheMetadata {

    private static final Log LOG = LogFactory.getLog(DataChunkCacheMetadata.class);
    
    private DataChunkCacheType type;
    private String hash;
    private int size;
    private int version; // incremental
    private String transferNode;
    private Set<String> waitingNodes = new HashSet<String>();
    
    public static DataChunkCacheMetadata createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (DataChunkCacheMetadata) JsonSerializer.fromJsonFile(file, DataChunkCacheMetadata.class);
    }
    
    public static DataChunkCacheMetadata createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (DataChunkCacheMetadata) JsonSerializer.fromJson(json, DataChunkCacheMetadata.class);
    }
    
    DataChunkCacheMetadata() {
    }
    
    public DataChunkCacheMetadata(DataChunkCacheType type, String hash, int size, int version) {
        if(type == null) {
            throw new IllegalArgumentException("type is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        
        if(version <= 0) {
            throw new IllegalArgumentException("version is negative");
        }
        
        this.type = type;
        this.hash = hash;
        this.size = size;
        this.transferNode = null;
        this.version = version;
    }
    
    public DataChunkCacheMetadata(DataChunkCacheType type, String hash, int size, int version, String transferNode) {
        if(type == null) {
            throw new IllegalArgumentException("type is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        
        if(version <= 0) {
            throw new IllegalArgumentException("version is negative");
        }
        
        //if(transferNode == null || transferNode.isEmpty()) {
        //    throw new IllegalArgumentException("transferNode is null or empty");
        //}
        
        this.type = type;
        this.hash = hash;
        this.size = size;
        this.transferNode = transferNode;
        this.version = version;
    }
    
    public DataChunkCacheMetadata(DataChunkCacheType type, String hash, int size, int version, String transferNode, Collection<String> waitingNodes) {
        if(type == null) {
            throw new IllegalArgumentException("type is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        
        if(version <= 0) {
            throw new IllegalArgumentException("version is negative");
        }
        
        //if(transferNode == null || transferNode.isEmpty()) {
        //    throw new IllegalArgumentException("transferNode is null or empty");
        //}
        
        //if(waitingNodes == null) {
        //    throw new IllegalArgumentException("waitingNodes is null or empty");
        //}
        
        //if(data == null) {
        //    throw new IllegalArgumentException("data is null");
        //}
        
        this.type = type;
        this.hash = hash;
        this.size = size;
        this.version = version;
        this.transferNode = transferNode;
        if(waitingNodes != null) {
            this.waitingNodes.addAll(waitingNodes);
        }
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
        
        this.hash = hash;
    }
    
    @JsonProperty("size")
    public int getSize() {
        return this.size;
    }
    
    @JsonProperty("size")
    public void setSize(int size) {
        if(size < 0) {
            throw new IllegalArgumentException("size is negative");
        }
        
        this.size = size;
    }
    
    @JsonProperty("version")
    public int getVersion() {
        return this.version;
    }
    
    @JsonProperty("version")
    public void setVersion(int version) {
        if(version <= 0) {
            throw new IllegalArgumentException("version is negative");
        }
        
        this.version = version;
    }
    
    @JsonIgnore
    public void increaseVersion() {
        if(this.version == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot increase version as it is already the max value");
        }
        this.version++;
    }
    
    @JsonProperty("transfer_node")
    public String getTransferNode() {
        return this.transferNode;
    }
    
    @JsonProperty("transfer_node")
    public void setTransferNode(String transferNode) {
        // this can be null
        //if(transferNode == null || transferNode.isEmpty()) {
        //    throw new IllegalArgumentException("transferNode is null or empty");
        //}
        
        this.transferNode = transferNode;
    }
    
    @JsonIgnore
    public void clearTransferNode() {
        this.transferNode = null;
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
        
        if(!this.waitingNodes.contains(node)) {
            this.waitingNodes.add(node);
        }
    }
    
    @JsonIgnore
    public void clearWaitingNode() {
        this.waitingNodes.clear();
    }
    
    @JsonIgnore
    boolean hasWaitingNode(String node) {
        if(node == null || node.isEmpty()) {
            throw new IllegalArgumentException("node is null or empty");
        }
        
        return this.waitingNodes.contains(node);
    }
    
    @JsonIgnore
    public byte[] toBytes() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(output);
        
        oos.writeInt(this.type.getNumVal());
        
        byte[] hashBytes = HexUtils.toBytes(this.hash);
        oos.writeInt(hashBytes.length);
        oos.write(hashBytes);
        
        oos.writeInt(this.size);
        oos.writeInt(this.version);
        
        if(this.transferNode == null) {
            oos.writeInt(0);
        } else {
            byte[] transferNodeName = this.transferNode.getBytes();
            oos.writeInt(transferNodeName.length);
            oos.write(transferNodeName);
        }
        
        
        oos.writeInt(this.waitingNodes.size());
        for(String node : this.waitingNodes) {
            byte[] nodeBytes = node.getBytes();
            oos.writeInt(nodeBytes.length);
            oos.write(nodeBytes);
        }
        
        oos.close();
        output.close();
        return output.toByteArray();
    }
    
    @JsonIgnore
    public static DataChunkCacheMetadata fromBytes(byte[] buf) throws IOException {
        ByteArrayInputStream input = new ByteArrayInputStream(buf);
        ObjectInputStream ois = new ObjectInputStream(input);
        
        int typeVal = ois.readInt();
        DataChunkCacheType type = DataChunkCacheType.fromNumVal(typeVal);
        
        int hashBytesLen = ois.readInt();
        byte[] hashBytes = new byte[hashBytesLen];
        ois.readFully(hashBytes, 0, hashBytesLen);
        String hash = HexUtils.toHexString(hashBytes).toLowerCase();
        
        int size = ois.readInt();
        int version = ois.readInt();
        
        String transferNode = null;
        int transferNodeBytesLen = ois.readInt();
        if(transferNodeBytesLen > 0) {
            byte[] transferNodeBytes = new byte[transferNodeBytesLen];
            ois.readFully(transferNodeBytes, 0, transferNodeBytesLen);
            transferNode = new String(transferNodeBytes);
        }
        
        int waitingNodeNum = ois.readInt();
        Set<String> waitingNodes = new HashSet<String>();
        for(int i=0;i<waitingNodeNum;i++) {
            int nodeBytesLen = ois.readInt();
            byte[] nodeBytes = new byte[nodeBytesLen];
            ois.readFully(nodeBytes, 0, nodeBytesLen);
            String waitingNode = new String(nodeBytes);
            waitingNodes.add(waitingNode);
        }
        
        ois.close();
        input.close();
        
        return new DataChunkCacheMetadata(type, hash, size, version, transferNode, waitingNodes);
    }
    
    @Override
    @JsonIgnore
    public String toString() {
        return "DataChunkCache{" + "type=" + type + ", hash=" + hash + ", size=" + size + ", version=" + version + ", transferNode=" + transferNode + ", waitingNodes=" + waitingNodes + '}';
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
