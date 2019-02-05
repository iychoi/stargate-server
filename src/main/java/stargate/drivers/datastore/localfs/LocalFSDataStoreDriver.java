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
package stargate.drivers.datastore.localfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.AbstractDataStoreDriver;
import stargate.commons.datastore.AbstractDataStoreDriverConfig;
import stargate.commons.datastore.AbstractQueue;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.utils.IOUtils;
import stargate.commons.utils.PathUtils;

/**
 *
 * @author iychoi
 */
public class LocalFSDataStoreDriver extends AbstractDataStoreDriver {

    private static final Log LOG = LogFactory.getLog(LocalFSDataStoreDriver.class);
    
    private LocalFSDataStoreDriverConfig config;
    private File rootPath;
    private Map<String, LocalFSKeyValueStore> kvStores = new HashMap<String, LocalFSKeyValueStore>();
    private Map<String, LocalFSQueue> queues = new HashMap<String, LocalFSQueue>();
    
    public LocalFSDataStoreDriver(AbstractDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof LocalFSDataStoreDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of LocalFSDataStoreDriverConfig");
        }
        
        this.config = (LocalFSDataStoreDriverConfig) config;
    }
    
    public LocalFSDataStoreDriver(AbstractDataStoreDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof LocalFSDataStoreDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of LocalFSDataStoreDriverConfig");
        }
        
        this.config = (LocalFSDataStoreDriverConfig) config;
    }
    
    public LocalFSDataStoreDriver(LocalFSDataStoreDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void init() throws IOException {
        super.init();
        
        LOG.debug("Initializing Local File System Persistent Storage Driver");
        
        this.rootPath = this.config.getRootPath();
        LOG.debug("localfs root - " + this.rootPath);
    }

    @Override
    public synchronized void uninit() throws IOException {
        for(LocalFSKeyValueStore store : this.kvStores.values()) {
            EnumDataStoreProperty property = store.getProperty();
            if(EnumDataStoreProperty.isVolatile(property)) {
                store.clear();
            }
        }
        this.kvStores.clear();
        
        super.uninit();
    }
    
    private File getStorePath(String store) {
        String rootPath = this.rootPath.getAbsolutePath();
        String storePath = PathUtils.concatPath(rootPath, store);
        return new File(storePath);
    }
    
    private File getKeyPath(String store, String key) {
        String rootPath = this.rootPath.getAbsolutePath();
        String storePath = PathUtils.concatPath(rootPath, store);
        String keyPath = PathUtils.concatPath(storePath, key);
        return new File(keyPath);
    }
    
    private boolean clearRecursive(File f) {
        if(f.isDirectory()) {
            File[] listFiles = f.listFiles();
            for(File file : listFiles) {
                if(file.isDirectory()) {
                    if(!removeRecursive(file)) {
                        return false;
                    }
                } else {
                    if(!file.delete()) {
                        return false;
                    }
                }
            }
            return true;
        }
        return false;
    }
    
    private boolean removeRecursive(File f) {
        if(f.isDirectory()) {
            File[] listFiles = f.listFiles();
            for(File file : listFiles) {
                if(file.isDirectory()) {
                    if(!removeRecursive(file)) {
                        return false;
                    }
                } else {
                    if(!file.delete()) {
                        return false;
                    }
                }
            }
        }
        
        return f.delete();
    }

    public boolean existKey(String store, String key) throws IOException {
        if(store == null || store.isEmpty()) {
            throw new IllegalArgumentException("store is null or empty");
        }
        
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        File localFile = getKeyPath(store, key);
        return localFile.exists();
    }
    
    public Collection<String> listKeys(String store) throws IOException, FileNotFoundException {
        if(store == null || store.isEmpty()) {
            throw new IllegalArgumentException("store is null or empty");
        }
        
        File localFile = getStorePath(store);
        if(!localFile.exists()) {
            throw new FileNotFoundException("directory (" + localFile.getAbsolutePath() + ") does not exist");
        }
        
        if(!localFile.isDirectory()) {
            throw new FileNotFoundException(localFile.getAbsolutePath() + " is not a directory");
        }
        
        File[] listFiles = localFile.listFiles();
        List<String> entries = new ArrayList<String>();
        if(listFiles != null && listFiles.length > 0) {
            for(File file : listFiles) {
                entries.add(file.getName());
            }
        }
        
        return Collections.unmodifiableCollection(entries);
    }
    
    public boolean makeStore(String store) throws IOException {
        if(store == null || store.isEmpty()) {
            throw new IllegalArgumentException("store is null or empty");
        }
        
        File localFile = getStorePath(store);
        if(!localFile.exists()) {
            return localFile.mkdirs();
        }
        
        if(!localFile.isDirectory()) {
            throw new FileNotFoundException(localFile.getAbsolutePath() + " is not a directory");
        }
        
        return true;
    }
    
    public boolean clearStore(String store) throws IOException, FileNotFoundException {
        if(store == null || store.isEmpty()) {
            throw new IllegalArgumentException("store is null or empty");
        }
        
        File localFile = getStorePath(store);
        if(!localFile.exists()) {
            throw new FileNotFoundException("directory (" + localFile.getAbsolutePath() + ") does not exist");
        }
        
        if(!localFile.isDirectory()) {
            throw new FileNotFoundException(localFile.getAbsolutePath() + " is not a directory");
        }
        
        return clearRecursive(localFile);
    }
    
    public boolean removeStore(String store) throws IOException, FileNotFoundException {
        if(store == null || store.isEmpty()) {
            throw new IllegalArgumentException("store is null or empty");
        }
        
        File localFile = getStorePath(store);
        if(!localFile.exists()) {
            throw new FileNotFoundException("directory (" + localFile.getAbsolutePath() + ") does not exist");
        }
        
        if(!localFile.isDirectory()) {
            throw new FileNotFoundException(localFile.getAbsolutePath() + " is not a directory");
        }
        
        return removeRecursive(localFile);
    }
    
    public byte[] getBytes(String store, String key) throws IOException {
        if(store == null || store.isEmpty()) {
            throw new IllegalArgumentException("store is null or empty");
        }
        
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        File localFile = getKeyPath(store, key);
        if(localFile.exists()) {
            FileInputStream is = new FileInputStream(localFile);
            byte[] bytes = IOUtils.read(is);
            is.close();
            return bytes;
        } else {
            return null;
        }
    }
    
    public void putBytes(String store, String key, byte[] value) throws IOException {
        if(store == null || store.isEmpty()) {
            throw new IllegalArgumentException("store is null or empty");
        }
        
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        File localFile = getKeyPath(store, key);
        if(localFile.exists()) {
            // delete first
            localFile.delete();
        }
        
        FileOutputStream os = new FileOutputStream(localFile);
        IOUtils.write(os, value);
        os.close();
    }
    
    public void remove(String store, String key) throws IOException {
        if(store == null || store.isEmpty()) {
            throw new IllegalArgumentException("store is null or empty");
        }
        
        if(key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key is null or empty");
        }
        
        File localFile = getKeyPath(store, key);
        if(localFile.exists()) {
            // delete
            localFile.delete();
        }
    }

    @Override
    public AbstractKeyValueStore getKeyValueStore(String name, Class valueClass, EnumDataStoreProperty property) throws IOException {
        LocalFSKeyValueStore s = this.kvStores.get(name);
        if(s == null) {
            if(!makeStore(name)) {
                throw new IOException("Cannot create a kv store (" + name + ")");
            }
            
            s = new LocalFSKeyValueStore(this, name, valueClass, property);
            this.kvStores.put(name, s);
        }
        return s;
    }
    
    @Override
    public AbstractKeyValueStore getKeyValueStore(String name, Class valueClass, EnumDataStoreProperty property, TimeUnit timeunit, long timeval) throws IOException {
        LocalFSKeyValueStore s = this.kvStores.get(name);
        if(s == null) {
            if(!makeStore(name)) {
                throw new IOException("Cannot create a kv store (" + name + ")");
            }
            
            s = new LocalFSKeyValueStore(this, name, valueClass, property, timeunit, timeval);
            this.kvStores.put(name, s);
        }
        return s;
    }

    @Override
    public AbstractQueue getQueue(String name, Class valueClass, EnumDataStoreProperty property) throws IOException {
        LocalFSQueue q = this.queues.get(name);
        if(q == null) {
            if(!makeStore(name)) {
                throw new IOException("Cannot create a queue (" + name + ")");
            }
            
            q = new LocalFSQueue(this, name, valueClass, property);
            this.queues.put(name, q);
        }
        return q;
    }
}
