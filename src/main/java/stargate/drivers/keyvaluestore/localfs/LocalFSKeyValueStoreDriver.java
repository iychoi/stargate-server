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
package stargate.drivers.keyvaluestore.localfs;

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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.driver.AbstractDriverConfig;
import stargate.commons.keyvaluestore.AbstractKeyValueStore;
import stargate.commons.keyvaluestore.AbstractKeyValueStoreDriver;
import stargate.commons.keyvaluestore.AbstractKeyValueStoreDriverConfig;
import stargate.commons.keyvaluestore.EnumKeyValueStoreProperty;
import stargate.commons.utils.IOUtils;
import stargate.commons.utils.PathUtils;

/**
 *
 * @author iychoi
 */
public class LocalFSKeyValueStoreDriver extends AbstractKeyValueStoreDriver {

    private static final Log LOG = LogFactory.getLog(LocalFSKeyValueStoreDriver.class);
    
    private LocalFSKeyValueStoreDriverConfig config;
    private File rootPath;
    private Map<String, LocalFSKeyValueStore> stores = new HashMap<String, LocalFSKeyValueStore>();
    
    public LocalFSKeyValueStoreDriver(AbstractDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof LocalFSKeyValueStoreDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of LocalFSKeyValueStoreDriverConfig");
        }
        
        this.config = (LocalFSKeyValueStoreDriverConfig) config;
    }
    
    public LocalFSKeyValueStoreDriver(AbstractKeyValueStoreDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(!(config instanceof LocalFSKeyValueStoreDriverConfig)) {
            throw new IllegalArgumentException("config is not an instance of LocalFSKeyValueStoreDriverConfig");
        }
        
        this.config = (LocalFSKeyValueStoreDriverConfig) config;
    }
    
    public LocalFSKeyValueStoreDriver(LocalFSKeyValueStoreDriverConfig config) {
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        this.config = config;
    }
    
    @Override
    public synchronized void init() throws IOException {
        super.init();
        
        LOG.info("Initializing Local File System Persistent Storage Driver");
        
        this.rootPath = this.config.getRootPath();
        LOG.info("localfs root - " + this.rootPath);
    }

    @Override
    public synchronized void uninit() throws IOException {
        for(LocalFSKeyValueStore store : this.stores.values()) {
            EnumKeyValueStoreProperty property = store.getProperty();
            if(EnumKeyValueStoreProperty.isVolatile(property)) {
                store.clear();
            }
        }
        this.stores.clear();
        
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
    public AbstractKeyValueStore getKeyValueStore(String name, Class valueClass, EnumKeyValueStoreProperty property) throws IOException {
        LocalFSKeyValueStore s = this.stores.get(name);
        if(s == null) {
            if(!makeStore(name)) {
                throw new IOException("Cannot create a store (" + name + ")");
            }
            
            s = new LocalFSKeyValueStore(this, name, valueClass, property);
            this.stores.put(name, s);
        }
        return s;
    }
}
