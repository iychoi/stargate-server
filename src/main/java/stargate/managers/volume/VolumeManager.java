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
package stargate.managers.volume;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.dataobject.Directory;
import stargate.commons.datasource.AbstractDataSourceDriver;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.datasource.SourceFileMetadata;
import stargate.commons.driver.NullDriver;
import stargate.commons.keyvaluestore.AbstractKeyValueStore;
import stargate.commons.keyvaluestore.EnumKeyValueStoreProperty;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.utils.PathUtils;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.dataexport.DataExportManager;
import stargate.managers.datasource.DataSourceManager;
import stargate.managers.keyvaluestore.KeyValueStoreManager;
import stargate.managers.recipe.RecipeManager;
import stargate.managers.transport.TransportManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class VolumeManager extends AbstractManager<NullDriver> {
    
    private static final Log LOG = LogFactory.getLog(VolumeManager.class);
    
    private static VolumeManager instance;
    
    private AbstractKeyValueStore localVolumeStore; // <String, Directory>
    protected long lastUpdateTime;
    
    private static final String VOLUME_STORE = "volume";
    
    public static VolumeManager getInstance(StargateService service) throws ManagerNotInstantiatedException {
        synchronized (VolumeManager.class) {
            if(instance == null) {
                instance = new VolumeManager(service);
            }
            return instance;
        }
    }
    
    public static VolumeManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (VolumeManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("VolumeManager is not started");
            }
            return instance;
        }
    }
    
    VolumeManager(StargateService service) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        this.setService(service);
    }
    
    private StargateService getStargateService() {
        return (StargateService) this.getService();
    }
    
    @Override
    public synchronized void start() throws IOException {
        super.start();
    }
    
    @Override
    public synchronized void stop() throws IOException {
        super.stop();
    }
    
    private synchronized void safeInitLocalVolumeStore() throws IOException {
        if(this.localVolumeStore == null) {
            try {
                StargateService stargateService = getStargateService();
                KeyValueStoreManager keyValueStoreManager = stargateService.getKeyValueStoreManager();
                this.localVolumeStore = keyValueStoreManager.getDriver().getKeyValueStore(VOLUME_STORE, Directory.class, EnumKeyValueStoreProperty.KEY_VALUE_STORE_PROP_PERSISTENT_REPLICATED);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
    
    private Directory getRootDirectory() throws IOException {
        // don't need to put into the store
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();
            
            long updateTime = clusterManager.getLastUpdateTime();
            
            DataObjectURI rootDataObjectURI = new DataObjectURI("", "/");
            Directory rootDirectory = new Directory(rootDataObjectURI, updateTime);
            
            String localClusterName = clusterManager.getLocalClusterName();
            Collection<String> remoteClusterNames = clusterManager.getRemoteClusterNames();
            
            
            DataObjectURI localClusterDataObjectPath = new DataObjectURI(localClusterName, "/");
            DataObjectMetadata localClusterMetdata = new DataObjectMetadata(localClusterDataObjectPath, Directory.DIRECTORY_METADATA_SIZE, true, updateTime);
            rootDirectory.addEntry(localClusterMetdata);
            
            for(String remoteClusterName : remoteClusterNames) {
                DataObjectURI remoteClusterDataObjectPath = new DataObjectURI(remoteClusterName, "/");
                DataObjectMetadata remoteClusterMetdata = new DataObjectMetadata(remoteClusterDataObjectPath, Directory.DIRECTORY_METADATA_SIZE, true, updateTime);
                rootDirectory.addEntry(remoteClusterMetdata);
            }
            
            return rootDirectory;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.info(ex);
            throw new IOException(ex);
        }
    }
    
    private void buildLocalDirectoryHierarchy(Collection<DataObjectMetadata> metadatas) throws IOException {
        if(metadatas == null) {
            throw new IllegalArgumentException("metadatas is null or empty");
        }
        
        safeInitLocalVolumeStore();
        
        this.localVolumeStore.clear();
        
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();
            String localClusterName = clusterManager.getLocalClusterName();
            long updateTime = clusterManager.getLastUpdateTime();
            
            Map<String, Directory> hierMap = new HashMap<String, Directory>();
            
            // cluster root
            DataObjectURI localClusterDataObjectPath = new DataObjectURI(localClusterName, "/");
            Directory clusterDirectory = new Directory(localClusterDataObjectPath, updateTime);
            
            hierMap.put("/", clusterDirectory);
            
            if(metadatas != null) {
                for(DataObjectMetadata metadata : metadatas) {
                    String stargatePath = metadata.getURI().getPath();
                    Collection<String> parents = PathUtils.getParents(stargatePath);
                    Directory prevDir = null;
                    for(String parentDirString : parents) {
                        Directory parentDir = hierMap.get(parentDirString);
                        if(parentDir == null) {
                            // make
                            DataObjectURI parentDataObjectPath = new DataObjectURI(localClusterName, parentDirString);
                            parentDir = new Directory(parentDataObjectPath, updateTime);
                            hierMap.put(parentDirString, parentDir);
                            
                            if(prevDir != null) {
                                prevDir.addEntry(parentDir.toDataObjectMetadata());
                            }
                        }
                        
                        prevDir = parentDir;
                    }
                    
                    String fileName = PathUtils.getFileName(stargatePath);
                    if(fileName != null) {
                        prevDir.addEntry(metadata);
                    }
                }
            }
            
            Collection<Directory> values = hierMap.values();
            for(Directory dir : values) {
                DataObjectURI uri = dir.getURI();
                String path = uri.getPath();
                this.localVolumeStore.put(path, dir);
            }
            
            this.lastUpdateTime = DateTimeUtils.getTimestamp();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.info(ex);
            throw new IOException(ex);
        }
    }
    
    public synchronized void buildLocalDirectoryHierarchy() throws IOException {
        try {
            StargateService stargateService = getStargateService();
            RecipeManager recipeManager = stargateService.getRecipeManager();
            
            Collection<Recipe> recipes = recipeManager.getRecipes();
            List<DataObjectMetadata> metadatas = new ArrayList<DataObjectMetadata>();
            for(Recipe recipe : recipes) {
                DataObjectMetadata metadata = recipe.getMetadata();
                metadatas.add(metadata);
            }
            
            buildLocalDirectoryHierarchy(metadatas);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.info(ex);
            throw new IOException(ex);
        }
    }
    
    private boolean isLocalCluster(String clusterName) throws IOException {
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }
        
        if(clusterName.equalsIgnoreCase("local")) {
            return true;
        } else {
            try {
                StargateService stargateService = getStargateService();
                ClusterManager clusterManager = stargateService.getClusterManager();
                String localClusterName = clusterManager.getLocalClusterName();
                if(clusterName.equals(localClusterName)) {
                    return true;
                }
                return false;
            } catch (ManagerNotInstantiatedException ex) {
                LOG.info(ex);
                return false;
            }
        }
    }
    
    private boolean isLocalDataObject(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        DataObjectURI absPath = makeAbsolutePath(uri);
        
        if(absPath.isRoot()) {
            return true;
        } else if(isLocalCluster(absPath.getClusterName())) {
            return true;
        }
        return false;
    }
    
    private DataObjectURI makeAbsolutePath(DataObjectURI uri) throws IOException {
        if(uri.isRoot()) {
            return uri;
        }
        
        String clusterName = uri.getClusterName();
        if(clusterName == null || clusterName.isEmpty() || clusterName.equalsIgnoreCase("local")) {
            try {
                StargateService stargateService = getStargateService();
                ClusterManager clusterManager = stargateService.getClusterManager();
                Cluster localCluster = clusterManager.getLocalCluster();
                return new DataObjectURI(localCluster.getName(), uri.getPath());
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
        
        return uri;
    }
    
    public synchronized Directory getDirectory(DataObjectURI uri) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        DataObjectURI absPath = makeAbsolutePath(uri);
        if(absPath.isRoot()) {
            return getRootDirectory();
        } else if(isLocalCluster(absPath.getClusterName())) {
            // local
            Directory dir = (Directory) this.localVolumeStore.get(absPath.getPath());
            if(dir == null) {
                throw new FileNotFoundException(String.format("cannot find a directory (%s)", absPath.getPath()));
            }
            
            return dir;
        } else {
            // remote
            try {
                StargateService stargateService = getStargateService();
                TransportManager transportManager = stargateService.getTransportManager();
                Directory directory = transportManager.getDirectory(absPath);
                if(directory == null) {
                    throw new FileNotFoundException(String.format("cannot find a remote directory (%s)", absPath.toString()));
                }
                return directory;
            } catch (ManagerNotInstantiatedException ex) {
                LOG.info(ex);
                throw new IOException(ex);
            }
        }
    }
    
    public synchronized URI getLocalDataSourcePath(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        DataObjectURI absPath = makeAbsolutePath(uri);
        if(absPath.isRoot()) {
            throw new IOException("cannot get local data path for a root directory");
        } else if(absPath.isClusterRoot()) {
            throw new IOException("cannot get local data path for a cluster directory");
        } else if(isLocalDataObject(absPath)) {
            // local
            try {
                StargateService stargateService = getStargateService();
                DataExportManager dataExportManager = stargateService.getDataExportManager();
                DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(absPath.getPath());
                if(dataExportEntry == null) {
                    throw new IOException(String.format("unable to find a data export entry for %s", absPath.getPath()));
                }
                
                return dataExportEntry.getSourceURI();
            } catch (ManagerNotInstantiatedException ex) {
                LOG.info(ex);
                throw new IOException(ex);
            }
        } else {
            // remote
            throw new IOException(String.format("cannot get a data source path from a remote cluster %s", absPath.toString()));
        }
    }
    
    public synchronized DataObjectMetadata getDataObjectMetadata(DataObjectURI uri) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        DataObjectURI absPath = makeAbsolutePath(uri);
        DataObjectURI parentPath = absPath.getParent();
        if(absPath.isRoot()) {
            // root
            Directory rootDir = getRootDirectory();
            return rootDir.toDataObjectMetadata();
        } else if(absPath.isClusterRoot()) {
            Directory rootDir = getRootDirectory();
            if(rootDir == null) {
                throw new IOException("cannot find a root directory");
            }
            
            DataObjectMetadata entry = rootDir.getEntry(absPath);
            if(entry == null) {
                throw new FileNotFoundException(String.format("cannot find a file %s", absPath.getPath()));
            }
            return entry;
        } else {
            Directory parentDir = getDirectory(parentPath);
            if(parentDir == null) {
                throw new FileNotFoundException(String.format("cannot find a directory %s", parentPath.getPath()));
            }
            
            DataObjectMetadata entry = parentDir.getEntry(absPath);
            if(entry == null) {
                throw new FileNotFoundException(String.format("cannot find a file %s", absPath.getPath()));
            }
            return entry;
        }
    }
    
    public synchronized Recipe getRecipe(DataObjectURI uri) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        DataObjectURI absPath = makeAbsolutePath(uri);
        if(isLocalDataObject(absPath)) {
            // local
            try {
                StargateService stargateService = getStargateService();
                RecipeManager recipeManager = stargateService.getRecipeManager();
                Recipe recipe = recipeManager.getRecipe(uri.getPath());
                if(recipe == null) {
                    throw new FileNotFoundException(String.format("cannot find a recipe %s", absPath.getPath()));
                }
                return recipe;
            } catch (ManagerNotInstantiatedException ex) {
                LOG.info(ex);
                throw new IOException(ex);
            }
        } else {
            // remote
            try {
                StargateService stargateService = getStargateService();
                TransportManager transportManager = stargateService.getTransportManager();
                Recipe recipe = transportManager.getRecipe(absPath);
                if(recipe == null) {
                    throw new FileNotFoundException(String.format("cannot find a remote recipe (%s)", absPath.toString()));
                }
                return recipe;
            } catch (ManagerNotInstantiatedException ex) {
                LOG.info(ex);
                throw new IOException(ex);
            }
        }
    }
    
    public InputStream getDataChunk(DataObjectURI uri, String hash) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        DataObjectURI absPath = makeAbsolutePath(uri);
        if(isLocalDataObject(absPath)) {
            // local
            try {
                StargateService stargateService = getStargateService();
                RecipeManager recipeManager = stargateService.getRecipeManager();
                DataExportManager dataExportManager = stargateService.getDataExportManager();
                DataSourceManager dataSourceManager = stargateService.getDataSourceManager();
                
                Recipe recipe = recipeManager.getRecipe(absPath.getPath());
                if(recipe == null) {
                    throw new FileNotFoundException(String.format("cannot find a recipe (%s)", absPath.getPath()));
                }
                
                RecipeChunk chunk = recipe.getChunk(hash);
                if(chunk == null) {
                    throw new IOException(String.format("cannot find a chunk for hash %s", hash));
                }
            
                DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(absPath.getPath());
                if(dataExportEntry == null) {
                    throw new IOException(String.format("cannot find a data export entry (%s)", absPath.getPath()));
                }
                
                URI sourceURI = dataExportEntry.getSourceURI();
                AbstractDataSourceDriver dataSourceDriver = dataSourceManager.getDriver(sourceURI);
                SourceFileMetadata sourcecMetadata = dataSourceDriver.getMetadata(sourceURI);
                if(sourcecMetadata == null || !sourcecMetadata.exist()) {
                    throw new IOException(String.format("cannot find a source file (%s)", sourceURI.toASCIIString()));
                }
                return dataSourceDriver.openFile(sourcecMetadata.getURI());
            } catch (ManagerNotInstantiatedException ex) {
                LOG.info(ex);
                throw new IOException(ex);
            }
        } else {
            // remote
            try {
                StargateService stargateService = getStargateService();
                TransportManager transportManager = stargateService.getTransportManager();
                
                String clusterName = absPath.getClusterName();
                
                InputStream is = transportManager.getDataChunk(clusterName, hash);
                if(is == null) {
                    throw new FileNotFoundException(String.format("cannot find a remote data chunk for has %s", clusterName, hash));
                }
                return is;
            } catch (ManagerNotInstantiatedException ex) {
                LOG.info(ex);
                throw new IOException(ex);
            }
        }
    }
    
    public InputStream getDataChunk(String hash) throws IOException, FileNotFoundException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        // local
        try {
            StargateService stargateService = getStargateService();
            RecipeManager recipeManager = stargateService.getRecipeManager();
            DataExportManager dataExportManager = stargateService.getDataExportManager();
            DataSourceManager dataSourceManager = stargateService.getDataSourceManager();

            Recipe recipe = recipeManager.getRecipeByHash(hash);
            if(recipe == null) {
                throw new FileNotFoundException(String.format("cannot find a recipe for hash %s", hash));
            }

            DataObjectMetadata metadata = recipe.getMetadata();
            RecipeChunk chunk = recipe.getChunk(hash);
            if(chunk == null) {
                throw new IOException(String.format("cannot find a chunk for hash %s", hash));
            }

            DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(metadata.getURI().getPath());
            if(dataExportEntry == null) {
                throw new IOException(String.format("cannot find a data export entry (%s)", metadata.getURI().getPath()));
            }

            URI sourceURI = dataExportEntry.getSourceURI();
            AbstractDataSourceDriver dataSourceDriver = dataSourceManager.getDriver(sourceURI);
            SourceFileMetadata sourcecMetadata = dataSourceDriver.getMetadata(sourceURI);
            if(sourcecMetadata == null || !sourcecMetadata.exist()) {
                throw new IOException(String.format("cannot find a source file (%s)", sourceURI.toASCIIString()));
            }
            return dataSourceDriver.openFile(sourcecMetadata.getURI(), chunk.getOffset(), chunk.getLength());
        } catch (ManagerNotInstantiatedException ex) {
            LOG.info(ex);
            throw new IOException(ex);
        }
    }
    
    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
}
