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
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.utils.PathUtils;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.dataexport.DataExportManager;
import stargate.managers.datasource.DataSourceManager;
import stargate.managers.datastore.DataStoreManager;
import stargate.managers.recipe.RecipeManager;
import stargate.commons.transport.TransferAssignment;
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
    private Directory rootDirectoryCache = null;
    private long rootDirectoryCacheUpdateTime = 0;
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
    
    private void safeInitLocalVolumeStore() throws IOException {
        if(this.localVolumeStore == null) {
            try {
                StargateService stargateService = getStargateService();
                DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                this.localVolumeStore = keyValueStoreManager.getDriver().getKeyValueStore(VOLUME_STORE, Directory.class, EnumDataStoreProperty.DATASTORE_PROP_PERSISTENT_REPLICATED);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error("Manager is not instantiated", ex);
                throw new IOException(ex);
            } catch (DriverNotInitializedException ex) {
                LOG.error("Driver is not initialized", ex);
                throw new IOException(ex);
            }
        }
    }
    
    private boolean isLocalCluster(String clusterName) throws IOException, DriverNotInitializedException {
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }
        
        if(clusterName.equalsIgnoreCase(DataObjectURI.WILDCARD_LOCAL_CLUSTER_NAME)) {
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
                LOG.error("Manager is not instantiated", ex);
                return false;
            }
        }
    }
    
    private boolean isLocalDataObject(DataObjectURI uri) throws IOException, DriverNotInitializedException {
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
    
    private DataObjectURI makeAbsolutePath(DataObjectURI uri) throws IOException, DriverNotInitializedException {
        if(uri.isRoot()) {
            return uri;
        }
        
        String clusterName = uri.getClusterName();
        if(clusterName == null || clusterName.isEmpty() || clusterName.equalsIgnoreCase(DataObjectURI.WILDCARD_LOCAL_CLUSTER_NAME)) {
            try {
                StargateService stargateService = getStargateService();
                ClusterManager clusterManager = stargateService.getClusterManager();
                Cluster localCluster = clusterManager.getLocalCluster();
                return new DataObjectURI(localCluster.getName(), uri.getPath());
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error("Manager is not instantiated", ex);
                throw new IOException(ex);
            }
        }
        
        return uri;
    }
    
    private synchronized Directory getRootDirectory() throws IOException, DriverNotInitializedException {
        // don't need to put into the store
        // we cache root directory because it may take long time
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();
            
            long updateTime = clusterManager.getLastUpdateTime();
            
            if(this.rootDirectoryCacheUpdateTime != updateTime) {
                // something changed -> create a new directory
                this.rootDirectoryCache = null;
            }
            
            if(this.rootDirectoryCache != null) {
                return this.rootDirectoryCache;
            }
            
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
            
            this.rootDirectoryCache = rootDirectory;
            this.rootDirectoryCacheUpdateTime = updateTime;
            this.lastUpdateTime = DateTimeUtils.getTimestamp();
            return rootDirectory;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    private synchronized void buildLocalDirectoryHierarchy(Collection<DataObjectMetadata> metadatas) throws IOException, DriverNotInitializedException {
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
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    //TODO: Need to rework for better efficiency
    public synchronized void buildLocalDirectoryHierarchy() throws IOException, DriverNotInitializedException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
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
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    private synchronized Directory getLocalDirectory(DataObjectURI uri) throws IOException, FileNotFoundException {
        Directory dir = (Directory) this.localVolumeStore.get(uri.getPath());
        if(dir == null) {
            throw new FileNotFoundException(String.format("cannot find a directory (%s)", uri.getPath()));
        }

        return dir;
    }
    
    private Directory getRemoteDirectory(DataObjectURI uri) throws IOException, FileNotFoundException, DriverNotInitializedException {
        try {
            StargateService stargateService = getStargateService();
            TransportManager transportManager = stargateService.getTransportManager();
            Directory directory = transportManager.getDirectory(uri);
            if(directory == null) {
                throw new FileNotFoundException(String.format("cannot find a remote directory (%s)", uri.toUri().toASCIIString()));
            }
            return directory;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public Directory getDirectory(DataObjectURI uri) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        DataObjectURI absPath = makeAbsolutePath(uri);
        if(absPath.isRoot()) {
            // root
            return getRootDirectory();
        } else if(isLocalCluster(absPath.getClusterName())) {
            // local
            return getLocalDirectory(absPath);
        } else {
            // remote
            return getRemoteDirectory(absPath);
        }
    }
    
    public DataObjectMetadata getDataObjectMetadata(DataObjectURI uri) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        DataObjectURI absPath = makeAbsolutePath(uri);
        DataObjectURI parentPath = absPath.getParent();
        if(absPath.isRoot()) {
            // root
            Directory rootDir = getRootDirectory();
            return rootDir.toDataObjectMetadata();
        } else if(absPath.isClusterRoot()) {
            // cluster root
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
            // we get metadata list in a directory level for efficiency
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
    
    private Recipe getLocalRecipe(DataObjectURI uri) throws IOException, FileNotFoundException {
        try {
            StargateService stargateService = getStargateService();
            RecipeManager recipeManager = stargateService.getRecipeManager();
            Recipe recipe = recipeManager.getRecipe(uri.getPath());
            if(recipe == null) {
                throw new FileNotFoundException(String.format("cannot find a recipe %s", uri.getPath()));
            }
            return recipe;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    private Recipe getRemoteRecipe(DataObjectURI uri) throws IOException, FileNotFoundException, DriverNotInitializedException {
        try {
            StargateService stargateService = getStargateService();
            TransportManager transportManager = stargateService.getTransportManager();
            Recipe recipe = transportManager.getRecipe(uri);
            if(recipe == null) {
                throw new FileNotFoundException(String.format("cannot find a remote recipe (%s)", uri.toString()));
            }

            return recipe;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public Recipe getRecipe(DataObjectURI uri) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        DataObjectURI absPath = makeAbsolutePath(uri);
        if(isLocalDataObject(absPath)) {
            // local
            return getLocalRecipe(absPath);
        } else {
            // remote
            return getRemoteRecipe(absPath);
        }
    }
    
    public Recipe getRemoteRecipeWithTransferSchedule(DataObjectURI uri) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        DataObjectURI absPath = makeAbsolutePath(uri);
        if(isLocalDataObject(absPath)) {
            // local
            return getLocalRecipe(absPath);
        } else {
            // remote
            try {
                StargateService stargateService = getStargateService();
                TransportManager transportManager = stargateService.getTransportManager();
                ClusterManager clusterManager = stargateService.getClusterManager();
                
                Cluster localCluster = clusterManager.getLocalCluster();
                
                Recipe remoteRecipe = getRemoteRecipe(absPath);
                Recipe newRecipe = new Recipe(remoteRecipe.getMetadata(), remoteRecipe.getHashAlgorithm(), remoteRecipe.getChunkSize(), localCluster.getNodeNames());
                
                Collection<RecipeChunk> recipeChunks = remoteRecipe.getChunks();
                for(RecipeChunk chunk : recipeChunks) {
                    TransferAssignment assignment = transportManager.prefetchDataChunk(localCluster, remoteRecipe, chunk.getHash());
                    String assignedNodeName = assignment.getTransferNode();
                    int assignedNodeID = newRecipe.getNodeID(assignedNodeName);
                    
                    RecipeChunk newChunk = new RecipeChunk(chunk.getOffset(), chunk.getLength(), chunk.getHash());
                    newChunk.addNodeID(assignedNodeID);
                    
                    newRecipe.addChunk(newChunk);
                }
                
                return newRecipe;
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error("Manager is not instantiated", ex);
                throw new IOException(ex);
            }
        }
    }
    
    public InputStream getLocalDataChunk(String hash) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        try {
            StargateService stargateService = getStargateService();
            RecipeManager recipeManager = stargateService.getRecipeManager();
            Recipe recipe = recipeManager.getRecipeByHash(hash);
            if(recipe == null) {
                throw new FileNotFoundException(String.format("cannot find a recipe for hash - %s", hash));
            }
            
            return getLocalDataChunk(recipe, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public InputStream getLocalDataChunk(DataObjectURI uri, String hash) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        try {
            StargateService stargateService = getStargateService();
            RecipeManager recipeManager = stargateService.getRecipeManager();
            Recipe recipe = recipeManager.getRecipe(uri.getPath());
            if(recipe == null) {
                throw new FileNotFoundException(String.format("cannot find a recipe %s", uri.getPath()));
            }
            
            return getLocalDataChunk(recipe, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public InputStream getLocalDataChunk(Recipe recipe, String hash) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        try {
            StargateService stargateService = getStargateService();
            DataExportManager dataExportManager = stargateService.getDataExportManager();
            DataSourceManager dataSourceManager = stargateService.getDataSourceManager();

            // find data object metadata
            DataObjectMetadata metadata = recipe.getMetadata();
            
            // find data export entry to find a source file
            DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(metadata.getURI().getPath());
            if(dataExportEntry == null) {
                throw new IOException(String.format("cannot find a data export entry (%s)", metadata.getURI().getPath()));
            }

            // find source metadata
            URI sourceURI = dataExportEntry.getSourceURI();
            AbstractDataSourceDriver dataSourceDriver = dataSourceManager.getDriver(sourceURI);
            SourceFileMetadata sourceMetadata = dataSourceDriver.getMetadata(sourceURI);
            if(sourceMetadata == null || !sourceMetadata.exist()) {
                throw new IOException(String.format("cannot find a source file (%s)", sourceURI.toASCIIString()));
            }
            
            // get recipe chunk for offset/length 
            RecipeChunk chunk = recipe.getChunk(hash);
            if(chunk == null) {
                throw new IOException(String.format("cannot find a chunk for hash %s", hash));
            }
            
            // access the file
            return dataSourceDriver.openFile(sourceMetadata.getURI(), chunk.getOffset(), chunk.getLength());
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    private InputStream getRemoteDataChunk(DataObjectURI uri, String hash) throws IOException, FileNotFoundException, DriverNotInitializedException {
        try {
            StargateService stargateService = getStargateService();
            TransportManager transportManager = stargateService.getTransportManager();

            InputStream is = transportManager.getDataChunk(uri, hash);
            if(is == null) {
                throw new FileNotFoundException(String.format("cannot find a remote data chunk for has %s", uri.getClusterName(), hash));
            }
            return is;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public InputStream getDataChunk(DataObjectURI uri, String hash) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null or empty");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        if(this.isLocalDataObject(uri)) {
            // local
            return getLocalDataChunk(uri, hash);
        } else {
            // remote
            return getRemoteDataChunk(uri, hash);
        }
    }
    
    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public void setLastUpdateTime(long time) {
        if(time < 0) {
            throw new IllegalArgumentException("time is negative");
        }
        
        this.lastUpdateTime = time;
    }
}
