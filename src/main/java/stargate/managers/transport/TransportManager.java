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
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.dataobject.Directory;
import stargate.commons.datasource.AbstractDataSourceDriver;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.driver.AbstractDriver;
import stargate.commons.driver.DriverFailedToLoadException;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.AbstractQueue;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerConfig;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.transport.AbstractTransportClient;
import stargate.commons.transport.AbstractTransportDriver;
import stargate.commons.transport.TransportServiceInfo;
import stargate.commons.utils.DateTimeUtils;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.dataexport.DataExportManager;
import stargate.managers.datasource.DataSourceManager;
import stargate.managers.datastore.DataStoreManager;
import stargate.managers.recipe.RecipeManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class TransportManager extends AbstractManager<AbstractTransportDriver> {

    private static final Log LOG = LogFactory.getLog(TransportManager.class);
    
    private static TransportManager instance;
    
    private AbstractKeyValueStore remoteDirectoryCacheStore; // <DataObjectURI, Directory>
    private final Object remoteDirectoryCacheStoreSyncObj = new Object();
    private AbstractKeyValueStore remoteRecipeCacheStore; // <DataObjectURI, Recipe>
    private final Object remoteRecipeCacheStoreSyncObj = new Object();
    private AbstractKeyValueStore dataChunkCacheStore; // <String, byte[]> key = hashstring
    private final Object dataChunkCacheStoreSyncObj = new Object();
    private AbstractQueue prefetchTransferQueue; // <TransferEvent>
    private AbstractKeyValueStore transferInQueueStore; //<String, TransferAssignment> key = hashstring
    private final Object transferSyncObj = new Object();
    private AbstractContactNodeDeterminationAlgorithm contactNodeDeterminationAlgorithm;
    private AbstractTransferLayoutAlgorithm transferLayoutAlgorithm;
    protected long lastUpdateTime;
    
    private static final String REMOTE_DIRECTORY_CACHE_STORE = "remote_dir_cache";
    private static final String REMOTE_RECIPE_CACHE_STORE = "remote_recipe_cache";
    private static final String DATA_CHUNK_CACHE_STORE = "data_chunk_cache";
    private static final String PREFETCH_TRANSFER_QUEUE = "prefetch_transfer_queue";
    private static final String TRANSFER_IN_QUEUE_STORE = "transfer_in_queue";
    
    public static TransportManager getInstance(StargateService service, Collection<AbstractTransportDriver> drivers) throws ManagerNotInstantiatedException {
        synchronized (TransportManager.class) {
            if(instance == null) {
                instance = new TransportManager(service, drivers);
            }
            return instance;
        }
    }
    
    public static TransportManager getInstance(StargateService service, ManagerConfig config) throws ManagerNotInstantiatedException {
        synchronized (TransportManager.class) {
            if(instance == null) {
                if(config == null) {
                    throw new IllegalArgumentException("config is null");
                }
                
                try {
                    // type cast
                    Collection<AbstractDriver> drivers = (Collection<AbstractDriver>) config.getDrivers();
                    List<AbstractTransportDriver> transportDrivers = new ArrayList<AbstractTransportDriver>();
                    for(AbstractDriver driver : drivers) {
                        transportDrivers.add((AbstractTransportDriver) driver);
                    }
                    instance = new TransportManager(service, transportDrivers);
                } catch (DriverFailedToLoadException ex) {
                    LOG.error(ex);
                    throw new ManagerNotInstantiatedException(ex.toString());
                }
            }
            return instance;
        }
    }
    
    public static TransportManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (TransportManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("TransportManager is not started");
            }
            return instance;
        }
    }
    
    TransportManager(StargateService service, Collection<AbstractTransportDriver> drivers) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(drivers == null || drivers.isEmpty()) {
            throw new IllegalArgumentException("drivers is null or empty");
        }
        
        this.setService(service);
        
        for(AbstractTransportDriver driver : drivers) {
            this.drivers.add(driver);
        }
    }
    
    public AbstractTransportDriver getDriver() {
        if(this.drivers.size() > 0) {
            return this.drivers.get(0);
        }
        return null;
    }
    
    private StargateService getStargateService() {
        return (StargateService) this.getService();
    }
    
    @Override
    public synchronized void start() throws IOException {
        super.start();
        
        for(AbstractTransportDriver driver : drivers) {
            driver.startServer();
        }
        
        // init algorithms
        this.contactNodeDeterminationAlgorithm = new RoundRobinContactNodeDeterminationAlgorithm(this.service, this);
        
        safeInitDataChunkCacheStore(); //  chunk cache store must be called before next line is executed
        this.transferLayoutAlgorithm = new StaticTransferLayoutAlgorithm(this.service, this, this.dataChunkCacheStore);
    }
    
    @Override
    public synchronized void stop() throws IOException {
        for(AbstractTransportDriver driver : drivers) {
            driver.stopServer();
        }
        
        super.stop();
    }
    
    public TransportServiceInfo getServiceInfo() throws IOException {
        AbstractTransportDriver driver = getDriver();
        URI serviceURI = driver.getServiceURI();
        return new TransportServiceInfo(driver.getClass().getName(), serviceURI);
    }
    
    private void safeInitRemoteDirectoryCacheStore() throws IOException {
        synchronized(this.remoteDirectoryCacheStoreSyncObj) {
            if(this.remoteDirectoryCacheStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    this.remoteDirectoryCacheStore = keyValueStoreManager.getDriver().getKeyValueStore(REMOTE_DIRECTORY_CACHE_STORE, Directory.class, EnumDataStoreProperty.DATASTORE_PROP_VOLATILE_REPLICATED, TimeUnit.MINUTES, 5);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }
        }
    }
    
    private void safeInitRemoteRecipeCacheStore() throws IOException {
        synchronized(this.remoteRecipeCacheStoreSyncObj) {
            if(this.remoteRecipeCacheStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    this.remoteRecipeCacheStore = keyValueStoreManager.getDriver().getKeyValueStore(REMOTE_RECIPE_CACHE_STORE, Recipe.class, EnumDataStoreProperty.DATASTORE_PROP_VOLATILE_REPLICATED, TimeUnit.DAYS, 1);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }
        }
    }
    
    private void safeInitDataChunkCacheStore() throws IOException {
        synchronized(this.dataChunkCacheStoreSyncObj) {
            if(this.dataChunkCacheStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    this.dataChunkCacheStore = keyValueStoreManager.getDriver().getKeyValueStore(DATA_CHUNK_CACHE_STORE, byte[].class, EnumDataStoreProperty.DATASTORE_PROP_PERSISTENT_DISTRIBUTED);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }
        }
    }
    
    private void safeInitTransferQueueStore() throws IOException {
        synchronized(this.transferSyncObj) {
            if(this.prefetchTransferQueue == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    this.prefetchTransferQueue = keyValueStoreManager.getDriver().getQueue(PREFETCH_TRANSFER_QUEUE, TransferEvent.class, EnumDataStoreProperty.DATASTORE_PROP_VOLATILE_REPLICATED);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }

            if(this.transferInQueueStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    this.transferInQueueStore = keyValueStoreManager.getDriver().getKeyValueStore(TRANSFER_IN_QUEUE_STORE, TransferAssignment.class, EnumDataStoreProperty.DATASTORE_PROP_VOLATILE_REPLICATED, TimeUnit.MINUTES, 5);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }
        }
    }
    
    public DataChunkCache getDataChunkCache(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkCacheStoreSyncObj) {
            byte[] bytes = (byte[]) this.dataChunkCacheStore.get(hash);
            if(bytes == null) {
                return null;
            }
            
            return DataChunkCache.fromBytes(bytes);
        }
    }
    
    public boolean hasDataChunkCache(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkCacheStoreSyncObj) {
            return this.dataChunkCacheStore.containsKey(hash);
        }
    }
    
    public boolean hasPendingDataChunkCache(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkCacheStoreSyncObj) {
            if(!this.dataChunkCacheStore.containsKey(hash)) {
                return false;
            }
            
            byte[] bytes = (byte[]) this.dataChunkCacheStore.get(hash);
            if(bytes == null) {
                return false;
            }
            
            DataChunkCache dataChunkCache = DataChunkCache.fromBytes(bytes);
            if(dataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PENDING) {
                return true;
            }
            return false;
        }
    }
    
    public boolean hasDataChunkCacheData(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkCacheStoreSyncObj) {
            if(!this.dataChunkCacheStore.containsKey(hash)) {
                return false;
            }
            
            byte[] bytes = (byte[]) this.dataChunkCacheStore.get(hash);
            if(bytes == null) {
                return false;
            }
            
            DataChunkCache dataChunkCache = DataChunkCache.fromBytes(bytes);
            if(dataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT) {
                return true;
            }
            return false;
        }
    }
    
    public void clearDataChunkCaches() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkCacheStoreSyncObj) {
            this.dataChunkCacheStore.clear();
        }
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
    }
    
    public void ___addDataChunkCache(String hash, DataChunkCache dataChunkCache) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(dataChunkCache == null) {
            throw new IllegalArgumentException("dataChunkCache is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkCacheStoreSyncObj) {
            if(this.dataChunkCacheStore.containsKey(hash)) {
                if(dataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT) {
                    // overwrite
                    this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                    this.lastUpdateTime = DateTimeUtils.getTimestamp();
                }
            } else {
                this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                this.lastUpdateTime = DateTimeUtils.getTimestamp();
            }
        }
    }
    
    public void removeDataChunkCache(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkCacheStoreSyncObj) {
            this.dataChunkCacheStore.remove(hash);
        }
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
    }
    
    public void putPendingDataChunkCache(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkCacheStoreSyncObj) {
            Lock lock = this.dataChunkCacheStore.getKeyLock(hash);
            lock.lock();
            try {
                if(!this.dataChunkCacheStore.containsKey(hash)) {
                    DataChunkCache dataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, 1, null);
                    this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                    this.lastUpdateTime = DateTimeUtils.getTimestamp();
                }
            } finally {
                lock.unlock();
            }
        }
    }
    
    public void putPendingDataChunkCache(String hash, String waitingNodeName) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(waitingNodeName == null || waitingNodeName.isEmpty()) {
            throw new IllegalArgumentException("waitingNodeName is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkCacheStoreSyncObj) {
            Lock lock = this.dataChunkCacheStore.getKeyLock(hash);
            lock.lock();
            try {
                if(this.dataChunkCacheStore.containsKey(hash)) {
                    DataChunkCache dataChunkCache = (DataChunkCache) this.dataChunkCacheStore.get(hash);
                    if(dataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PENDING) {
                        // overwrite
                        if(!dataChunkCache.hasWaitingNode(waitingNodeName)) {
                            dataChunkCache.increaseVersion();
                            dataChunkCache.addWaitingNode(waitingNodeName);
                            
                            this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                            this.lastUpdateTime = DateTimeUtils.getTimestamp();
                        }
                    }
                } else {
                    DataChunkCache dataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, 1, waitingNodeName, null);
                    this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                    this.lastUpdateTime = DateTimeUtils.getTimestamp();
                }
            } finally {
                lock.unlock();
            }
        }
    }
    
    public void putDataChunkCacheData(String hash, byte[] data) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(data == null) {
            throw new IllegalArgumentException("data is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkCacheStoreSyncObj) {
            Lock lock = this.dataChunkCacheStore.getKeyLock(hash);
            lock.lock();
            try {
                if(this.dataChunkCacheStore.containsKey(hash)) {
                    DataChunkCache dataChunkCache = (DataChunkCache) this.dataChunkCacheStore.get(hash);
                    if(dataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PENDING) {
                        // overwrite
                        dataChunkCache.increaseVersion();
                        dataChunkCache.setType(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT);
                        dataChunkCache.setData(data);
                        
                        this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                        this.lastUpdateTime = DateTimeUtils.getTimestamp();
                    }
                } else {
                    DataChunkCache dataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT, hash, 1, data);
                    this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                    this.lastUpdateTime = DateTimeUtils.getTimestamp();
                }
            } finally {
                lock.unlock();
            }
        }
    }
    
    public Cluster getRemoteCluster(Cluster remoteCluster) throws IOException {
        if(remoteCluster == null) {
            throw new IllegalArgumentException("remoteCluster is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();
            
            Node remoteNode = this.contactNodeDeterminationAlgorithm.getResponsibleRemoteNode(clusterManager.getLocalCluster(), clusterManager.getLocalNode(), remoteCluster);
            if(remoteNode == null) {
                throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", remoteCluster.getName()));
            }

            return getRemoteCluster(remoteNode);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public Cluster getRemoteCluster(Node remoteClusterNode) throws IOException {
        if(remoteClusterNode == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        AbstractTransportDriver driver = getDriver();
        AbstractTransportClient client = driver.getClient(remoteClusterNode);
        return client.getLocalCluster();
    }
    
    public TransferResult cacheRemoteDataChunk(DataObjectURI uri, String hash) throws IOException {
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
            ClusterManager clusterManager = stargateService.getClusterManager();
            Cluster remoteCluster = clusterManager.getRemoteCluster(uri.getClusterName());
            if(remoteCluster == null) {
                throw new IOException(String.format("remote cluster %s does not exist", uri.getClusterName()));
            }
            
            Recipe recipe = getRecipe(uri);
            Node remoteNode = this.transferLayoutAlgorithm.determineRemoteNode(remoteCluster, recipe, hash);
            if(remoteNode == null) {
                throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", uri.getClusterName()));
            }
            
            // double-check cache
            if(hasDataChunkCacheData(hash)) {
                return new TransferResult(remoteNode.getName(), DateTimeUtils.getTimestamp(), true);
            }
            
            AbstractTransportDriver driver = getDriver();
            AbstractTransportClient client = driver.getClient(remoteNode);
            
            // increase workload
            Cluster localCluster = clusterManager.getLocalCluster();
            Node localNode = clusterManager.getLocalNode();
            
            this.transferLayoutAlgorithm.increaseNodeWorkload(localCluster, localNode);
            this.transferLayoutAlgorithm.increaseNodeWorkload(remoteCluster, remoteNode);

            InputStream dataChunkInputStream = client.getDataChunk(hash);
            if (dataChunkInputStream == null) {
                throw new IOException("dataChunkInputStream is null");
            }
            
            // check if there is unaccessed data
            // if so, read fully and cache
            byte[] buffer = new byte[64*1024];
            ByteArrayOutputStream cacheData = new ByteArrayOutputStream();

            int read = 0;
            while((read = dataChunkInputStream.read(buffer, 0, 64*1024)) > 0) {
                cacheData.write(buffer, 0, read);
            }

            cacheData.close();
            byte[] cacheDataBytes = cacheData.toByteArray();
            dataChunkInputStream.close();
            cacheData.close();

            // decrease workload
            this.transferLayoutAlgorithm.decreaseNodeWorkload(localCluster, localNode);
            this.transferLayoutAlgorithm.decreaseNodeWorkload(remoteCluster, remoteNode);

            // put to the cache
            putDataChunkCacheData(hash, cacheDataBytes);
            
            return new TransferResult(remoteNode.getName(), DateTimeUtils.getTimestamp(), true);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public InputStream getDataChunk(DataObjectURI uri, String hash) throws IOException, IOException, IOException {
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
            ClusterManager clusterManager = stargateService.getClusterManager();
            Cluster remoteCluster = clusterManager.getRemoteCluster(uri.getClusterName());
            if(remoteCluster == null) {
                throw new IOException(String.format("remote cluster %s does not exist", uri.getClusterName()));
            }
            
            Recipe recipe = getRecipe(uri);
            Node remoteNode = this.transferLayoutAlgorithm.determineRemoteNode(remoteCluster, recipe, hash);
            if(remoteNode == null) {
                throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", uri.getClusterName()));
            }
            
            return getDataChunk(remoteNode, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public InputStream getDataChunk(Node remoteNode, String hash) throws IOException {
        if(remoteNode == null) {
            throw new IllegalArgumentException("remoteNode is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        // step 1. check cache
        if(hasDataChunkCache(hash)) {
            DataChunkCache dataChunkCache = getDataChunkCache(hash);
            DataChunkCacheType dataChunkCacheType = dataChunkCache.getType();
            if(dataChunkCacheType == DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT) {
                byte[] data = dataChunkCache.getData();
                ByteArrayInputStream bais = new ByteArrayInputStream(data);
                return bais;
            } else if(dataChunkCacheType == DataChunkCacheType.DATA_CHUNK_CACHE_PENDING) {
                // place holder
                //TODO: Wait until the data transfer is complete
            }
        }
        
        // step 2. check local recipes
        StargateService stargateService = getStargateService();
        
        try {
            RecipeManager recipeManager = stargateService.getRecipeManager();
            DataExportManager dataExportManager = stargateService.getDataExportManager();
            DataSourceManager dataSourceManager = stargateService.getDataSourceManager();
            
            Recipe recipe = recipeManager.getRecipeByHash(hash);
            if(recipe != null) {
                DataObjectMetadata metadata = recipe.getMetadata();
                DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(metadata.getURI().getPath());
                if(dataExportEntry != null) {
                    URI sourceURI = dataExportEntry.getSourceURI();
                    AbstractDataSourceDriver driver = dataSourceManager.getDriver(sourceURI);
                    
                    RecipeChunk chunk = recipe.getChunk(hash);
                    return driver.openFile(sourceURI, chunk.getOffset(), chunk.getLength());
                }
            }
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
        
        // step 3. go remote
        AbstractTransportDriver driver = getDriver();
        AbstractTransportClient client = driver.getClient(remoteNode);
        
        // increase workload
        Cluster localCluster = null;
        Node localNode = null;
        Cluster remoteCluster = null;
        
        // increase workload
        try {
            ClusterManager clusterManager = stargateService.getClusterManager();
            localCluster = clusterManager.getLocalCluster();
            localNode = clusterManager.getLocalNode();
            remoteCluster = clusterManager.getRemoteCluster(remoteNode.getClusterName());
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
        
        this.transferLayoutAlgorithm.increaseNodeWorkload(localCluster, localNode);
        this.transferLayoutAlgorithm.increaseNodeWorkload(remoteCluster, remoteNode);
        
        InputStream dataChunkInputStream = client.getDataChunk(hash);
        if (dataChunkInputStream == null) {
            throw new IOException("dataChunkInputStream is null");
        }
        
        return new CacheableInputStream(this, dataChunkInputStream, hash);
    }
    
    public void reportNodeUnreachable(Node node) throws IOException {
        if(node == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();

            if(clusterManager.isLocalNode(node.getName())) {
                clusterManager.reportLocalNodeUnreachable(node.getName());
            } else {
                String clusterName = node.getClusterName();
                if(clusterName != null) {
                    clusterManager.reportRemoteNodeUnreachable(clusterName, node.getName());
                }
            }
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public TransferAssignment schedulePrefetch(DataObjectURI uri, String hash) throws IOException {
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
            ClusterManager clusterManager = stargateService.getClusterManager();

            Cluster localCluster = clusterManager.getLocalCluster();
            
            Recipe recipe = getRecipe(uri);
            return schedulePrefetch(localCluster, recipe, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public TransferAssignment schedulePrefetch(Cluster localCluster, DataObjectURI uri, String hash) throws IOException {
        if(localCluster == null) {
            throw new IllegalArgumentException("localCluster is null or empty");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        Recipe recipe = getRecipe(uri);
        return schedulePrefetch(localCluster, recipe, hash);
    }
    
    public TransferAssignment schedulePrefetch(Recipe recipe, String hash) throws IOException {
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
            ClusterManager clusterManager = stargateService.getClusterManager();

            Cluster localCluster = clusterManager.getLocalCluster();
            
            return schedulePrefetch(localCluster, recipe, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public TransferAssignment schedulePrefetch(Cluster localCluster, Recipe recipe, String hash) throws IOException {
        if(localCluster == null) {
            throw new IllegalArgumentException("localCluster is null");
        }
        
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitTransferQueueStore();
        safeInitDataChunkCacheStore();
        
        DataObjectMetadata metadata = recipe.getMetadata();
        RecipeChunk chunk = recipe.getChunk(hash);
        
        if(chunk == null) {
            throw new IllegalArgumentException(String.format("cannot find recipe chunk for hash %s", hash));
        }
        
        // put a pending cache
        this.putPendingDataChunkCache(hash);
        
        synchronized(this.transferSyncObj) {
            TransferAssignment existingAssignment = (TransferAssignment) this.transferInQueueStore.get(hash);
            if(existingAssignment != null) {
                TransferEventType eventType = existingAssignment.getEventType();
                if(eventType == TransferEventType.TRANSFER_EVENT_TYPE_ONDEMAND || eventType == TransferEventType.TRANSFER_EVENT_TYPE_PREFETCH) {
                    // exist already
                    return existingAssignment;
                }
            }

            long timestamp = DateTimeUtils.getTimestamp();
            long order = this.prefetchTransferQueue.size();

            // determine where to copy 
            Node determinedLocalNode = this.transferLayoutAlgorithm.determineLocalNode(localCluster, recipe, hash);

            TransferEvent event = new TransferEvent(TransferEventType.TRANSFER_EVENT_TYPE_PREFETCH, metadata.getURI(), hash, determinedLocalNode.getName());
            this.prefetchTransferQueue.enqueue(event);

            TransferAssignment assignment = new TransferAssignment(TransferEventType.TRANSFER_EVENT_TYPE_PREFETCH, event, timestamp, order);
            this.transferInQueueStore.put(hash, assignment);

            this.lastUpdateTime = DateTimeUtils.getTimestamp();
            return assignment;
        }
    }
    
    public Directory getDirectory(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteDirectoryCacheStore();
        
        synchronized(this.remoteDirectoryCacheStoreSyncObj)  {
            Directory cachedDirectory = (Directory) this.remoteDirectoryCacheStore.get(uri.toUri().toASCIIString());
            if(cachedDirectory == null) {
                String clusterName = uri.getClusterName();
                try {
                    StargateService stargateService = getStargateService();
                    ClusterManager clusterManager = stargateService.getClusterManager();
                    Cluster remoteCluster = clusterManager.getRemoteCluster(clusterName);
                    if(remoteCluster == null) {
                        throw new IOException(String.format("remote cluster %s does not exist", clusterName));
                    }

                    Node remoteNode = this.contactNodeDeterminationAlgorithm.getResponsibleRemoteNode(clusterManager.getLocalCluster(), clusterManager.getLocalNode(), remoteCluster);
                    if(remoteNode == null) {
                        throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", clusterName));
                    }

                    AbstractTransportDriver driver = getDriver();
                    AbstractTransportClient client = driver.getClient(remoteNode);
                    Directory directory = client.getDirectory(uri);

                    if(directory != null) {
                        this.remoteDirectoryCacheStore.put(uri.toUri().toASCIIString(), directory);
                    }
                    cachedDirectory = directory;
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }
            return cachedDirectory;
        }
    }
    
    public Directory getDirectory(Node remoteNode, DataObjectURI uri) throws IOException {
        if(remoteNode == null) {
            throw new IllegalArgumentException("remoteNode is null");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteDirectoryCacheStore();
        
        synchronized(this.remoteDirectoryCacheStoreSyncObj)  {
            Directory cachedDirectory = (Directory) this.remoteDirectoryCacheStore.get(uri.toUri().toASCIIString());
            if(cachedDirectory == null) {
                AbstractTransportDriver driver = getDriver();
                AbstractTransportClient client = driver.getClient(remoteNode);
                Directory directory = client.getDirectory(uri);

                if(directory != null) {
                    this.remoteDirectoryCacheStore.put(uri.toUri().toASCIIString(), directory);
                }
                cachedDirectory = directory;
            }
            return cachedDirectory;
        }
    }

    public Collection<DataObjectMetadata> listDataObjectMetadata(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        String clusterName = uri.getClusterName();
        
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Cluster remoteCluster = clusterManager.getRemoteCluster(clusterName);
            if(remoteCluster == null) {
                throw new IOException(String.format("remote cluster %s does not exist", clusterName));
            }
            
            Node remoteNode = this.contactNodeDeterminationAlgorithm.getResponsibleRemoteNode(clusterManager.getLocalCluster(), clusterManager.getLocalNode(), remoteCluster);
            if(remoteNode == null) {
                throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", clusterName));
            }
            
            return listDataObjectMetadata(remoteNode, uri);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public Collection<DataObjectMetadata> listDataObjectMetadata(Node remoteNode, DataObjectURI uri) throws IOException {
        if(remoteNode == null) {
            throw new IllegalArgumentException("remoteNode is null");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        AbstractTransportDriver driver = getDriver();
        AbstractTransportClient client = driver.getClient(remoteNode);
        return client.listDataObjectMetadata(uri);
    }
    
    public Recipe getRecipe(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteRecipeCacheStore();
        
        synchronized(this.remoteRecipeCacheStoreSyncObj) {
            Recipe cachedRecipe = (Recipe) this.remoteRecipeCacheStore.get(uri.toUri().toASCIIString());
            if(cachedRecipe == null) {
                String clusterName = uri.getClusterName();

                try {
                    StargateService stargateService = getStargateService();
                    ClusterManager clusterManager = stargateService.getClusterManager();
                    Cluster remoteCluster = clusterManager.getRemoteCluster(clusterName);
                    if(remoteCluster == null) {
                        throw new IOException(String.format("remote cluster %s does not exist", clusterName));
                    }

                    Node remoteNode = this.contactNodeDeterminationAlgorithm.getResponsibleRemoteNode(clusterManager.getLocalCluster(), clusterManager.getLocalNode(), remoteCluster);
                    if(remoteNode == null) {
                        throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", clusterName));
                    }

                    AbstractTransportDriver driver = getDriver();
                    AbstractTransportClient client = driver.getClient(remoteNode);
                    Recipe recipe = client.getRecipe(uri);

                    if(recipe != null) {
                        this.remoteRecipeCacheStore.put(uri.toUri().toASCIIString(), recipe);
                    }
                    cachedRecipe = recipe;
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }
            return cachedRecipe;
        }
    }
    
    public Recipe getRecipe(Node remoteNode, DataObjectURI uri) throws IOException {
        if(remoteNode == null) {
            throw new IllegalArgumentException("remoteNode is null");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteRecipeCacheStore();
        
        synchronized(this.remoteRecipeCacheStoreSyncObj) {
            Recipe cachedRecipe = (Recipe) this.remoteRecipeCacheStore.get(uri.toUri().toASCIIString());
            if(cachedRecipe == null) {
                AbstractTransportDriver driver = getDriver();
                AbstractTransportClient client = driver.getClient(remoteNode);

                Recipe recipe = client.getRecipe(uri);

                if(recipe != null) {
                    this.remoteRecipeCacheStore.put(uri.toUri().toASCIIString(), recipe);
                }
                cachedRecipe = recipe;
            }
            return cachedRecipe;
        }
    }
}
