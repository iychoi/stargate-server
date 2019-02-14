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

import stargate.commons.transport.TransferAssignment;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.event.AbstractEventHandler;
import stargate.commons.event.StargateEvent;
import stargate.commons.event.StargateEventType;
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
import stargate.managers.event.EventManager;
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
    private final Object remoteDirectorySyncObj = new Object();
    private AbstractKeyValueStore remoteRecipeCacheStore; // <DataObjectURI, Recipe>
    private final Object remoteRecipeSyncObj = new Object();
    
    private AbstractKeyValueStore dataChunkCacheStore; // <String, byte[]> key = hashstring
    private final Object dataChunkSyncObj = new Object();
    private Map<String, Object> waitObjects = new HashMap<String, Object>();
    
    private ExecutorService prefetchThreadPool = Executors.newFixedThreadPool(5);
    
    private AbstractContactNodeDeterminationAlgorithm contactNodeDeterminationAlgorithm;
    private AbstractTransferLayoutAlgorithm transferLayoutAlgorithm;
    protected long lastUpdateTime;
    
    private static final String REMOTE_DIRECTORY_CACHE_STORE = "remote_dir_cache";
    private static final String REMOTE_RECIPE_CACHE_STORE = "remote_recipe_cache";
    private static final String DATA_CHUNK_CACHE_STORE = "data_chunk_cache";
    
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
            try {
                driver.startServer();
            } catch (DriverNotInitializedException ex) {
                throw new IOException(ex);
            }
        }
        
        setEventHandler();
    }
    
    @Override
    public synchronized void stop() throws IOException {
        this.prefetchThreadPool.shutdownNow();
        
        for(AbstractTransportDriver driver : drivers) {
            try {
                driver.stopServer();
            } catch (DriverNotInitializedException ex) {
                throw new IOException(ex);
            }
        }
        
        super.stop();
    }
    
    private void setEventHandler() throws IOException {
        AbstractEventHandler hander = new AbstractEventHandler() {
            private final StargateEventType[] acceptedEventTypes = {StargateEventType.STARGATE_EVENT_TYPE_TRANSPORT};
                    
            @Override
            public StargateEventType[] getAcceptedTypes() {
                return this.acceptedEventTypes;
            }

            @Override
            public void raised(StargateEvent event) {
                String jsonValue = event.getJsonValue();
                try {
                    TransferEvent evt = TransferEvent.createInstance(jsonValue);
                    processTransferEvent(evt);
                } catch (IOException ex) {
                    LOG.error(ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error(ex);
                }
            }
        };
        
        StargateService stargateService = getStargateService();
        stargateService.addEventHandler(hander);
    }
    
    public TransportServiceInfo getServiceInfo() throws IOException, DriverNotInitializedException {
        AbstractTransportDriver driver = getDriver();
        URI serviceURI = driver.getServiceURI();
        return new TransportServiceInfo(driver.getClass().getName(), serviceURI);
    }
    
    private void safeInitRemoteDirectoryCacheStore() throws IOException {
        synchronized(this.remoteDirectorySyncObj) {
            if(this.remoteDirectoryCacheStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    this.remoteDirectoryCacheStore = keyValueStoreManager.getDriver().getKeyValueStore(REMOTE_DIRECTORY_CACHE_STORE, Directory.class, EnumDataStoreProperty.DATASTORE_PROP_VOLATILE_REPLICATED, TimeUnit.MINUTES, 5);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }
        }
    }
    
    private void safeInitRemoteRecipeCacheStore() throws IOException {
        synchronized(this.remoteRecipeSyncObj) {
            if(this.remoteRecipeCacheStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    this.remoteRecipeCacheStore = keyValueStoreManager.getDriver().getKeyValueStore(REMOTE_RECIPE_CACHE_STORE, Recipe.class, EnumDataStoreProperty.DATASTORE_PROP_VOLATILE_REPLICATED, TimeUnit.DAYS, 1);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }
        }
    }
    
    private void safeInitDataChunkCacheStore() throws IOException {
        synchronized(this.dataChunkSyncObj) {
            if(this.dataChunkCacheStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    this.dataChunkCacheStore = keyValueStoreManager.getDriver().getKeyValueStore(DATA_CHUNK_CACHE_STORE, byte[].class, EnumDataStoreProperty.DATASTORE_PROP_PERSISTENT_DISTRIBUTED);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error(ex);
                    throw new IOException(ex);
                }
            }
        }
    }
    
    private void safeInitLayoutAlgorithm() throws IOException {
        // init algorithms
        safeInitDataChunkCacheStore(); //  chunk cache store must be called before next line is executed
        if(this.transferLayoutAlgorithm == null) {
            StargateService stargateService = getStargateService();
            this.transferLayoutAlgorithm = new StaticTransferLayoutAlgorithm(stargateService, this, this.dataChunkCacheStore);
        }
        
        if(this.contactNodeDeterminationAlgorithm == null) {
            StargateService stargateService = getStargateService();
            this.contactNodeDeterminationAlgorithm = new RoundRobinContactNodeDeterminationAlgorithm(stargateService, this);
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
        
        synchronized(this.dataChunkSyncObj) {
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
        
        synchronized(this.dataChunkSyncObj) {
            return this.dataChunkCacheStore.containsKey(hash);
        }
    }
    
    public boolean hasDataChunkCachePlaceholder(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkSyncObj) {
            if(!this.dataChunkCacheStore.containsKey(hash)) {
                return false;
            }
            
            byte[] bytes = (byte[]) this.dataChunkCacheStore.get(hash);
            if(bytes == null) {
                return false;
            }
            
            DataChunkCache dataChunkCache = DataChunkCache.fromBytes(bytes);
            if(dataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PLACEHOLDER) {
                return true;
            }
            return false;
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
        
        synchronized(this.dataChunkSyncObj) {
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
        
        synchronized(this.dataChunkSyncObj) {
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
        
        // TODO: need to clear this safely
        // there can be many waiting threads for data cache
        synchronized(this.dataChunkSyncObj) {
            this.dataChunkCacheStore.clear();
        
            // notify all and clear
            Collection<Object> values = this.waitObjects.values();
            for(Object syncObj : values) {
                synchronized(syncObj) {
                    syncObj.notifyAll();
                }
            }
            this.waitObjects.clear();
        }
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
    }
    
    public void removeDataChunkCache(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        // TODO: need to clear this safely
        // there can be many waiting threads for data cache
        synchronized(this.dataChunkSyncObj) {
            this.dataChunkCacheStore.remove(hash);
        
            // notify all and clear
            Object syncObj = this.waitObjects.get(hash);
            if(syncObj != null) {
                synchronized(syncObj) {
                    syncObj.notifyAll();
                }
            }
            this.waitObjects.remove(hash);
        }
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
    }
    
    private void putDataChunkCachePlaceholder(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkSyncObj) {
            Lock lock = this.dataChunkCacheStore.getKeyLock(hash);
            lock.lock();
            try {
                if(!this.dataChunkCacheStore.containsKey(hash)) {
                    DataChunkCache dataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PLACEHOLDER, hash, 1, null, null);
                    this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                    this.lastUpdateTime = DateTimeUtils.getTimestamp();
                }
            } finally {
                lock.unlock();
            }
        }
    }
    
    private void putPendingDataChunkCache(String hash, String nodeName) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(nodeName == null || nodeName.isEmpty()) {
            throw new IllegalArgumentException("nodeName is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        synchronized(this.dataChunkSyncObj) {
            Lock lock = this.dataChunkCacheStore.getKeyLock(hash);
            lock.lock();
            try {
                if(this.dataChunkCacheStore.containsKey(hash)) {
                    // there already is
                    DataChunkCache dataChunkCache = (DataChunkCache) this.dataChunkCacheStore.get(hash);
                    if(dataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PLACEHOLDER) {
                        // escalate
                        dataChunkCache.setType(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING);
                        dataChunkCache.setTransferNode(nodeName);
                        dataChunkCache.addWaitingNode(nodeName);
                        dataChunkCache.increaseVersion();
                        this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                        
                        this.lastUpdateTime = DateTimeUtils.getTimestamp();
                    } else if(dataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PENDING) {
                        // escalate
                        dataChunkCache.addWaitingNode(nodeName);
                        dataChunkCache.increaseVersion();
                        this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                        
                        this.lastUpdateTime = DateTimeUtils.getTimestamp();
                    }
                } else {
                    // if there's no existing cache
                    DataChunkCache dataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, 1, nodeName, nodeName, null);
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
        
        synchronized(this.dataChunkSyncObj) {
            Lock lock = this.dataChunkCacheStore.getKeyLock(hash);
            lock.lock();
            try {
                if(this.dataChunkCacheStore.containsKey(hash)) {
                    DataChunkCache dataChunkCache = (DataChunkCache) this.dataChunkCacheStore.get(hash);
                    if(dataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PLACEHOLDER) {
                        // escalate
                        dataChunkCache.setType(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT);
                        dataChunkCache.clearTransferNode();
                        dataChunkCache.clearWaitingNode();
                        dataChunkCache.setData(data);
                        dataChunkCache.increaseVersion();
                        this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                        
                        this.lastUpdateTime = DateTimeUtils.getTimestamp();
                    } else if(dataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PENDING) {
                        // escalate
                        dataChunkCache.setType(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT);
                        dataChunkCache.clearTransferNode();
                        dataChunkCache.clearWaitingNode();
                        dataChunkCache.setData(data);
                        dataChunkCache.increaseVersion();
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
    
    public Cluster getRemoteCluster(Cluster remoteCluster) throws IOException, DriverNotInitializedException {
        if(remoteCluster == null) {
            throw new IllegalArgumentException("remoteCluster is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLayoutAlgorithm();
        
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
    
    public Cluster getRemoteCluster(Node remoteClusterNode) throws IOException, DriverNotInitializedException {
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
    
    public void cacheRemoteDataChunk(DataObjectURI uri, String hash) throws IOException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLayoutAlgorithm();
        
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
                return;
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

            // decrease workload
            this.transferLayoutAlgorithm.decreaseNodeWorkload(localCluster, localNode);
            this.transferLayoutAlgorithm.decreaseNodeWorkload(remoteCluster, remoteNode);

            // put to the cache
            putDataChunkCacheData(hash, cacheDataBytes);
            //notify
            raiseEventForTransferCompletion(uri, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public InputStream getDataChunk(DataObjectURI uri, String hash) throws IOException, IOException, IOException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLayoutAlgorithm();
        
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
            
            return getDataChunk(remoteNode, uri, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    private InputStream getDataChunk(Node remoteNode, DataObjectURI uri, String hash) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(remoteNode == null) {
            throw new IllegalArgumentException("remoteNode is null");
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
        
        safeInitLayoutAlgorithm();
        
        // step 1. check cache
        if(hasDataChunkCache(hash)) {
            DataChunkCache dataChunkCache = getDataChunkCache(hash);
            DataChunkCacheType dataChunkCacheType = dataChunkCache.getType();
            if(dataChunkCacheType == DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT) {
                byte[] data = dataChunkCache.getData();
                ByteArrayInputStream bais = new ByteArrayInputStream(data);
                return bais;
            } else if(dataChunkCacheType == DataChunkCacheType.DATA_CHUNK_CACHE_PLACEHOLDER || 
                    dataChunkCacheType == DataChunkCacheType.DATA_CHUNK_CACHE_PENDING) {
                //Wait until the data transfer is complete
                this.waitObjects.putIfAbsent(hash, new Object());
                Object syncObj = this.waitObjects.get(hash);
                synchronized(syncObj) {
                    try {
                        syncObj.wait();
                    } catch (InterruptedException ex) {
                        LOG.error(ex);
                    }
                }

                // re-check
                DataChunkCache updatedDataChunkCache = getDataChunkCache(hash);
                if(updatedDataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT) {
                    byte[] data = dataChunkCache.getData();
                    ByteArrayInputStream bais = new ByteArrayInputStream(data);
                    return bais;
                } else {
                    // something caused the waiting thread to wake up
                    throw new IOException(String.format("Something caused the thread waiting for the data %s to wake up", hash));
                }
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

        // add to pending chunk cache
        putPendingDataChunkCache(hash, localNode.getName());

        InputStream dataChunkInputStream = client.getDataChunk(hash);
        if (dataChunkInputStream == null) {
            throw new IOException("dataChunkInputStream is null");
        }

        // fully download the chunk and cache and return
        byte[] buffer = new byte[64*1024];
        ByteArrayOutputStream cacheData = new ByteArrayOutputStream();

        int read = 0;
        while((read = dataChunkInputStream.read(buffer, 0, 64*1024)) > 0) {
            cacheData.write(buffer, 0, read);
        }

        cacheData.close();

        byte[] cacheDataBytes = cacheData.toByteArray();
        dataChunkInputStream.close();

        // cache data
        putDataChunkCacheData(hash, cacheDataBytes);
        //notify
        raiseEventForTransferCompletion(uri, hash);
        return new ByteArrayInputStream(cacheDataBytes);
    }
    
    public void reportNodeUnreachable(Node node) throws IOException, DriverNotInitializedException {
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
    
    public TransferAssignment schedulePrefetch(DataObjectURI uri, String hash) throws IOException, DriverNotInitializedException {
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
    
    public TransferAssignment schedulePrefetch(Cluster localCluster, DataObjectURI uri, String hash) throws IOException, DriverNotInitializedException {
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
    
    public TransferAssignment schedulePrefetch(Recipe recipe, String hash) throws IOException, DriverNotInitializedException {
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
    
    public TransferAssignment schedulePrefetch(Cluster localCluster, Recipe recipe, String hash) throws IOException, DriverNotInitializedException {
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
        
        safeInitDataChunkCacheStore();
        safeInitLayoutAlgorithm();
        
        DataObjectMetadata metadata = recipe.getMetadata();
        RecipeChunk chunk = recipe.getChunk(hash);
        
        if(chunk == null) {
            throw new IllegalArgumentException(String.format("cannot find recipe chunk for hash %s", hash));
        }
        
        // check local recipes
        if(!this.hasDataChunkCache(hash)) {
            StargateService stargateService = getStargateService();

            try {
                RecipeManager recipeManager = stargateService.getRecipeManager();
                DataExportManager dataExportManager = stargateService.getDataExportManager();
                DataSourceManager dataSourceManager = stargateService.getDataSourceManager();

                Recipe localRecipe = recipeManager.getRecipeByHash(hash);
                if(localRecipe != null) {
                    DataObjectMetadata localMetadata = localRecipe.getMetadata();
                    DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(localMetadata.getURI().getPath());
                    if(dataExportEntry != null) {
                        URI sourceURI = dataExportEntry.getSourceURI();
                        AbstractDataSourceDriver driver = dataSourceManager.getDriver(sourceURI);

                        RecipeChunk localChunk = localRecipe.getChunk(hash);
                        Collection<Integer> nodeIDs = localChunk.getNodeIDs();
                        Collection<String> nodeNames = localRecipe.getNodeNames(nodeIDs);
                        for(String nodeName : nodeNames) {
                            TransferAssignment assignment = new TransferAssignment(recipe.getMetadata().getURI(), hash, nodeName);
                            return assignment;
                        }
                    }
                }
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }

        // check cache and go remote
        synchronized(this.dataChunkSyncObj) {
            // put a placeholder
            putDataChunkCachePlaceholder(hash);
            
            // if the request already exists
            DataChunkCache dataChunkCache = getDataChunkCache(hash);
            if(dataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PENDING) {
                TransferAssignment assignment = new TransferAssignment(recipe.getMetadata().getURI(), hash, dataChunkCache.getTransferNode());
                return assignment;
            } else if(dataChunkCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT) {
                String nodeName = this.dataChunkCacheStore.getNodeForData(hash);
                TransferAssignment assignment = new TransferAssignment(recipe.getMetadata().getURI(), hash, nodeName);
                return assignment;
            }
                    
            // determine where to copy 
            Node determinedLocalNode = this.transferLayoutAlgorithm.determineLocalNode(localCluster, recipe, hash);

            // put to the pending chunk cache
            putPendingDataChunkCache(hash, determinedLocalNode.getName());
            
            // send to remote
            raiseEventForPrefetchTransfer(metadata.getURI(), hash, determinedLocalNode.getName());
            
            TransferAssignment assignment = new TransferAssignment(recipe.getMetadata().getURI(), hash, determinedLocalNode.getName());
            
            this.lastUpdateTime = DateTimeUtils.getTimestamp();
            return assignment;
        }
    }
    
    public Directory getDirectory(DataObjectURI uri) throws IOException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLayoutAlgorithm();
        
        try {
            String clusterName = uri.getClusterName();
            
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

            return getDirectory(remoteNode, uri);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public Directory getDirectory(Node remoteNode, DataObjectURI uri) throws IOException, DriverNotInitializedException {
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
        
        synchronized(this.remoteDirectorySyncObj)  {
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

    public Collection<DataObjectMetadata> listDataObjectMetadata(DataObjectURI uri) throws IOException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLayoutAlgorithm();
        
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
    
    public Collection<DataObjectMetadata> listDataObjectMetadata(Node remoteNode, DataObjectURI uri) throws IOException, DriverNotInitializedException {
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
    
    public Recipe getRecipe(DataObjectURI uri) throws IOException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLayoutAlgorithm();
        
        try {
            String clusterName = uri.getClusterName();
            
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

            return getRecipe(remoteNode, uri);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public Recipe getRecipe(Node remoteNode, DataObjectURI uri) throws IOException, DriverNotInitializedException {
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
        
        synchronized(this.remoteRecipeSyncObj) {
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
    
    private void raiseEventForPrefetchTransfer(DataObjectURI uri, String hash, String nodeName) throws IOException, DriverNotInitializedException {
        TransferEvent transferEvent = new TransferEvent(TransferEventType.TRANSFER_EVENT_TYPE_PREFETCH, uri, hash);
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Node localNode = clusterManager.getLocalNode();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_TRANSPORT, nodeName, localNode.getName(), transferEvent.toJson());
            eventManager.raiseEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
        }
    }
    
    private void raiseEventForTransferCompletion(DataObjectURI uri, String hash) throws IOException, DriverNotInitializedException {
        TransferEvent transferEvent = new TransferEvent(TransferEventType.TRANSFER_EVENT_TYPE_COMPLETE, uri, hash);
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Cluster localCluster = clusterManager.getLocalCluster();
            Collection<String> nodeNames = localCluster.getNodeNames();
            Node localNode = clusterManager.getLocalNode();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_TRANSPORT, nodeNames, localNode.getName(), transferEvent.toJson());
            eventManager.raiseEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
        }
    }
    
    private void processPrefetchEvent(DataObjectURI uri, String hash) {
        LOG.debug(String.format("Scheduling prefetching for %s - %s", uri.toUri().toASCIIString(), hash));
        PrefetchTask task = new PrefetchTask(this, uri, hash);
        this.prefetchThreadPool.execute(task);
    }
    
    private void processTransferEvent(TransferEvent event) throws IOException, DriverNotInitializedException {
        switch(event.getEventType()) {
            case TRANSFER_EVENT_TYPE_ONDEMAND:
                LOG.debug(String.format("on-demand transfser is requested : %s - %s", event.getURI().toUri().toASCIIString(), event.getHash()));
                break;
            case TRANSFER_EVENT_TYPE_PREFETCH:
                LOG.debug(String.format("prefetch is requested : %s - %s", event.getURI().toUri().toASCIIString(), event.getHash()));
                processPrefetchEvent(event.getURI(), event.getHash());
                break;
            case TRANSFER_EVENT_TYPE_COMPLETE:
                LOG.debug(String.format("transfser is finished : %s - %s", event.getURI().toUri().toASCIIString(), event.getHash()));
                
                Object syncObj = this.waitObjects.get(event.getHash());
                if(syncObj != null) {
                    synchronized(syncObj) {
                        syncObj.notifyAll();
                    }
                }
                break;
            default:
                LOG.error(String.format("cannot handle %s", event.getEventType().name()));
                break;
        }
    }
    
    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
}
