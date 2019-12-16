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

import stargate.commons.utils.Reference;
import stargate.managers.transport.layout.AbstractContactNodeSelectionAlgorithm;
import stargate.managers.transport.layout.AbstractTransferLayoutAlgorithm;
import stargate.managers.transport.layout.RoundRobinContactNodeSelectionAlgorithm;
import stargate.managers.transport.layout.StaticTransferLayoutAlgorithm;
import stargate.managers.transport.layout.TransferLayoutAlgorithms;
import stargate.commons.transport.TransferAssignment;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.dataobject.Directory;
import stargate.commons.datasource.AbstractDataSourceDriver;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.datastore.AbstractBigKeyValueStore;
import stargate.commons.datastore.AbstractDataStoreDriver;
import stargate.commons.driver.AbstractDriver;
import stargate.commons.driver.DriverFailedToLoadException;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.BigKeyValueStoreMetadata;
import stargate.commons.datastore.DataStoreProperties;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.event.AbstractEventHandler;
import stargate.commons.event.StargateEvent;
import stargate.commons.event.StargateEventType;
import stargate.commons.io.AbstractSeekableInputStream;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.transport.AbstractTransportClient;
import stargate.commons.transport.AbstractTransportDriver;
import stargate.commons.transport.TransportServiceInfo;
import stargate.commons.userinterface.DataChunkSource;
import stargate.commons.userinterface.DataChunkStatus;
import stargate.commons.utils.DateTimeUtils;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.dataexport.DataExportManager;
import stargate.managers.datasource.DataSourceManager;
import stargate.managers.datastore.DataStoreManager;
import stargate.managers.event.EventManager;
import stargate.managers.recipe.RecipeManager;
import stargate.managers.statistics.StatisticsManager;
import stargate.managers.transport.layout.ContactNodeSelectionAlgorithms;
import stargate.managers.transport.layout.FairTransferLayoutAlgorithm;
import stargate.managers.transport.layout.RandomContactNodeSelectionAlgorithm;
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
    private AbstractKeyValueStore localizedRemoteRecipeCacheStore; // <DataObjectURI, Recipe>
    private final Object localizedRemoteRecipeSyncObj = new Object();
    
    private AbstractBigKeyValueStore dataChunkCacheStore; // <String, byte[]> key = hashstring
    private final Object dataChunkSyncObj = new Object();
    private Map<String, Reference> waitObjects = new ConcurrentHashMap<String, Reference>();
    
    private AbstractContactNodeSelectionAlgorithm contactNodeSelectionAlgorithm;
    private AbstractTransferLayoutAlgorithm transferLayoutAlgorithm;
    
    private TransferScheduler transferScheduler;
    private ExecutorService pendingPrefetchThreadPool;
    
    protected long lastUpdateTime;
    
    private static final String REMOTE_DIRECTORY_CACHE_STORE = "remote_dir_cache";
    private static final String REMOTE_RECIPE_CACHE_STORE = "remote_recipe_cache";
    private static final String LOCALIZED_REMOTE_RECIPE_CACHE_STORE = "localized_remote_recipe_cache";
    private static final String DATA_CHUNK_CACHE_STORE = "data_chunk_cache";
    
    public static TransportManager getInstance(StargateService service, TransportManagerConfig config, Collection<AbstractTransportDriver> drivers) throws ManagerNotInstantiatedException {
        synchronized (TransportManager.class) {
            if(instance == null) {
                instance = new TransportManager(service, config, drivers);
            }
            return instance;
        }
    }
    
    public static TransportManager getInstance(StargateService service, TransportManagerConfig config) throws ManagerNotInstantiatedException {
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
                    instance = new TransportManager(service, config, transportDrivers);
                } catch (DriverFailedToLoadException ex) {
                    LOG.error("Could not load driver", ex);
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
    
    TransportManager(StargateService service, TransportManagerConfig config, Collection<AbstractTransportDriver> drivers) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(drivers == null || drivers.isEmpty()) {
            throw new IllegalArgumentException("drivers is null or empty");
        }
        
        this.setService(service);
        this.setConfig(config);
        
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
        
        TransportManagerConfig managerConfig = (TransportManagerConfig) this.config;
        
        this.transferScheduler = new TransferScheduler(managerConfig.getTransferThreads(), managerConfig.getPendingPrefetchTimeoutSec());
        this.transferScheduler.start();
        this.pendingPrefetchThreadPool = Executors.newFixedThreadPool(managerConfig.getPrefetchSchedulerThreads());
        
        for(AbstractTransportDriver driver : drivers) {
            try {
                driver.startServer();
            } catch (DriverNotInitializedException ex) {
                throw new IOException(ex);
            }
        }
        
        setEventHandler();
        
        this.getService().addPostStartTask(new Runnable() {
            @Override
            public void run() {
                try {
                    postStart();
                } catch (IOException ex) {
                    LOG.error("Cannot execute postStart", ex);
                }
            }
        });
    }
    
    private synchronized void postStart() throws IOException {
        // init this when it is started
        safeInitRemoteDirectoryCacheStore();
        safeInitRemoteRecipeCacheStore();
        safeInitLocalizedRemoteRecipeCacheStore();
        safeInitDataChunkCacheStore();
        safeInitLayoutAlgorithm();
    }
    
    @Override
    public synchronized void stop() throws IOException {
        this.transferScheduler.stop();
        this.transferScheduler = null;
        this.pendingPrefetchThreadPool.shutdownNow();
        this.pendingPrefetchThreadPool = null;
        
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
                    LOG.error("IOException", ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error("Driver is not initialized", ex);
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
                    
                    TransportManagerConfig managerConfig = (TransportManagerConfig) this.config;
                    
                    DataStoreProperties properties = new DataStoreProperties();
                    properties.setReplicated(true);
                    properties.setPersistent(false);
                    properties.setExpirable(true);
                    properties.setExpireTimeUnit(TimeUnit.SECONDS);
                    properties.setExpireTimeVal(managerConfig.getDirectoryCacheTimeoutSec());
                    this.remoteDirectoryCacheStore = keyValueStoreManager.getDriver().getKeyValueStore(REMOTE_DIRECTORY_CACHE_STORE, Directory.class, properties);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error("Manager is not instantiated", ex);
                    throw new IOException(ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error("Driver is not initialized", ex);
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
                    
                    TransportManagerConfig managerConfig = (TransportManagerConfig) this.config;
                    
                    DataStoreProperties properties = new DataStoreProperties();
                    properties.setReplicated(true);
                    properties.setPersistent(false);
                    properties.setExpirable(true);
                    properties.setExpireTimeUnit(TimeUnit.SECONDS);
                    properties.setExpireTimeVal(managerConfig.getRecipeCacheTimeoutSec());
                    this.remoteRecipeCacheStore = keyValueStoreManager.getDriver().getKeyValueStore(REMOTE_RECIPE_CACHE_STORE, Recipe.class, properties);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error("Manager is not instantiated", ex);
                    throw new IOException(ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error("Driver is not initialized", ex);
                    throw new IOException(ex);
                }
            }
        }
    }
    
    private void safeInitLocalizedRemoteRecipeCacheStore() throws IOException {
        synchronized(this.localizedRemoteRecipeSyncObj) {
            if(this.localizedRemoteRecipeCacheStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    
                    TransportManagerConfig managerConfig = (TransportManagerConfig) this.config;
                    
                    DataStoreProperties properties = new DataStoreProperties();
                    properties.setReplicated(true);
                    properties.setPersistent(false);
                    properties.setExpirable(true);
                    properties.setExpireTimeUnit(TimeUnit.SECONDS);
                    properties.setExpireTimeVal(managerConfig.getRecipeCacheTimeoutSec());
                    this.localizedRemoteRecipeCacheStore = keyValueStoreManager.getDriver().getKeyValueStore(LOCALIZED_REMOTE_RECIPE_CACHE_STORE, Recipe.class, properties);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error("Manager is not instantiated", ex);
                    throw new IOException(ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error("Driver is not initialized", ex);
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
                    
                    TransportManagerConfig managerConfig = (TransportManagerConfig) this.config;
                    
                    DataStoreProperties properties = new DataStoreProperties();
                    properties.setSharded(true);
                    properties.setReplicaNum(0);
                    properties.setPersistent(true);
                    
                    if(managerConfig.getDataChunkCacheTimeoutSec() > 0) {
                        properties.setExpirable(true);
                        properties.setExpireTimeUnit(TimeUnit.SECONDS);
                        properties.setExpireTimeVal(managerConfig.getDataChunkCacheTimeoutSec());
                    } else {
                        properties.setExpirable(false);
                    }
                    
                    this.dataChunkCacheStore = keyValueStoreManager.getDriver().getBigKeyValueStore(DATA_CHUNK_CACHE_STORE, properties);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error("Manager is not instantiated", ex);
                    throw new IOException(ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error("Driver is not initialized", ex);
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
            TransportManagerConfig managerConfig = (TransportManagerConfig) this.config;
            TransferLayoutAlgorithms transferAlg = managerConfig.getLayoutAlgorithm();
            if(transferAlg == null) {
                transferAlg = TransferLayoutAlgorithms.TRANSFER_LAYOUT_ALGORITHM_STATIC;
            }
            
            LOG.info(String.format("safeInitLayoutAlgorithm: Using transfer layout algorithm - %s", transferAlg.name()));
            
            switch(transferAlg) {
                case TRANSFER_LAYOUT_ALGORITHM_STATIC:
                    this.transferLayoutAlgorithm = new StaticTransferLayoutAlgorithm(stargateService, this, this.dataChunkCacheStore);
                    break;
                case TRANSFER_LAYOUT_ALGORITHM_FAIR:
                    this.transferLayoutAlgorithm = new FairTransferLayoutAlgorithm(stargateService, this, this.dataChunkCacheStore);
                    break;
                default:
                    throw new IOException(String.format("Cannot find transfer algorithm %s", transferAlg.name()));
            }
        }
        
        if(this.contactNodeSelectionAlgorithm == null) {
            StargateService stargateService = getStargateService();
            TransportManagerConfig managerConfig = (TransportManagerConfig) this.config;
            ContactNodeSelectionAlgorithms selectionAlg = managerConfig.getContactNodeSelectionAlgorithm();
            if(selectionAlg == null) {
                selectionAlg = ContactNodeSelectionAlgorithms.CONTACT_NODE_SELECTION_ALGORITHM_ROUNDROBIN;
            }
            
            LOG.info(String.format("safeInitLayoutAlgorithm: Using contact node selection algorithm - %s", selectionAlg.name()));
            
            switch(selectionAlg) {
                case CONTACT_NODE_SELECTION_ALGORITHM_ROUNDROBIN:
                    this.contactNodeSelectionAlgorithm = new RoundRobinContactNodeSelectionAlgorithm(stargateService, this);
                    break;
                case CONTACT_NODE_SELECTION_ALGORITHM_RANDOM:
                    this.contactNodeSelectionAlgorithm = new RandomContactNodeSelectionAlgorithm(stargateService, this);
                    break;
                default:
                    throw new IOException(String.format("Cannot find transfer algorithm %s", selectionAlg.name()));
            }
        }
    }
    
    public int getDataChunkPartSize() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitDataChunkCacheStore();
        
        return this.dataChunkCacheStore.getPartSize();
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
            
            Node remoteNode = this.contactNodeSelectionAlgorithm.getResponsibleRemoteNode(clusterManager.getLocalCluster(), clusterManager.getLocalNode(), remoteCluster);
            if(remoteNode == null) {
                throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", remoteCluster.getName()));
            }

            return getRemoteCluster(remoteNode);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
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
        
        Recipe recipe = getRecipe(uri);
        RecipeChunk recipeChunk = recipe.getChunk(hash);
        
        LOG.debug(String.format("cacheRemoteDataChunk: Caching a remote data chunk - %s, %s", uri.toUri().toASCIIString(), hash));
        
        try {
            StargateService service = getStargateService();
            
            ClusterManager clusterManager = service.getClusterManager();
            Node localNode = clusterManager.getLocalNode();
            
            LOG.debug(String.format("cacheRemoteDataChunk: Checking and putting a pending request for an on-demand transfer for - %s, %s", uri.toUri().toASCIIString(), hash));
            
            // put to the cache
            DataChunkCacheMetadata pendingDataChunkCacheMetadata = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, recipeChunk.getLength(), 1, localNode.getName());
            boolean insert = this.dataChunkCacheStore.putIfAbsent(hash, null, recipeChunk.getLength(), pendingDataChunkCacheMetadata.toBytes());
            if(insert) {
                LOG.debug(String.format("cacheRemoteDataChunk: Put a pending request for a on-demand transfer for - %s, %s", uri.toUri().toASCIIString(), hash));
                this.lastUpdateTime = DateTimeUtils.getTimestamp();
            } else {
                // read it
                BigKeyValueStoreMetadata existingMetadata = this.dataChunkCacheStore.getMetadata(hash);
                DataChunkCacheMetadata existingDataChunkCacheMetadata = DataChunkCacheMetadata.fromBytes(existingMetadata.getExtra());
            
                switch (existingDataChunkCacheMetadata.getType()) {
                    case DATA_CHUNK_CACHE_PRESENT:
                        // existing
                        LOG.debug(String.format("cacheRemoteDataChunk: Found a chunk cache for - %s, %s", uri.toUri().toASCIIString(), hash));
                        return;
                    case DATA_CHUNK_CACHE_PENDING:
                        String transferNode = existingDataChunkCacheMetadata.getTransferNode();
                        if(!localNode.getName().equals(transferNode)) {
                            // it's not my task
                            LOG.debug(String.format("cacheRemoteDataChunk: Transfer schedule is found but pending (not the task of this node) for %s, %s", uri.toUri().toASCIIString(), hash));
                            return;
                        }
                        break;
                    default:
                        throw new IOException("Unknown data chunk cache type");
                }
            }
            
            this.waitObjects.putIfAbsent(hash, new Reference());
            Reference reference = this.waitObjects.get(hash);
            reference.increaseReference();
            
            // get from remote
            Cluster remoteCluster = clusterManager.getRemoteCluster(uri.getClusterName());
            if(remoteCluster == null) {
                reference.decreaseReference();
                if(reference.getReferenceCount() <= 0) {
                    this.waitObjects.remove(hash);
                }
                throw new IOException(String.format("remote cluster %s does not exist", uri.getClusterName()));
            }

            Node remoteNode = this.transferLayoutAlgorithm.determineRemoteNode(remoteCluster, recipe, hash);
            if(remoteNode == null) {
                reference.decreaseReference();
                if(reference.getReferenceCount() <= 0) {
                    this.waitObjects.remove(hash);
                }
                throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", uri.getClusterName()));
            }

            // go remote
            AbstractTransportDriver driver = getDriver();
            AbstractTransportClient client = driver.getClient(remoteNode);

            // increase workload
            //Cluster localCluster = clusterManager.getLocalCluster();

            //this.transferLayoutAlgorithm.increaseNodeWorkload(localCluster, localNode);
            this.transferLayoutAlgorithm.increaseNodeWorkload(remoteCluster, remoteNode);

            StatisticsManager statisticsManager = service.getStatisticsManager();
            statisticsManager.addDataChunkTransferReceiveStartStatistics(uri.toUri().toASCIIString(), hash);

            LOG.info(String.format("Download a data chunk %s", hash));
            InputStream dataChunkInputStream = client.getDataChunk(hash);
            
            statisticsManager.addDataChunkTransferReceiveEndStatistics(uri.toUri().toASCIIString(), hash);

            // decrease workload
            //this.transferLayoutAlgorithm.decreaseNodeWorkload(localCluster, localNode);
            this.transferLayoutAlgorithm.decreaseNodeWorkload(remoteCluster, remoteNode);

            // put to the cache
            DataChunkCacheMetadata dataChunkCacheMetadata = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT, hash, recipeChunk.getLength(), Integer.MAX_VALUE, null);
            LOG.debug(String.format("cacheRemoteDataChunk: Putting a data chunk cache for - %s", hash));
            this.dataChunkCacheStore.put(hash, dataChunkInputStream, recipeChunk.getLength(), dataChunkCacheMetadata.toBytes());
            this.lastUpdateTime = DateTimeUtils.getTimestamp();

            LOG.debug(String.format("cacheRemoteDataChunk: Waking up all threads that are waiting for a data chunk for - %s", hash));
            reference.wakeup();
            reference.decreaseReference();
            if(reference.getReferenceCount() <= 0) {
                this.waitObjects.remove(hash);
            }

            //notify
            raiseEventForTransferCompletion(uri, hash, recipeChunk.getOffset());

            LOG.debug(String.format("cacheRemoteDataChunk: Cached a chunk for - %s, %s at %s", uri.toUri().toASCIIString(), hash, localNode.getName()));
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public String getDataChunkCacheNodeName(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLayoutAlgorithm();
        return this.dataChunkCacheStore.getPrimaryNodeForData(hash);
    }
    
    public File getDataChunkCacheFilePath(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLayoutAlgorithm();
        return this.dataChunkCacheStore.getCacheFilePath(hash);
    }
    
    public DataChunkStatus requestDataChunk(DataObjectURI uri, String hash) throws IOException, IOException, IOException, DriverNotInitializedException {
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
        
        TransportManagerConfig managerConfig = (TransportManagerConfig) this.config;
        
        Recipe recipe = getRecipe(uri);
        RecipeChunk recipeChunk = recipe.getChunk(hash);
        
        LOG.debug(String.format("requestDataChunk: Get a data chunk - %s, %s", uri.toUri().toASCIIString(), hash));
        
        try {
            StargateService service = getStargateService();
            
            ClusterManager clusterManager = service.getClusterManager();
            Node localNode = clusterManager.getLocalNode();
            
            LOG.debug(String.format("requestDataChunk: Checking and putting a pending request for an on-demand transfer for - %s, %s", uri.toUri().toASCIIString(), hash));
            
            BigKeyValueStoreMetadata existingMetadata = this.dataChunkCacheStore.getMetadata(hash);
            DataChunkCacheMetadata existingDataChunkCacheMetadata = null;
            boolean hasExistingPendingRequest = false;
            if(existingMetadata != null) {
                existingDataChunkCacheMetadata = DataChunkCacheMetadata.fromBytes(existingMetadata.getExtra());

                switch (existingDataChunkCacheMetadata.getType()) {
                    case DATA_CHUNK_CACHE_PRESENT:
                        // existing
                        LOG.debug(String.format("requestDataChunk: Found a chunk cache for - %s, %s", uri.toUri().toASCIIString(), hash));
                        String cacheNodeName = this.dataChunkCacheStore.getPrimaryNodeForData(hash);
                        File cacheFilePath = null;
                        if(localNode.getName().equals(cacheNodeName)) {
                            cacheFilePath = this.dataChunkCacheStore.getCacheFilePath(hash);
                        }
                        return new DataChunkStatus(DataChunkSource.DATA_CHUNK_SOURCE_REMOTE_CLUSTER, recipeChunk.getLength(), this.dataChunkCacheStore.getPartSize(), cacheNodeName, cacheFilePath);
                    case DATA_CHUNK_CACHE_PENDING:
                        hasExistingPendingRequest = true;
                        break;
                    default:
                        throw new IOException("Unknown data chunk cache type");
                }
            } else {
                // check local recipes
                RecipeManager recipeManager = service.getRecipeManager();
                
                LOG.debug(String.format("requestDataChunk: Checking local recipes for - %s, %s", uri.toUri().toASCIIString(), hash));
            
                Recipe localRecipe = recipeManager.getRecipeByHash(hash);
                if(localRecipe != null) {
                    LOG.debug(String.format("requestDataChunk: Found a local chunk for - %s, %s", uri.toUri().toASCIIString(), hash));
                    return new DataChunkStatus(DataChunkSource.DATA_CHUNK_SOURCE_REMOTE_CLUSTER, recipeChunk.getLength(), this.dataChunkCacheStore.getPartSize(), null, null);
                }
                
                // put to the cache
                DataChunkCacheMetadata pendingDataChunkCache = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, recipeChunk.getLength(), 1, localNode.getName());
                boolean insert = this.dataChunkCacheStore.putIfAbsent(hash, null, recipeChunk.getLength(), pendingDataChunkCache.toBytes());
                if(insert) {
                    LOG.debug(String.format("requestDataChunk: Put a pending request for a on-demand transfer for - %s, %s", uri.toUri().toASCIIString(), hash));
                    this.lastUpdateTime = DateTimeUtils.getTimestamp();
                    hasExistingPendingRequest = false;
                } else {
                    hasExistingPendingRequest = true;
                    // read it again
                    existingMetadata = this.dataChunkCacheStore.getMetadata(hash);
                    existingDataChunkCacheMetadata = DataChunkCacheMetadata.fromBytes(existingMetadata.getExtra());
                }
            }
            
            if(hasExistingPendingRequest) {
                if(!existingDataChunkCacheMetadata.getWaitingNodes().contains(localNode.getName())) {
                    while(true) {
                        DataChunkCacheMetadata newDataChunkCache = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, existingDataChunkCacheMetadata.getSize(), existingDataChunkCacheMetadata.getVersion() + 1, existingDataChunkCacheMetadata.getTransferNode());
                        newDataChunkCache.addWaitingNodes(existingDataChunkCacheMetadata.getWaitingNodes());
                        newDataChunkCache.addWaitingNode(localNode.getName());

                        BigKeyValueStoreMetadata newMetadata = new BigKeyValueStoreMetadata(existingMetadata.getKey(), existingMetadata.getPartNum(), existingMetadata.getEntrySize(), newDataChunkCache.toBytes());
                        boolean replaced = this.dataChunkCacheStore.replace(hash, existingMetadata, newMetadata);
                        if(replaced) {
                            this.lastUpdateTime = DateTimeUtils.getTimestamp();
                            break;
                        } else {
                            LOG.warn("requestDataChunk: Could not replaced chunk cache entry - try it again");
                            // read it again
                            existingMetadata = this.dataChunkCacheStore.getMetadata(hash);
                            existingDataChunkCacheMetadata = DataChunkCacheMetadata.fromBytes(existingMetadata.getExtra());
                        }
                    }
                }
            }
            
            this.waitObjects.putIfAbsent(hash, new Reference());
            Reference reference = this.waitObjects.get(hash);
            reference.increaseReference();
            
            // increase data transfer priority
            raiseEventForOnDemandTransfer(uri, hash, recipeChunk.getOffset(), existingDataChunkCacheMetadata.getTransferNode());
                            
            if(hasExistingPendingRequest) {
                LOG.debug(String.format("requestDataChunk: Waiting for finishing data transfer for %s", hash));
                try {
                    if(reference.await(managerConfig.getDataTransferTimeoutSec(), TimeUnit.SECONDS)) {
                        // done
                        LOG.debug(String.format("requestDataChunk: Re-checking transferred data for %s", hash));
                        existingMetadata = this.dataChunkCacheStore.getMetadata(hash);
                        existingDataChunkCacheMetadata = DataChunkCacheMetadata.fromBytes(existingMetadata.getExtra());

                        if(existingDataChunkCacheMetadata.getType().equals(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT)) {
                            reference.wakeup();
                            reference.decreaseReference();
                            if(reference.getReferenceCount() <= 0) {
                                this.waitObjects.remove(hash);
                            }

                            LOG.debug(String.format("requestDataChunk: Found a chunk cache for - %s, %s", uri.toUri().toASCIIString(), hash));
                            String cacheNodeName = this.dataChunkCacheStore.getPrimaryNodeForData(hash);
                            File cacheFilePath = null;
                            if(localNode.getName().equals(cacheNodeName)) {
                                cacheFilePath = this.dataChunkCacheStore.getCacheFilePath(hash);
                            }
                            return new DataChunkStatus(DataChunkSource.DATA_CHUNK_SOURCE_REMOTE_CLUSTER, recipeChunk.getLength(), this.dataChunkCacheStore.getPartSize(), cacheNodeName, cacheFilePath);
                        } else {
                            reference.decreaseReference();
                            if(reference.getReferenceCount() <= 0) {
                                this.waitObjects.remove(hash);
                            }
                            throw new IOException("Invalid data chunk cache type");
                        }
                    } else {
                        reference.decreaseReference();
                        if(reference.getReferenceCount() <= 0) {
                            this.waitObjects.remove(hash);
                        }
                        throw new IOException(String.format("Timeout for waiting to finish data transfer for %s", hash));
                    }
                } catch (InterruptedException ex) {
                    LOG.error("InterruptedException", ex);
                    reference.decreaseReference();
                    if(reference.getReferenceCount() <= 0) {
                        this.waitObjects.remove(hash);
                    }
                    throw new IOException(ex);
                }
            } else {
                Cluster remoteCluster = clusterManager.getRemoteCluster(uri.getClusterName());
                if(remoteCluster == null) {
                    reference.decreaseReference();
                    if(reference.getReferenceCount() <= 0) {
                        this.waitObjects.remove(hash);
                    }
                    throw new IOException(String.format("remote cluster %s does not exist", uri.getClusterName()));
                }
                
                Node remoteNode = this.transferLayoutAlgorithm.determineRemoteNode(remoteCluster, recipe, hash);
                if(remoteNode == null) {
                    reference.decreaseReference();
                    if(reference.getReferenceCount() <= 0) {
                        this.waitObjects.remove(hash);
                    }
                    throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", uri.getClusterName()));
                }
                
                // go remote
                AbstractTransportDriver driver = getDriver();
                AbstractTransportClient client = driver.getClient(remoteNode);

                // increase workload
                //Cluster localCluster = clusterManager.getLocalCluster();

                //this.transferLayoutAlgorithm.increaseNodeWorkload(localCluster, localNode);
                this.transferLayoutAlgorithm.increaseNodeWorkload(remoteCluster, remoteNode);

                StatisticsManager statisticsManager = service.getStatisticsManager();
                statisticsManager.addDataChunkTransferReceiveStartStatistics(uri.toUri().toASCIIString(), hash);

                LOG.info(String.format("Download a data chunk %s", hash));
                InputStream dataChunkInputStream = client.getDataChunk(hash);

                statisticsManager.addDataChunkTransferReceiveEndStatistics(uri.toUri().toASCIIString(), hash);

                // decrease workload
                //this.transferLayoutAlgorithm.decreaseNodeWorkload(localCluster, localNode);
                this.transferLayoutAlgorithm.decreaseNodeWorkload(remoteCluster, remoteNode);

                // put to the cache
                DataChunkCacheMetadata dataChunkCache = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT, hash, recipeChunk.getLength(), Integer.MAX_VALUE);
                LOG.debug(String.format("requestDataChunk: Putting a data chunk cache for - %s", hash));
                this.dataChunkCacheStore.put(hash, dataChunkInputStream, recipeChunk.getLength(), dataChunkCache.toBytes());
                this.lastUpdateTime = DateTimeUtils.getTimestamp();

                LOG.debug(String.format("requestDataChunk: Waking up all threads that are waiting for a data chunk for - %s", hash));
                reference.wakeup();
                reference.decreaseReference();
                if(reference.getReferenceCount() <= 0) {
                    this.waitObjects.remove(hash);
                }

                //notify
                raiseEventForTransferCompletion(uri, hash, recipeChunk.getOffset());
                
                String cacheNodeName = this.dataChunkCacheStore.getPrimaryNodeForData(hash);
                File cacheFilePath = null;
                if(localNode.getName().equals(cacheNodeName)) {
                    cacheFilePath = this.dataChunkCacheStore.getCacheFilePath(hash);
                }
                return new DataChunkStatus(DataChunkSource.DATA_CHUNK_SOURCE_REMOTE_CLUSTER, recipeChunk.getLength(), this.dataChunkCacheStore.getPartSize(), cacheNodeName, cacheFilePath);
            }
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
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
        
        LOG.debug(String.format("getDataChunk: Get a data chunk - %s, %s", uri.toUri().toASCIIString(), hash));
        
        try {
            StargateService service = getStargateService();
            
            AbstractSeekableInputStream dataStream = this.dataChunkCacheStore.getData(hash);
            if(dataStream != null) {
                LOG.debug(String.format("getDataChunk: Get a chunk for - %s, %s", uri.toUri().toASCIIString(), hash));
                return dataStream;
            }
            
            // check local recipes
            RecipeManager recipeManager = service.getRecipeManager();

            LOG.debug(String.format("getDataChunk: Checking local recipes for - %s, %s", uri.toUri().toASCIIString(), hash));
            Recipe localRecipe = recipeManager.getRecipeByHash(hash);
            if(localRecipe != null) {
                DataExportManager dataExportManager = service.getDataExportManager();
                DataObjectMetadata metadata = localRecipe.getMetadata();

                DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(metadata.getURI().getPath());
                if(dataExportEntry != null) {
                    DataSourceManager dataSourceManager = service.getDataSourceManager();

                    URI sourceURI = dataExportEntry.getSourceURI();
                    AbstractDataSourceDriver driver = dataSourceManager.getDriver(sourceURI);

                    RecipeChunk localRecipeChunk = localRecipe.getChunk(hash);
                    InputStream inputStream = driver.openFile(sourceURI, localRecipeChunk.getOffset(), localRecipeChunk.getLength());
                    LOG.debug(String.format("getDataChunk: Found a local chunk for - %s, %s", uri.toUri().toASCIIString(), hash));
                    return inputStream;
                } else {
                    throw new IOException(String.format("cannot find a local data export entry - %s", metadata.getURI().getPath()));
                }
            } else {
                throw new IOException(String.format("cannot find a the chunk for - %s, %s", uri.toUri().toASCIIString(), hash));
            }
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public InputStream getDataChunkPart(DataObjectURI uri, String hash, int partNo) throws IOException, IOException, IOException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(partNo < 0) {
            throw new IllegalArgumentException("partNo is negative");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        LOG.debug(String.format("getDataChunkPart: Get a data chunk part - %s, %s, %d", uri.toUri().toASCIIString(), hash, partNo));
        
        try {
            StargateService service = getStargateService();
            
            DataStoreManager dataStoreManager = service.getDataStoreManager();
            AbstractDataStoreDriver dataStoreDriver = dataStoreManager.getDriver();
            int partSize = dataStoreDriver.getPartSize();
            long partOffset = partNo * partSize;
            
            AbstractSeekableInputStream dataPartStream = this.dataChunkCacheStore.getDataPart(hash, partNo);
            if(dataPartStream != null) {
                LOG.debug(String.format("getDataChunkPart: Get a chunk part for - %s, %s, %d", uri.toUri().toASCIIString(), hash, partNo));
                return dataPartStream;
            }
            
            // check local recipes
            RecipeManager recipeManager = service.getRecipeManager();
            
            LOG.debug(String.format("getDataChunkPart: Checking local recipes for - %s, %s", uri.toUri().toASCIIString(), hash));
            Recipe localRecipe = recipeManager.getRecipeByHash(hash);
            if(localRecipe != null) {
                DataExportManager dataExportManager = service.getDataExportManager();
                DataObjectMetadata metadata = localRecipe.getMetadata();

                DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(metadata.getURI().getPath());
                if(dataExportEntry != null) {
                    DataSourceManager dataSourceManager = service.getDataSourceManager();

                    URI sourceURI = dataExportEntry.getSourceURI();
                    AbstractDataSourceDriver driver = dataSourceManager.getDriver(sourceURI);

                    RecipeChunk localRecipeChunk = localRecipe.getChunk(hash);
                    if(partOffset >= localRecipeChunk.getLength()) {
                        throw new IOException("cannot read beyond chunk size");
                    }

                    InputStream inputStream = driver.openFile(sourceURI, localRecipeChunk.getOffset() + partOffset, Math.min(localRecipeChunk.getLength() - partOffset, partSize));
                    LOG.debug(String.format("getDataChunkPart: Found a local chunk for - %s, %s", uri.toUri().toASCIIString(), hash));
                    return inputStream;
                } else {
                    throw new IOException(String.format("cannot find a local data export entry - %s", metadata.getURI().getPath()));
                }
            } else {
                throw new IOException(String.format("cannot find a the chunk part for - %s, %s, %d", uri.toUri().toASCIIString(), hash, partNo));
            }
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public TransferAssignment prefetchDataChunk(DataObjectURI uri, String hash) throws IOException, DriverNotInitializedException {
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
        
        Recipe recipe = getRecipe(uri);
        RecipeChunk recipeChunk = recipe.getChunk(hash);
        DataObjectMetadata metadata = recipe.getMetadata();
        
        LOG.debug(String.format("prefetchDataChunk: Scheduling a prefetching - %s, hash(%s)", metadata.getURI().toUri().toASCIIString(), hash));
        
        if(recipeChunk == null) {
            throw new IllegalArgumentException(String.format("cannot find recipe chunk - hash(%s)", hash));
        }
        
        try {
            StargateService stargateService = getStargateService();
            
            // check cache
            LOG.debug(String.format("prefetchDataChunk: Checking existing cache for - %s, hash(%s)", metadata.getURI().toUri().toASCIIString(), hash));
            
            BigKeyValueStoreMetadata existingMetadata = this.dataChunkCacheStore.getMetadata(hash);
            
            if(existingMetadata != null) {
                DataChunkCacheMetadata existingDataChunkCacheMetadata = DataChunkCacheMetadata.fromBytes(existingMetadata.getExtra());
                
                switch (existingDataChunkCacheMetadata.getType()) {
                    case DATA_CHUNK_CACHE_PENDING:
                    {
                        Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
                        TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), existingDataChunkCacheMetadata.getTransferNode(), primaryAndBackupNodesForData);
                        LOG.debug(String.format("prefetchDataChunk: Found a pending prefetch schedule for - %s, hash(%s) at node(%s)", metadata.getURI().toUri().toASCIIString(), hash, existingDataChunkCacheMetadata.getTransferNode()));
                        return assignment;
                    }
                    case DATA_CHUNK_CACHE_PRESENT:
                    {
                        Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
                        String primaryNodeForData = primaryAndBackupNodesForData.iterator().next();
                        TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), primaryNodeForData, primaryAndBackupNodesForData);
                        LOG.debug(String.format("prefetchDataChunk: Found a local cache for - %s, hash(%s) at node(%s)", metadata.getURI().toUri().toASCIIString(), hash, primaryNodeForData));
                        return assignment;
                    }
                    default:
                        throw new IOException("Unknown data chunk cache type");
                }
            }
            
            // check local recipes
            RecipeManager recipeManager = stargateService.getRecipeManager();
            
            LOG.debug(String.format("prefetchDataChunk: Checking local recipes for a prefetching for - %s, hash(%s)", metadata.getURI().toUri().toASCIIString(), hash));
            
            Recipe localRecipe = recipeManager.getRecipeByHash(hash);
            if(localRecipe != null) {
                DataExportManager dataExportManager = stargateService.getDataExportManager();
                DataObjectMetadata localMetadata = localRecipe.getMetadata();
                
                DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(localMetadata.getURI().getPath());
                if(dataExportEntry != null) {
                    RecipeChunk localChunk = localRecipe.getChunk(hash);
                    Collection<Integer> nodeIDs = localChunk.getNodeIDs();
                    Collection<String> nodeNames = localRecipe.getNodeNames(nodeIDs);
                    
                    String nodeName = nodeNames.iterator().next();
                    TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), nodeName, nodeNames);
                    LOG.debug(String.format("prefetchDataChunk: Found a local recipe (%s) for - %s, hash(%s) at node(%s)", localMetadata.getURI().toUri().toASCIIString(), metadata.getURI().toUri().toASCIIString(), hash, nodeName));
                    return assignment;
                } else {
                    throw new IOException(String.format("cannot find a local data export entry - %s", metadata.getURI().getPath()));
                }
            }
            
            // go remote
            LOG.debug(String.format("prefetchDataChunk: Putting a pending request for a prefetching for - %s, hash(%s)", metadata.getURI().toUri().toASCIIString(), hash));
            
            ClusterManager clusterManager = stargateService.getClusterManager();
            Cluster localCluster = clusterManager.getLocalCluster();
            
            // determine where to copy 
            Node determinedLocalNode = this.transferLayoutAlgorithm.determineLocalNode(localCluster, recipe, hash);
            if(determinedLocalNode == null) {
                throw new IOException(String.format("Could not determine local node for hash(%s)", hash));
            }

            // put to the pending chunk cache
            DataChunkCacheMetadata pendingDataChunkCache = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, recipeChunk.getLength(), 1, determinedLocalNode.getName());
            pendingDataChunkCache.addWaitingNode(determinedLocalNode.getName());
            boolean inserted = this.dataChunkCacheStore.putIfAbsent(hash, null, recipeChunk.getLength(), pendingDataChunkCache.toBytes());
            if(inserted) {
                this.lastUpdateTime = DateTimeUtils.getTimestamp();
                
                // send to remote
                LOG.debug(String.format("prefetchDataChunk: Sending a prefetching request for - %s, hash(%s)", metadata.getURI().toUri().toASCIIString(), hash));
                raiseEventForPrefetch(metadata.getURI(), hash, recipeChunk.getOffset(), determinedLocalNode.getName());
            }
            
            Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
            TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), determinedLocalNode.getName(), primaryAndBackupNodesForData);
            LOG.debug(String.format("prefetchDataChunk: Scheduled a prefetching for - %s, hash(%s) at node(%s)", metadata.getURI().toUri().toASCIIString(), hash, determinedLocalNode.getName()));
            return assignment;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public PendingPrefetchSchedule prefetchDataChunk2(DataObjectURI uri, String hash) throws IOException, DriverNotInitializedException {
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
            return prefetchDataChunk2(localCluster, recipe, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public PendingPrefetchSchedule prefetchDataChunk2(Cluster localCluster, DataObjectURI uri, String hash) throws IOException, DriverNotInitializedException {
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
        return prefetchDataChunk2(localCluster, recipe, hash);
    }
    
    public PendingPrefetchSchedule prefetchDataChunk2(Recipe recipe, String hash) throws IOException, DriverNotInitializedException {
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
            
            return prefetchDataChunk2(localCluster, recipe, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public PendingPrefetchSchedule prefetchDataChunk2(Cluster localCluster, Recipe recipe, String hash) throws IOException, DriverNotInitializedException {
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
        
        safeInitLayoutAlgorithm();
        
        DataObjectMetadata metadata = recipe.getMetadata();
        RecipeChunk recipeChunk = recipe.getChunk(hash);
        
        LOG.debug(String.format("prefetchDataChunk2: Scheduling a prefetching - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));
        
        if(recipeChunk == null) {
            throw new IllegalArgumentException(String.format("cannot find recipe chunk for hash %s", hash));
        }
        
        try {
            StargateService stargateService = getStargateService();
            
            // check cache
            LOG.debug(String.format("prefetchDataChunk2: Checking existing cache for - %s, hash(%s)", metadata.getURI().toUri().toASCIIString(), hash));
            
            BigKeyValueStoreMetadata existingMetadata = this.dataChunkCacheStore.getMetadata(hash);
            
            if(existingMetadata != null) {
                DataChunkCacheMetadata existingDataChunkCacheMetadata = DataChunkCacheMetadata.fromBytes(existingMetadata.getExtra());
                
                switch (existingDataChunkCacheMetadata.getType()) {
                    case DATA_CHUNK_CACHE_PENDING:
                    {
                        Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
                        TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), existingDataChunkCacheMetadata.getTransferNode(), primaryAndBackupNodesForData);
                        LOG.debug(String.format("prefetchDataChunk2: Found a pending prefetch schedule for - %s, hash(%s) at node(%s)", metadata.getURI().toUri().toASCIIString(), hash, existingDataChunkCacheMetadata.getTransferNode()));
                        return new PendingPrefetchSchedule(assignment);
                    }
                    case DATA_CHUNK_CACHE_PRESENT:
                    {
                        Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
                        String primaryNodeForData = primaryAndBackupNodesForData.iterator().next();
                        TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), primaryNodeForData, primaryAndBackupNodesForData);
                        LOG.debug(String.format("prefetchDataChunk2: Found a local cache for - %s, hash(%s) at node(%s)", metadata.getURI().toUri().toASCIIString(), hash, primaryNodeForData));
                        return new PendingPrefetchSchedule(assignment);
                    }
                    default:
                        throw new IOException("Unknown data chunk cache type");
                }
            }

            // check local recipes
            RecipeManager recipeManager = stargateService.getRecipeManager();
            
            LOG.debug(String.format("prefetchDataChunk2: Checking local recipes for a prefetching for - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));
            
            Recipe localRecipe = recipeManager.getRecipeByHash(hash);
            if(localRecipe != null) {
                DataExportManager dataExportManager = stargateService.getDataExportManager();
                DataObjectMetadata localMetadata = localRecipe.getMetadata();
                
                DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(localMetadata.getURI().getPath());
                if(dataExportEntry != null) {
                    RecipeChunk localChunk = localRecipe.getChunk(hash);
                    Collection<Integer> nodeIDs = localChunk.getNodeIDs();
                    Collection<String> nodeNames = localRecipe.getNodeNames(nodeIDs);
                    
                    String nodeName = nodeNames.iterator().next();
                    TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), nodeName, nodeNames);
                    LOG.debug(String.format("prefetchDataChunk2: Found a local recipe (%s) for - %s, hash(%s) at node(%s)", localMetadata.getURI().toUri().toASCIIString(), metadata.getURI().toUri().toASCIIString(), hash, nodeName));
                    return new PendingPrefetchSchedule(assignment);
                } else {
                    throw new IOException(String.format("cannot find a local data export entry - %s", metadata.getURI().getPath()));
                }
            }
            
            // go remote
            LOG.debug(String.format("prefetchDataChunk2: Putting a pending request for a prefetching for - %s, hash(%s)", metadata.getURI().toUri().toASCIIString(), hash));
            
            // determine where to copy 
            Node determinedLocalNode = this.transferLayoutAlgorithm.determineLocalNode(localCluster, recipe, hash);
            if(determinedLocalNode == null) {
                throw new IOException(String.format("Could not determine local node for hash(%s)", hash));
            }

            // put to the pending chunk cache
            DataChunkCacheMetadata pendingDataChunkCache = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, recipeChunk.getLength(), 1, determinedLocalNode.getName());
            pendingDataChunkCache.addWaitingNode(determinedLocalNode.getName());
            
            Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
            TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), determinedLocalNode.getName(), primaryAndBackupNodesForData);
            return new PendingPrefetchSchedule(assignment, pendingDataChunkCache);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public void processPendingPrefetchSchedules(Collection<PendingPrefetchSchedule> pendingPrefetchSchedules) throws IOException, DriverNotInitializedException {
        if(pendingPrefetchSchedules == null) {
            throw new IllegalArgumentException("pendingPrefetchSchedules is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        LOG.debug("processPendingPrefetchSchedules: Processing pending prefetching schedules");
        List<TransferAssignment> assignments = new ArrayList<TransferAssignment>();
        
        pendingPrefetchSchedules.parallelStream().forEach(new Consumer<PendingPrefetchSchedule>() {
            @Override
            public void accept(PendingPrefetchSchedule schedule) {
                TransferAssignment transferAssignment = schedule.getTransferAssignment();
                DataChunkCacheMetadata dataChunkCacheMetadata = schedule.getDataChunkCacheMetadata();

                try {
                    LOG.debug(String.format("processPendingPrefetchSchedules: Putting a pending request for a prefetching - %s, hash(%s)", transferAssignment.getDataObjectURI().toUri().toASCIIString(), dataChunkCacheMetadata.getHash()));
                    boolean inserted = dataChunkCacheStore.putIfAbsent(dataChunkCacheMetadata.getHash(), null, dataChunkCacheMetadata.getSize(), dataChunkCacheMetadata.toBytes());
                    if(inserted) {
                        assignments.add(transferAssignment);
                    }
                } catch(Exception ex) {
                    LOG.error(ex);
                }
            }
        });
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
        
        if(!assignments.isEmpty()) {
            // send to remote
            LOG.debug("processPendingPrefetchSchedules: Sending a prefetching request");
            raiseEventForPrefetch(assignments);
        }
    }
    
    public void processPendingPrefetchSchedulesAsync(Collection<PendingPrefetchSchedule> pendingPrefetchSchedules) throws IOException, DriverNotInitializedException {
        if(pendingPrefetchSchedules == null) {
            throw new IllegalArgumentException("pendingPrefetchSchedules is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    processPendingPrefetchSchedules(pendingPrefetchSchedules);
                } catch (IOException ex) {
                    LOG.error("IOException", ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error("Driver is not initialized", ex);
                }
            }
        };
        this.pendingPrefetchThreadPool.execute(r);
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

            Node remoteNode = this.contactNodeSelectionAlgorithm.getResponsibleRemoteNode(clusterManager.getLocalCluster(), clusterManager.getLocalNode(), remoteCluster);
            if(remoteNode == null) {
                throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", clusterName));
            }

            return getDirectory(remoteNode, uri);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
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
            
            Node remoteNode = this.contactNodeSelectionAlgorithm.getResponsibleRemoteNode(clusterManager.getLocalCluster(), clusterManager.getLocalNode(), remoteCluster);
            if(remoteNode == null) {
                throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", clusterName));
            }
            
            return listDataObjectMetadata(remoteNode, uri);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
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

            Node remoteNode = this.contactNodeSelectionAlgorithm.getResponsibleRemoteNode(clusterManager.getLocalCluster(), clusterManager.getLocalNode(), remoteCluster);
            if(remoteNode == null) {
                throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", clusterName));
            }

            return getRecipe(remoteNode, uri);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
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
    
    private void raiseEventForOnDemandTransfer(DataObjectURI uri, String hash, long offset, String nodeName) throws IOException, DriverNotInitializedException {
        TransferEvent transferEvent = new TransferEvent(TransferEventType.TRANSFER_EVENT_TYPE_ONDEMAND, uri, hash, offset);
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Node localNode = clusterManager.getLocalNode();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_TRANSPORT, nodeName, localNode.getName(), transferEvent.toJson());
            eventManager.raiseEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
        }
    }
    
    private void raiseEventForPrefetchStart(DataObjectURI uri, long offset) throws IOException, DriverNotInitializedException {
        TransferEvent transferEvent = new TransferEvent(TransferEventType.TRANSFER_EVENT_TYPE_PREFETCH_START, uri, offset);
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Cluster localCluster = clusterManager.getLocalCluster();
            Node localNode = clusterManager.getLocalNode();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_TRANSPORT, localCluster.getNodeNames(), localNode.getName(), transferEvent.toJson());
            eventManager.raiseEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
        }
    }
    
    private void raiseEventForPrefetch(DataObjectURI uri, String hash, long offset, String nodeName) throws IOException, DriverNotInitializedException {
        TransferEvent transferEvent = new TransferEvent(TransferEventType.TRANSFER_EVENT_TYPE_PREFETCH, uri, hash, offset);
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Node localNode = clusterManager.getLocalNode();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_TRANSPORT, nodeName, localNode.getName(), transferEvent.toJson());
            eventManager.raiseEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
        }
    }
    
    private void raiseEventForPrefetch(Collection<TransferAssignment> assignments) throws IOException, DriverNotInitializedException {
        List<StargateEvent> stargateEvents = new ArrayList<StargateEvent>();
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Node localNode = clusterManager.getLocalNode();
            
            for(TransferAssignment assignment : assignments) {
                TransferEvent transferEvent = new TransferEvent(TransferEventType.TRANSFER_EVENT_TYPE_PREFETCH, assignment.getDataObjectURI(), assignment.getHash(), assignment.getOffset());
                StargateEvent stargateEvent = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_TRANSPORT, assignment.getTransferNode(), localNode.getName(), transferEvent.toJson());
                stargateEvents.add(stargateEvent);
            }
            
            if(!stargateEvents.isEmpty()) {
                eventManager.raiseEvents(stargateEvents);
            }
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
        }
    }
    
    private void raiseEventForTransferCompletion(DataObjectURI uri, String hash, long offset) throws IOException, DriverNotInitializedException {
        TransferEvent transferEvent = new TransferEvent(TransferEventType.TRANSFER_EVENT_TYPE_COMPLETE, uri, hash, offset);
        
        try {
            StargateService service = getStargateService();
            EventManager eventManager = service.getEventManager();
            ClusterManager clusterManager = service.getClusterManager();
            Cluster localCluster = clusterManager.getLocalCluster();
            Collection<String> nodeNames = localCluster.getNodeNames();
            Node localNode = clusterManager.getLocalNode();
            
            StargateEvent event = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_TRANSPORT, nodeNames, localNode.getName(), transferEvent.toJson());
            eventManager.raiseEvent(event);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
        }
    }
    
    private void processTransferEvent(TransferEvent event) throws IOException, DriverNotInitializedException {
        TransportManagerConfig managerConfig = (TransportManagerConfig) this.config;
        
        switch(event.getEventType()) {
            case TRANSFER_EVENT_TYPE_ONDEMAND:
                LOG.debug(String.format("processTransferEvent: On-demand transfer is requested : %s - hash(%s), offset(%d)", event.getDataObjectURI().toUri().toASCIIString(), event.getHash(), event.getOffset()));
                OnDemandTransferTask onDemandTask = new OnDemandTransferTask(event.getHash(), this, event.getDataObjectURI(), event.getHash(), event.getOffset());
                this.transferScheduler.schedule(onDemandTask);
                // start if there're prefetch schedules pending
                this.transferScheduler.startPrefetch(event.getDataObjectURI(), event.getOffset(), managerConfig.getPrefetchWindowSize(), managerConfig.getPrefetchBlocks());
                break;
            case TRANSFER_EVENT_TYPE_PREFETCH:
                LOG.debug(String.format("processTransferEvent: A prefetching is requested : %s - hash(%s), offset(%d)", event.getDataObjectURI().toUri().toASCIIString(), event.getHash(), event.getOffset()));
                PrefetchTask prefetchTask = new PrefetchTask(event.getHash(), this, event.getDataObjectURI(), event.getHash(), event.getOffset());
                this.transferScheduler.schedule(prefetchTask);
                break;
            case TRANSFER_EVENT_TYPE_PREFETCH_START:
                LOG.debug(String.format("processTransferEvent: Start prefetching : %s - offset(%d)", event.getDataObjectURI().toUri().toASCIIString(), event.getOffset()));
                this.transferScheduler.startPrefetch(event.getDataObjectURI(), event.getOffset(), managerConfig.getPrefetchWindowSize(), managerConfig.getPrefetchBlocks());
                break;
            case TRANSFER_EVENT_TYPE_COMPLETE:
                LOG.debug(String.format("processTransferEvent: Transfer is finished : %s - hash(%s), offset(%d)", event.getDataObjectURI().toUri().toASCIIString(), event.getHash(), event.getOffset()));
                Reference reference = this.waitObjects.get(event.getHash());
                if(reference != null) {
                    reference.wakeup();
                }
                break;
            default:
                LOG.error(String.format("processTransferEvent: cannot handle %s", event.getEventType().name()));
                break;
        }
    }
    
    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
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
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }

    public Recipe getRemoteRecipeWithTransferSchedule(DataObjectURI uri) throws IOException, DriverNotInitializedException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitLocalizedRemoteRecipeCacheStore();
        
        synchronized(this.localizedRemoteRecipeSyncObj) {
            Recipe cachedLocalizedRecipe = (Recipe) this.localizedRemoteRecipeCacheStore.get(uri.toUri().toASCIIString());
            if(cachedLocalizedRecipe == null) {
                Recipe recipe = null;

                try {
                    StargateService service = getStargateService();
                    ClusterManager clusterManager = service.getClusterManager();

                    Cluster localCluster = clusterManager.getLocalCluster();

                    Recipe remoteRecipe = getRecipe(uri);
                    Recipe newRecipe = new Recipe(remoteRecipe.getMetadata(), remoteRecipe.getHashAlgorithm(), remoteRecipe.getChunkSize(), localCluster.getNodeNames());

                    Collection<RecipeChunk> recipeChunks = remoteRecipe.getChunks();
                    Map<RecipeChunk, Exception> streamExMap = new ConcurrentHashMap<RecipeChunk, Exception>();
                    Queue<PendingPrefetchSchedule> pendingPrefetchSchedules = new ConcurrentLinkedQueue<PendingPrefetchSchedule>();

                    recipeChunks.parallelStream().forEach(new Consumer<RecipeChunk>() {
                        @Override
                        public void accept(RecipeChunk chunk) {
                            try {
                                RecipeChunk newChunk = new RecipeChunk(chunk.getOffset(), chunk.getLength(), chunk.getHash());

                                PendingPrefetchSchedule pendingPrefetchSchedule = prefetchDataChunk2(localCluster, remoteRecipe, chunk.getHash());
                                TransferAssignment assignment = pendingPrefetchSchedule.getTransferAssignment();

                                String assignedNodeName = assignment.getTransferNode();
                                int assignedNodeID = newRecipe.getNodeID(assignedNodeName);
                                newChunk.addNodeID(assignedNodeID);

                                Collection<String> accessNodeNames = assignment.getAccessNodes();
                                for(String accessNodeName : accessNodeNames) {
                                    int accessNodeID = newRecipe.getNodeID(accessNodeName);
                                    if(accessNodeID != assignedNodeID) {
                                        newChunk.addNodeID(accessNodeID);
                                    }
                                }

                                newRecipe.addChunk(newChunk);

                                if(pendingPrefetchSchedule.hasDataChunkCacheMetadata()) {
                                    pendingPrefetchSchedules.add(pendingPrefetchSchedule);
                                }
                            } catch (IOException ex) {
                                streamExMap.put(chunk, ex);
                            } catch (DriverNotInitializedException ex) {
                                streamExMap.put(chunk, ex);
                            }
                        }
                    });
                    
                    // process pending prefetch schedules
                    if(!pendingPrefetchSchedules.isEmpty()) {
                        List<PendingPrefetchSchedule> sortedPendingPrefetchSchedules = new ArrayList<PendingPrefetchSchedule>();
                        sortedPendingPrefetchSchedules.addAll(pendingPrefetchSchedules);
                        Collections.sort(sortedPendingPrefetchSchedules, new PendingPrefetchScheduleComparator());

                        processPendingPrefetchSchedulesAsync(sortedPendingPrefetchSchedules);
                    }

                    if(!streamExMap.isEmpty()) {
                        Collection<Exception> values = streamExMap.values();
                        Exception ex = values.iterator().next();
                        throw new IOException(ex);
                    }

                    recipe = newRecipe;
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error("Manager is not instantiated", ex);
                    throw new IOException(ex);
                }

                this.localizedRemoteRecipeCacheStore.put(uri.toUri().toASCIIString(), recipe);
                cachedLocalizedRecipe = recipe;
            }
            return cachedLocalizedRecipe;
        }
    }
}
