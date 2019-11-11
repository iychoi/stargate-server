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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import stargate.commons.driver.AbstractDriver;
import stargate.commons.driver.DriverFailedToLoadException;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.BigKeyValueStoreMetadata;
import stargate.commons.datastore.DataStoreProperties;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.event.AbstractEventHandler;
import stargate.commons.event.StargateEvent;
import stargate.commons.event.StargateEventType;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.transport.AbstractTransportClient;
import stargate.commons.transport.AbstractTransportDriver;
import stargate.commons.transport.TransportServiceInfo;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.utils.IOUtils;
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
                    
                    // exclude non-data-nodes
                    ClusterManager clusterManager = stargateService.getClusterManager();
                    Cluster localCluster = clusterManager.getLocalCluster();
                    Collection<String> nonDataNodeNames = localCluster.getNonDataNodeNames();
                    properties.addNonDataNodes(nonDataNodeNames);
                    
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
        
        //AbstractLock lock = null;
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Node localNode = clusterManager.getLocalNode();
            
            //DataStoreManager dataStoreManager = stargateService.getDataStoreManager();
            //AbstractDataStoreDriver dataStoreDriver = dataStoreManager.getDriver();
            //lock = dataStoreDriver.getLock(hash);
            //lock.lock();
            
            // put to the cache
            boolean putPendingChunkCache = false;
            DataChunkCacheMetadata pendingDataChunkCacheMetadata = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, 1, localNode.getName());
            boolean insert = this.dataChunkCacheStore.putIfAbsent(hash, pendingDataChunkCacheMetadata.toBigKeyValueStoreMetadata(), null);
            if(insert) {
                this.lastUpdateTime = DateTimeUtils.getTimestamp();
                putPendingChunkCache = true;
            }
            
            this.waitObjects.putIfAbsent(hash, new Reference());
            Reference reference = this.waitObjects.get(hash);
            reference.increaseReference();
            // double-check cache
            if(!putPendingChunkCache) {
                BigKeyValueStoreMetadata existingMetadata = this.dataChunkCacheStore.getMetadata(hash);
                
                if(existingMetadata != null) {
                    DataChunkCacheMetadata existingDataChunkCacheMetadata = DataChunkCacheMetadata.fromBigKeyValueStoreMetadata(existingMetadata);
                    
                    if(existingDataChunkCacheMetadata.getType().equals(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT)) {
                        // existing
                        reference.wakeup();
                        reference.decreaseReference();
                        if(reference.getReferenceCount() <= 0) {
                            this.waitObjects.remove(hash);
                        }

                        LOG.debug(String.format("cacheRemoteDataChunk: Found a chunk cache for - %s, %s", uri.toUri().toASCIIString(), hash));
                        return;
                    } else if(existingDataChunkCacheMetadata.getType().equals(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING)) {
                        String transferNode = existingDataChunkCacheMetadata.getTransferNode();
                        if(!localNode.getName().equals(transferNode)) {
                            // it's not my task
                            reference.decreaseReference();
                            if(reference.getReferenceCount() <= 0) {
                                this.waitObjects.remove(hash);
                            }
                            LOG.debug(String.format("cacheRemoteDataChunk: Transfer schedule is found but pending (not the task of this node) for %s, %s", uri.toUri().toASCIIString(), hash));
                            return;
                        }
                    } else {
                        throw new IOException("Unknown data chunk cache type");
                    }
                }
            }
            
            //lock.unlock();
            //lock = null;
            
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

            StatisticsManager statisticsManager = stargateService.getStatisticsManager();
            statisticsManager.addDataChunkTransferReceiveStartStatistics(uri.toUri().toASCIIString(), hash);

            LOG.info(String.format("Download a data chunk %s", hash));
            InputStream dataChunkInputStream = client.getDataChunk(hash);
            if (dataChunkInputStream == null) {
                reference.decreaseReference();
                if(reference.getReferenceCount() <= 0) {
                    this.waitObjects.remove(hash);
                }
                throw new IOException("dataChunkInputStream is null");
            }

            statisticsManager.addDataChunkTransferReceiveEndStatistics(uri.toUri().toASCIIString(), hash);

            // decrease workload
            //this.transferLayoutAlgorithm.decreaseNodeWorkload(localCluster, localNode);
            this.transferLayoutAlgorithm.decreaseNodeWorkload(remoteCluster, remoteNode);

            // put to the cache
            DataChunkCacheMetadata dataChunkCacheMetadata = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT, hash, Integer.MAX_VALUE, null);
            LOG.debug(String.format("cacheRemoteDataChunk: Putting a data chunk cache for - %s", hash));
            this.dataChunkCacheStore.put(hash, dataChunkCacheMetadata.toBigKeyValueStoreMetadata(), dataChunkInputStream);
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
        } finally {
            //if(lock != null) {
            //    lock.unlock();
            //}
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
        
        TransportManagerConfig managerConfig = (TransportManagerConfig) this.config;
        
        Recipe recipe = getRecipe(uri);
        RecipeChunk recipeChunk = recipe.getChunk(hash);
        
        LOG.debug(String.format("getDataChunk: Get a data chunk - %s, %s", uri.toUri().toASCIIString(), hash));
        
        //AbstractLock lock = null;
        try {
            StargateService stargateService = getStargateService();
            
            // check local recipes
            RecipeManager recipeManager = stargateService.getRecipeManager();
            
            LOG.debug(String.format("getDataChunk: Checking local recipes for - %s, %s", uri.toUri().toASCIIString(), hash));
            
            Recipe localRecipe = recipeManager.getRecipeByHash(hash);
            if(localRecipe != null) {
                DataExportManager dataExportManager = stargateService.getDataExportManager();
                DataObjectMetadata metadata = localRecipe.getMetadata();

                DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(metadata.getURI().getPath());
                if(dataExportEntry != null) {
                    DataSourceManager dataSourceManager = stargateService.getDataSourceManager();

                    URI sourceURI = dataExportEntry.getSourceURI();
                    AbstractDataSourceDriver driver = dataSourceManager.getDriver(sourceURI);

                    RecipeChunk localRecipeChunk = localRecipe.getChunk(hash);
                    //Collection<Integer> nodeIDs = localRecipeChunk.getNodeIDs();
                    //Collection<String> nodeNames = localRecipe.getNodeNames(nodeIDs);

                    // just do it because it works for HDFS
                    //if(nodeNames.contains(localNode.getName())) {
                        InputStream inputStream = driver.openFile(sourceURI, localRecipeChunk.getOffset(), localRecipeChunk.getLength());
                        byte[] cacheDataBytes = IOUtils.toByteArray(inputStream);
                        inputStream.close();

                        ByteArrayInputStream bais = new ByteArrayInputStream(cacheDataBytes);

                        LOG.debug(String.format("getDataChunk: Found a local chunk for - %s, %s", uri.toUri().toASCIIString(), hash));

                        return bais;
                    //} else {
                    //}
                }
            }
            
            ClusterManager clusterManager = stargateService.getClusterManager();
            Cluster remoteCluster = clusterManager.getRemoteCluster(uri.getClusterName());
            if(remoteCluster == null) {
                throw new IOException(String.format("remote cluster %s does not exist", uri.getClusterName()));
            }
            
            Node localNode = clusterManager.getLocalNode();
            
            //DataStoreManager dataStoreManager = stargateService.getDataStoreManager();
            //AbstractDataStoreDriver dataStoreDriver = dataStoreManager.getDriver();
            //lock = dataStoreDriver.getLock(hash);
            //lock.lock();
            
            LOG.debug(String.format("getDataChunk: Checking and putting a pending request for an on-demand transfer for - %s, %s", uri.toUri().toASCIIString(), hash));
            
            // put to the cache
            boolean putPendingChunkCache = false;
            DataChunkCacheMetadata pendingDataChunkCache = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, 1, localNode.getName());
            boolean insert = this.dataChunkCacheStore.putIfAbsent(hash, pendingDataChunkCache.toBigKeyValueStoreMetadata(), null);
            if(insert) {
                LOG.debug(String.format("getDataChunk: Put a pending request for a on-demand transfer for - %s, %s", uri.toUri().toASCIIString(), hash));
                this.lastUpdateTime = DateTimeUtils.getTimestamp();
                putPendingChunkCache = true;
            }
            
            this.waitObjects.putIfAbsent(hash, new Reference());
            Reference reference = this.waitObjects.get(hash);
            reference.increaseReference();
            
            // double-check cache
            if(!putPendingChunkCache) {
                while(true) {
                    BigKeyValueStoreMetadata existingMetadata = this.dataChunkCacheStore.getMetadata(hash);
                    
                    if(existingMetadata != null) {
                        DataChunkCacheMetadata existingDataChunkCacheMetadata = DataChunkCacheMetadata.fromBigKeyValueStoreMetadata(existingMetadata);
                        
                        if(existingDataChunkCacheMetadata.getType().equals(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT)) {
                            // existing
                            reference.wakeup();
                            reference.decreaseReference();
                            if(reference.getReferenceCount() <= 0) {
                                this.waitObjects.remove(hash);
                            }

                            LOG.debug(String.format("getDataChunk: Found a chunk cache for - %s, %s", uri.toUri().toASCIIString(), hash));
                            return this.dataChunkCacheStore.getData(hash);
                        } else if(existingDataChunkCacheMetadata.getType().equals(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING)) {
                            LOG.debug(String.format("getDataChunk: A request for a on-demand transfer for - %s, %s is still pending", uri.toUri().toASCIIString(), hash));
                            
                            // wait until the data transfer is complete
                            if(!existingDataChunkCacheMetadata.getWaitingNodes().contains(localNode.getName())) {
                                DataChunkCacheMetadata newDataChunkCache = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, existingDataChunkCacheMetadata.getVersion() + 1, existingDataChunkCacheMetadata.getTransferNode());
                                newDataChunkCache.addWaitingNodes(existingDataChunkCacheMetadata.getWaitingNodes());
                                newDataChunkCache.addWaitingNode(localNode.getName());

                                boolean replaced = this.dataChunkCacheStore.replace(hash, existingMetadata, newDataChunkCache.toBigKeyValueStoreMetadata(), null);
                                if(replaced) {
                                    this.lastUpdateTime = DateTimeUtils.getTimestamp();
                                } else {
                                    LOG.warn("getDataChunk: Could not replaced chunk cache entry - try it again");
                                    continue;
                                }
                            }
                            
                            // increase priority by adding a new one
                            raiseEventForOnDemandTransfer(uri, hash, recipeChunk.getOffset(), existingDataChunkCacheMetadata.getTransferNode());
                            // tentatively unlock to allow prefetch to work
                            //lock.unlock();
                            
                            try {
                                LOG.debug(String.format("getDataChunk: Waiting for finishing data transfer for %s", hash));
                                if(reference.await(managerConfig.getDataTransferTimeoutSec(), TimeUnit.SECONDS)) {
                                    // done
                                    // re-check
                                    while(true) {
                                        //lock.lock();

                                        LOG.debug(String.format("getDataChunk: Re-checking transferred data for %s", hash));
                                        BigKeyValueStoreMetadata existingMetadata2 = this.dataChunkCacheStore.getMetadata(hash);
                                        DataChunkCacheMetadata dataChunkCache = DataChunkCacheMetadata.fromBigKeyValueStoreMetadata(existingMetadata2);

                                        if(dataChunkCache.getType().equals(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT)) {
                                            reference.wakeup();
                                            reference.decreaseReference();
                                            if(reference.getReferenceCount() <= 0) {
                                                this.waitObjects.remove(hash);
                                            }

                                            LOG.debug(String.format("getDataChunk: Found a chunk cache for - %s, %s", uri.toUri().toASCIIString(), hash));
                                            return this.dataChunkCacheStore.getData(hash);
                                        } else {
                                            //lock.unlock();
                                            LOG.debug(String.format("getDataChunk: Could not find transferred data for %s - sleep", hash));
                                            Thread.sleep(1000);
                                        }
                                    }
                                } else {
                                    // timeout
                                    //lock = null;

                                    reference.decreaseReference();
                                    if(reference.getReferenceCount() <= 0) {
                                        this.waitObjects.remove(hash);
                                    }
                                    throw new IOException(String.format("getDataChunk: Timeout for waiting to finish data transfer for %s", hash));
                                }
                            } catch (InterruptedException ex) {
                                //lock = null;

                                LOG.error("InterruptedException", ex);
                                reference.decreaseReference();
                                if(reference.getReferenceCount() <= 0) {
                                    this.waitObjects.remove(hash);
                                }
                                throw new IOException(ex);
                            }
                        } else {
                            throw new IOException("Unknown data chunk cache type");
                        }
                    } else {
                        throw new IOException("null data chunk cache");
                    }
                }
            }
            
            //lock.unlock();
            //lock = null;

            Node remoteNode = this.transferLayoutAlgorithm.determineRemoteNode(remoteCluster, recipe, hash);
            if(remoteNode == null) {
                throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", uri.getClusterName()));
            }

            // go remote
            AbstractTransportDriver driver = getDriver();
            AbstractTransportClient client = driver.getClient(remoteNode);

            // increase workload
            //Cluster localCluster = clusterManager.getLocalCluster();

            //this.transferLayoutAlgorithm.increaseNodeWorkload(localCluster, localNode);
            this.transferLayoutAlgorithm.increaseNodeWorkload(remoteCluster, remoteNode);

            StatisticsManager statisticsManager = stargateService.getStatisticsManager();
            statisticsManager.addDataChunkTransferReceiveStartStatistics(uri.toUri().toASCIIString(), hash);

            LOG.info(String.format("Download a data chunk %s", hash));
            InputStream dataChunkInputStream = client.getDataChunk(hash);

            // fully download the chunk and cache and return
            statisticsManager.addDataChunkTransferReceiveEndStatistics(uri.toUri().toASCIIString(), hash);

            // decrease workload
            //this.transferLayoutAlgorithm.decreaseNodeWorkload(localCluster, localNode);
            this.transferLayoutAlgorithm.decreaseNodeWorkload(remoteCluster, remoteNode);

            // put to the cache
            DataChunkCacheMetadata dataChunkCache = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT, hash, Integer.MAX_VALUE);
            LOG.debug(String.format("getDataChunk: Putting a data chunk cache for - %s", hash));
            this.dataChunkCacheStore.put(hash, dataChunkCache.toBigKeyValueStoreMetadata(), dataChunkInputStream);
            this.lastUpdateTime = DateTimeUtils.getTimestamp();

            LOG.debug(String.format("getDataChunk: Waking up all threads that are waiting for a data chunk for - %s", hash));
            reference.wakeup();
            reference.decreaseReference();
            if(reference.getReferenceCount() <= 0) {
                this.waitObjects.remove(hash);
            }

            //notify
            raiseEventForTransferCompletion(uri, hash, recipeChunk.getOffset());

            LOG.debug(String.format("getDataChunk: Get a chunk for - %s, %s", uri.toUri().toASCIIString(), hash));

            return this.dataChunkCacheStore.getData(hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        } finally {
            //if(lock != null) {
            //    lock.unlock();
            //}
            
            // do not propagate prefetch start event to other nodes in the cluster
            //raiseEventForPrefetchStart(uri, recipeChunk.getOffset());
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
        
        //AbstractLock lock = null;
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();

            Cluster localCluster = clusterManager.getLocalCluster();
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
                }
            }
            
            //DataStoreManager dataStoreManager = stargateService.getDataStoreManager();
            //AbstractDataStoreDriver dataStoreDriver = dataStoreManager.getDriver();
            //lock = dataStoreDriver.getLock(hash);
            //lock.lock();
            
            LOG.debug(String.format("prefetchDataChunk: Checking a pending request for a prefetching for - %s, hash(%s)", metadata.getURI().toUri().toASCIIString(), hash));
            
            // check cache and go remote
            BigKeyValueStoreMetadata existingMetadata = this.dataChunkCacheStore.getMetadata(hash);
            
            if(existingMetadata != null) {
                DataChunkCacheMetadata existingDataChunkCacheMetadata = DataChunkCacheMetadata.fromBigKeyValueStoreMetadata(existingMetadata);
                
                if(existingDataChunkCacheMetadata.getType().equals(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING)) {
                    Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
                    TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), existingDataChunkCacheMetadata.getTransferNode(), primaryAndBackupNodesForData);
                    LOG.debug(String.format("prefetchDataChunk: Found a pending prefetch schedule for - %s, hash(%s) at node(%s)", metadata.getURI().toUri().toASCIIString(), hash, existingDataChunkCacheMetadata.getTransferNode()));
                    return assignment;
                } else if(existingDataChunkCacheMetadata.getType().equals(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT)) {
                    Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
                    String primaryNodeForData = primaryAndBackupNodesForData.iterator().next();
                    TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), primaryNodeForData, primaryAndBackupNodesForData);
                    LOG.debug(String.format("prefetchDataChunk: Found a local cache for - %s, hash(%s) at node(%s)", metadata.getURI().toUri().toASCIIString(), hash, primaryNodeForData));
                    return assignment;
                } else {
                    throw new IOException("Unknown data chunk cache type");
                }
            }
            
            LOG.debug(String.format("prefetchDataChunk: Putting a pending request for a prefetching for - %s, hash(%s)", metadata.getURI().toUri().toASCIIString(), hash));
            
            // determine where to copy 
            Node determinedLocalNode = this.transferLayoutAlgorithm.determineLocalNode(localCluster, recipe, hash);
            if(determinedLocalNode == null) {
                throw new IOException(String.format("Could not determine local node for hash(%s)", hash));
            }

            // put to the pending chunk cache
            DataChunkCacheMetadata pendingDataChunkCache = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, 1, determinedLocalNode.getName());
            pendingDataChunkCache.addWaitingNode(determinedLocalNode.getName());
            boolean inserted = this.dataChunkCacheStore.putIfAbsent(hash, pendingDataChunkCache.toBigKeyValueStoreMetadata(), null);
            if(inserted) {
                this.lastUpdateTime = DateTimeUtils.getTimestamp();
                
                // send to remote
                LOG.debug(String.format("prefetchDataChunk: Sending a prefetching request for - %s, hash(%s)", metadata.getURI().toUri().toASCIIString(), hash));
                raiseEventForPrefetch(metadata.getURI(), hash, recipeChunk.getOffset(), determinedLocalNode.getName());
            }
            
            //lock.unlock();
            //lock = null;

            Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
            TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), determinedLocalNode.getName(), primaryAndBackupNodesForData);
            LOG.debug(String.format("prefetchDataChunk: Scheduled a prefetching for - %s, hash(%s) at node(%s)", metadata.getURI().toUri().toASCIIString(), hash, determinedLocalNode.getName()));
            return assignment;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        } finally {
            //if(lock != null) {
            //    lock.unlock();
            //}
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
        
        //AbstractLock lock = null;
        try {
            StargateService stargateService = getStargateService();

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
                }
            }
            
            //DataStoreManager dataStoreManager = stargateService.getDataStoreManager();
            //AbstractDataStoreDriver dataStoreDriver = dataStoreManager.getDriver();
            //lock = dataStoreDriver.getLock(hash);
            //lock.lock();
            
            LOG.debug(String.format("prefetchDataChunk2: Checking a pending request for a prefetching for - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));
            
            // check cache and go remote
            BigKeyValueStoreMetadata existingMetadata = this.dataChunkCacheStore.getMetadata(hash);
            
            if(existingMetadata != null) {
                DataChunkCacheMetadata existingDataChunkCacheMetadata = DataChunkCacheMetadata.fromBigKeyValueStoreMetadata(existingMetadata);
                
                if(existingDataChunkCacheMetadata.getType().equals(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING)) {
                    Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
                    TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), existingDataChunkCacheMetadata.getTransferNode(), primaryAndBackupNodesForData);
                    LOG.debug(String.format("prefetchDataChunk2: Found a pending prefetch schedule for - %s, hash(%s) at node(%s)", metadata.getURI().toUri().toASCIIString(), hash, existingDataChunkCacheMetadata.getTransferNode()));
                    return new PendingPrefetchSchedule(assignment);
                } else if(existingDataChunkCacheMetadata.getType().equals(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT)) {
                    Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
                    String primaryNodeForData = primaryAndBackupNodesForData.iterator().next();
                    TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), primaryNodeForData, primaryAndBackupNodesForData);
                    LOG.debug(String.format("prefetchDataChunk2: Found a local cache for - %s, hash(%s) at node(%s)", metadata.getURI().toUri().toASCIIString(), hash, primaryNodeForData));
                    return new PendingPrefetchSchedule(assignment);
                } else {
                    throw new IOException("Unknown data chunk cache type");
                }
            }
            
            LOG.debug(String.format("prefetchDataChunk2: Making a pending request for a prefetching for - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));
            
            // determine where to copy 
            Node determinedLocalNode = this.transferLayoutAlgorithm.determineLocalNode(localCluster, recipe, hash);
            if(determinedLocalNode == null) {
                throw new IOException(String.format("Could not determine local node for hash(%s)", hash));
            }

            // make the pending chunk cache
            DataChunkCacheMetadata pendingDataChunkCache = new DataChunkCacheMetadata(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, 1, determinedLocalNode.getName());
            pendingDataChunkCache.addWaitingNode(determinedLocalNode.getName());
            
            Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
            TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, recipeChunk.getOffset(), determinedLocalNode.getName(), primaryAndBackupNodesForData);
            return new PendingPrefetchSchedule(assignment, pendingDataChunkCache);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        } finally {
            //if(lock != null) {
            //    lock.unlock();
            //}
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
        
        for(PendingPrefetchSchedule schedule : pendingPrefetchSchedules) {
            TransferAssignment transferAssignment = schedule.getTransferAssignment();
            DataChunkCacheMetadata dataChunkCacheMetadata = schedule.getDataChunkCacheMetadata();
            
            LOG.debug(String.format("processPendingPrefetchSchedules: Putting a pending request for a prefetching - %s, hash(%s)", transferAssignment.getDataObjectURI().toUri().toASCIIString(), dataChunkCacheMetadata.getHash()));
            boolean inserted = this.dataChunkCacheStore.putIfAbsent(dataChunkCacheMetadata.getHash(), dataChunkCacheMetadata.toBigKeyValueStoreMetadata(), null);
            if(inserted) {
                assignments.add(transferAssignment);
            }
        }
        
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
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            ClusterManager clusterManager = stargateService.getClusterManager();
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
}
