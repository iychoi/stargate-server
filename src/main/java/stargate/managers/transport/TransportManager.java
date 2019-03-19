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
import stargate.commons.driver.AbstractDriver;
import stargate.commons.driver.DriverFailedToLoadException;
import stargate.commons.datastore.AbstractKeyValueStore;
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
    
    private static final int DEFAULT_PREFETCH_THREAD_NUM = 5;
    
    private static TransportManager instance;
    
    private AbstractKeyValueStore remoteDirectoryCacheStore; // <DataObjectURI, Directory>
    private final Object remoteDirectorySyncObj = new Object();
    private AbstractKeyValueStore remoteRecipeCacheStore; // <DataObjectURI, Recipe>
    private final Object remoteRecipeSyncObj = new Object();
    
    private AbstractKeyValueStore dataChunkCacheStore; // <String, byte[]> key = hashstring
    private final Object dataChunkSyncObj = new Object();
    private Map<String, TransferReference> waitObjects = new ConcurrentHashMap<String, TransferReference>();
    
    private AbstractContactNodeSelectionAlgorithm contactNodeSelectionAlgorithm;
    private AbstractTransferLayoutAlgorithm transferLayoutAlgorithm;
    
    private int prefetchThreadNum = DEFAULT_PREFETCH_THREAD_NUM;
    private ExecutorService prefetchThreadPool;
    
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
        
        this.prefetchThreadPool = Executors.newFixedThreadPool(this.prefetchThreadNum);
        
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
                    
                    DataStoreProperties properties = new DataStoreProperties();
                    properties.setReplicated(true);
                    properties.setPersistent(false);
                    properties.setExpirable(true);
                    properties.setExpireTimeUnit(TimeUnit.MINUTES);
                    properties.setExpireTimeVal(5);
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
                    
                    DataStoreProperties properties = new DataStoreProperties();
                    properties.setReplicated(true);
                    properties.setPersistent(false);
                    properties.setExpirable(true);
                    properties.setExpireTimeUnit(TimeUnit.DAYS);
                    properties.setExpireTimeVal(1);
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
                    
                    DataStoreProperties properties = new DataStoreProperties();
                    properties.setSharded(true);
                    properties.setReplicaNum(1);
                    properties.setPersistent(true);
                    properties.setExpirable(true);
                    properties.setExpireTimeUnit(TimeUnit.DAYS);
                    properties.setExpireTimeVal(7);
                    this.dataChunkCacheStore = keyValueStoreManager.getDriver().getKeyValueStore(DATA_CHUNK_CACHE_STORE, byte[].class, properties);
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
            TransportManagerConfig managerConfig = (TransportManagerConfig) this.getConfig();
            TransferLayoutAlgorithms transferAlg = managerConfig.getLayoutAlgorithm();
            if(transferAlg == null) {
                transferAlg = TransferLayoutAlgorithms.TRANSFER_LAYOUT_ALGORITHM_STATIC;
            }
            
            LOG.info(String.format("Using transfer layout algorithm - %s", transferAlg.name()));
            
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
            TransportManagerConfig managerConfig = (TransportManagerConfig) this.getConfig();
            ContactNodeSelectionAlgorithms selectionAlg = managerConfig.getContactNodeSelectionAlgorithm();
            if(selectionAlg == null) {
                selectionAlg = ContactNodeSelectionAlgorithms.CONTACT_NODE_SELECTION_ALGORITHM_ROUNDROBIN;
            }
            
            LOG.info(String.format("Using contact node selection algorithm - %s", selectionAlg.name()));
            
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
        
        LOG.debug(String.format("Caching a remote data chunk - %s, %s", uri.toUri().toASCIIString(), hash));
        
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Node localNode = clusterManager.getLocalNode();
            
            // check local recipes
            RecipeManager recipeManager = stargateService.getRecipeManager();
            DataExportManager dataExportManager = stargateService.getDataExportManager();
            DataSourceManager dataSourceManager = stargateService.getDataSourceManager();
            
            // put to the cache
            boolean putPendingChunkCache = false;
            DataChunkCache pendingDataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, 1, localNode.getName(), null);
            boolean insert = this.dataChunkCacheStore.putIfAbsent(hash, pendingDataChunkCache.toBytes());
            if(insert) {
                this.lastUpdateTime = DateTimeUtils.getTimestamp();
                putPendingChunkCache = true;
            }

            this.waitObjects.putIfAbsent(hash, new TransferReference());
            TransferReference reference = this.waitObjects.get(hash);
            reference.increaseReference();
            synchronized(reference) {
                Recipe localRecipe = recipeManager.getRecipeByHash(hash);
                if(localRecipe != null) {
                    DataObjectMetadata metadata = localRecipe.getMetadata();
                    DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(metadata.getURI().getPath());
                    if(dataExportEntry != null) {
                        URI sourceURI = dataExportEntry.getSourceURI();
                        AbstractDataSourceDriver driver = dataSourceManager.getDriver(sourceURI);

                        RecipeChunk chunk = localRecipe.getChunk(hash);
                        Collection<Integer> nodeIDs = chunk.getNodeIDs();
                        Collection<String> nodeNames = localRecipe.getNodeNames(nodeIDs);
                        
                        // just do it because it works for HDFS
                        //if(nodeNames.contains(localNode.getName())) {
                            InputStream inputStream = driver.openFile(sourceURI, chunk.getOffset(), chunk.getLength());
                            byte[] cacheDataBytes = IOUtils.toByteArray(inputStream);
                            inputStream.close();

                            // put to the cache
                            DataChunkCache dataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT, hash, Integer.MAX_VALUE, cacheDataBytes);
                            this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                            this.lastUpdateTime = DateTimeUtils.getTimestamp();

                            reference.finishTransfer();
                            reference.decreaseReference();
                            if(reference.getReferenceCount() <= 0) {
                                this.waitObjects.remove(hash);
                            }
                            
                            //notify
                            raiseEventForTransferCompletion(uri, hash);
                            
                            LOG.debug(String.format("Found a local chunk for - %s, %s", uri.toUri().toASCIIString(), hash));
                            return;
                        //} else {
                        //    // data is found in the local cluster but remote node
                        //    LOG.debug(String.format("Found a local chunk in the local cluster for - %s, %s, but in a remote node", uri.toUri().toASCIIString(), hash));  
                        //}
                    }
                }
            
                // get from remote
                Cluster remoteCluster = clusterManager.getRemoteCluster(uri.getClusterName());
                if(remoteCluster == null) {
                    reference.decreaseReference();
                    if(reference.getReferenceCount() <= 0) {
                        this.waitObjects.remove(hash);
                    }
                    throw new IOException(String.format("remote cluster %s does not exist", uri.getClusterName()));
                }

                // double-check cache
                if(!putPendingChunkCache) {
                    byte[] existingData = (byte[]) this.dataChunkCacheStore.get(hash);
                    if(existingData != null) {
                        DataChunkCache existingCache = DataChunkCache.fromBytes(existingData);
                        if(existingCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT) {
                            // existing
                            reference.finishTransfer();
                            reference.decreaseReference();
                            if(reference.getReferenceCount() <= 0) {
                                this.waitObjects.remove(hash);
                            }

                            LOG.debug(String.format("Found a chunk cache for - %s, %s", uri.toUri().toASCIIString(), hash));
                            return;
                        }

                        String transferNode = existingCache.getTransferNode();
                        if(!localNode.getName().equals(transferNode)) {
                            // it's not my task
                            reference.decreaseReference();
                            if(reference.getReferenceCount() <= 0) {
                                this.waitObjects.remove(hash);
                            }
                            LOG.debug(String.format("Transfer schedule is found but pending (not the task of this node) for %s, %s", uri.toUri().toASCIIString(), hash));
                            return;
                        }
                    }
                }
                
                Recipe recipe = getRecipe(uri);
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

                InputStream dataChunkInputStream = client.getDataChunk(hash);
                if (dataChunkInputStream == null) {
                    reference.decreaseReference();
                    if(reference.getReferenceCount() <= 0) {
                        this.waitObjects.remove(hash);
                    }
                    throw new IOException("dataChunkInputStream is null");
                }

                // fully download the chunk and cache and return
                byte[] cacheDataBytes = IOUtils.toByteArray(dataChunkInputStream);
                dataChunkInputStream.close();
                
                statisticsManager.addDataChunkTransferReceiveEndStatistics(uri.toUri().toASCIIString(), hash);

                // decrease workload
                //this.transferLayoutAlgorithm.decreaseNodeWorkload(localCluster, localNode);
                this.transferLayoutAlgorithm.decreaseNodeWorkload(remoteCluster, remoteNode);

                // put to the cache
                DataChunkCache dataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT, hash, Integer.MAX_VALUE, cacheDataBytes);
                this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                this.lastUpdateTime = DateTimeUtils.getTimestamp();
                
                reference.finishTransfer();
                reference.decreaseReference();
                if(reference.getReferenceCount() <= 0) {
                    this.waitObjects.remove(hash);
                }
                
                //notify
                raiseEventForTransferCompletion(uri, hash);
                
                LOG.debug(String.format("Cached a chunk for - %s, %s at %s", uri.toUri().toASCIIString(), hash, localNode.getName()));
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
        
        safeInitLayoutAlgorithm();
        
        LOG.debug(String.format("Get a remote data chunk - %s, %s", uri.toUri().toASCIIString(), hash));
        
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Cluster remoteCluster = clusterManager.getRemoteCluster(uri.getClusterName());
            if(remoteCluster == null) {
                throw new IOException(String.format("remote cluster %s does not exist", uri.getClusterName()));
            }
            
            Node localNode = clusterManager.getLocalNode();
            
            // check local recipes
            RecipeManager recipeManager = stargateService.getRecipeManager();
            
            // put to the cache
            boolean putPendingChunkCache = false;
            DataChunkCache pendingDataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, 1, localNode.getName(), null);
            boolean insert = this.dataChunkCacheStore.putIfAbsent(hash, pendingDataChunkCache.toBytes());
            if(insert) {
                this.lastUpdateTime = DateTimeUtils.getTimestamp();
                putPendingChunkCache = true;
            }
            
            this.waitObjects.putIfAbsent(hash, new TransferReference());
            TransferReference reference = this.waitObjects.get(hash);
            reference.increaseReference();
            synchronized(reference) {
                Recipe localRecipe = recipeManager.getRecipeByHash(hash);
                if(localRecipe != null) {
                    DataExportManager dataExportManager = stargateService.getDataExportManager();
                    DataObjectMetadata metadata = localRecipe.getMetadata();
                    
                    DataExportEntry dataExportEntry = dataExportManager.getDataExportEntry(metadata.getURI().getPath());
                    if(dataExportEntry != null) {
                        DataSourceManager dataSourceManager = stargateService.getDataSourceManager();

                        URI sourceURI = dataExportEntry.getSourceURI();
                        AbstractDataSourceDriver driver = dataSourceManager.getDriver(sourceURI);

                        RecipeChunk chunk = localRecipe.getChunk(hash);
                        Collection<Integer> nodeIDs = chunk.getNodeIDs();
                        Collection<String> nodeNames = localRecipe.getNodeNames(nodeIDs);
                        
                        // just do it because it works for HDFS
                        //if(nodeNames.contains(localNode.getName())) {
                            InputStream inputStream = driver.openFile(sourceURI, chunk.getOffset(), chunk.getLength());
                            byte[] cacheDataBytes = IOUtils.toByteArray(inputStream);
                            inputStream.close();

                            // put to the cache
                            DataChunkCache dataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT, hash, Integer.MAX_VALUE, cacheDataBytes);
                            this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                            this.lastUpdateTime = DateTimeUtils.getTimestamp();

                            reference.finishTransfer();
                            reference.decreaseReference();
                            if(reference.getReferenceCount() <= 0) {
                                this.waitObjects.remove(hash);
                            }
                            
                            //notify
                            raiseEventForTransferCompletion(uri, hash);
                            
                            ByteArrayInputStream bais = new ByteArrayInputStream(cacheDataBytes);
                            
                            LOG.debug(String.format("Found a local chunk for - %s, %s", uri.toUri().toASCIIString(), hash));
                            
                            return bais;
                        //} else {
                        //}
                    }
                }

                // double-check cache
                if(!putPendingChunkCache) {
                    byte[] existingData = (byte[]) this.dataChunkCacheStore.get(hash);
                    if(existingData != null) {
                        DataChunkCache existingCache = DataChunkCache.fromBytes(existingData);
                        if(existingCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT) {
                            // existing
                            byte[] data = existingCache.getData();
                            ByteArrayInputStream bais = new ByteArrayInputStream(data);

                            reference.finishTransfer();
                            reference.decreaseReference();
                            if(reference.getReferenceCount() <= 0) {
                                this.waitObjects.remove(hash);
                            }

                            LOG.debug(String.format("Found a chunk cache for - %s, %s", uri.toUri().toASCIIString(), hash));
                            return bais;
                        } else {
                            // wait until the data transfer is complete
                            while(true) {
                                byte[] bytes = (byte[]) this.dataChunkCacheStore.get(hash);
                                DataChunkCache dataChunkCache = DataChunkCache.fromBytes(bytes);

                                DataChunkCache newDataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, dataChunkCache.getVersion() + 1, dataChunkCache.getTransferNode(), null);
                                newDataChunkCache.addWaitingNodes(dataChunkCache.getWaitingNodes());
                                newDataChunkCache.addWaitingNode(localNode.getName());

                                boolean replaced = this.dataChunkCacheStore.replace(hash, bytes, newDataChunkCache.toBytes());
                                if(replaced) {
                                    this.lastUpdateTime = DateTimeUtils.getTimestamp();
                                    break;
                                } else {
                                    LOG.warn("Could not replaced chunk cache entry - try it again");
                                }
                            }

                            try {
                                LOG.debug(String.format("Waiting to finish data transfer for %s", hash));
                                reference.await(5, TimeUnit.MINUTES);
                            } catch (InterruptedException ex) {
                                LOG.error("InterruptedException", ex);
                                reference.decreaseReference();
                                if(reference.getReferenceCount() <= 0) {
                                    this.waitObjects.remove(hash);
                                }
                                throw new IOException(ex);
                            }

                            // re-check
                            existingData = (byte[]) this.dataChunkCacheStore.get(hash);
                            existingCache = DataChunkCache.fromBytes(existingData);

                            if(existingCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT) {
                                byte[] data = existingCache.getData();
                                ByteArrayInputStream bais = new ByteArrayInputStream(data);

                                reference.finishTransfer();
                                reference.decreaseReference();
                                if(reference.getReferenceCount() <= 0) {
                                    this.waitObjects.remove(hash);
                                }

                                LOG.debug(String.format("Found a chunk cache for - %s, %s", uri.toUri().toASCIIString(), hash));
                                return bais;
                            } else {
                                // something caused the waiting thread to wake up
                                reference.decreaseReference();
                                if(reference.getReferenceCount() <= 0) {
                                    this.waitObjects.remove(hash);
                                }
                                throw new IOException(String.format("Something caused the thread waiting for the data %s to wake up", hash));
                            }
                        }
                    }
                }
                
                Recipe recipe = getRecipe(uri);
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
                
                InputStream dataChunkInputStream = client.getDataChunk(hash);
                
                // fully download the chunk and cache and return
                byte[] cacheDataBytes = IOUtils.toByteArray(dataChunkInputStream);
                dataChunkInputStream.close();
                
                statisticsManager.addDataChunkTransferReceiveEndStatistics(uri.toUri().toASCIIString(), hash);
                
                // decrease workload
                //this.transferLayoutAlgorithm.decreaseNodeWorkload(localCluster, localNode);
                this.transferLayoutAlgorithm.decreaseNodeWorkload(remoteCluster, remoteNode);
                
                // put to the cache
                DataChunkCache dataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT, hash, Integer.MAX_VALUE, cacheDataBytes);
                this.dataChunkCacheStore.put(hash, dataChunkCache.toBytes());
                this.lastUpdateTime = DateTimeUtils.getTimestamp();
                
                reference.finishTransfer();
                reference.decreaseReference();
                if(reference.getReferenceCount() <= 0) {
                    this.waitObjects.remove(hash);
                }
                
                //notify
                raiseEventForTransferCompletion(uri, hash);
                
                LOG.debug(String.format("Get a chunk for - %s, %s", uri.toUri().toASCIIString(), hash));
                
                return new ByteArrayInputStream(cacheDataBytes);
            }
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
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
        
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();

            Cluster localCluster = clusterManager.getLocalCluster();
            
            Recipe recipe = getRecipe(uri);
            return prefetchDataChunk(localCluster, recipe, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public TransferAssignment prefetchDataChunk(Cluster localCluster, DataObjectURI uri, String hash) throws IOException, DriverNotInitializedException {
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
        return prefetchDataChunk(localCluster, recipe, hash);
    }
    
    public TransferAssignment prefetchDataChunk(Recipe recipe, String hash) throws IOException, DriverNotInitializedException {
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
            
            return prefetchDataChunk(localCluster, recipe, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public TransferAssignment prefetchDataChunk(Cluster localCluster, Recipe recipe, String hash) throws IOException, DriverNotInitializedException {
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
        RecipeChunk chunk = recipe.getChunk(hash);
        
        LOG.debug(String.format("Scheduling a prefetching - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));
        
        if(chunk == null) {
            throw new IllegalArgumentException(String.format("cannot find recipe chunk for hash %s", hash));
        }
        
        try {
            StargateService stargateService = getStargateService();

            // check local recipes
            RecipeManager recipeManager = stargateService.getRecipeManager();
            
            LOG.debug(String.format("Checking local recipes for a prefetching for - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));
            
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
                    TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, nodeName, nodeNames);
                    LOG.debug(String.format("Found a local recipe (%s) for - %s, %s at %s", localMetadata.getURI().toUri().toASCIIString(), metadata.getURI().toUri().toASCIIString(), hash, nodeName));
                    return assignment;
                }
            }
            
            LOG.debug(String.format("Checking a pending request for a prefetching for - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));
            
            // check cache and go remote
            byte[] existingData = (byte[]) this.dataChunkCacheStore.get(hash);
            if(existingData != null) {
                DataChunkCache existingCache = DataChunkCache.fromBytes(existingData);
                if(existingCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PENDING) {
                    Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
                    TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, existingCache.getTransferNode(), primaryAndBackupNodesForData);
                    LOG.debug(String.format("Found a pending prefetch schedule for - %s, %s at %s", metadata.getURI().toUri().toASCIIString(), hash, existingCache.getTransferNode()));
                    return assignment;
                } else if(existingCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT) {
                    Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
                    String primaryNodeForData = primaryAndBackupNodesForData.iterator().next();
                    TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, primaryNodeForData, primaryAndBackupNodesForData);
                    LOG.debug(String.format("Found a local cache for - %s, %s at %s", metadata.getURI().toUri().toASCIIString(), hash, primaryNodeForData));
                    return assignment;
                }
            }
            
            LOG.debug(String.format("Putting a pending request for a prefetching for - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));
            
            // determine where to copy 
            Node determinedLocalNode = this.transferLayoutAlgorithm.determineLocalNode(localCluster, recipe, hash);

            // put to the pending chunk cache
            DataChunkCache newDataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, 1, determinedLocalNode.getName(), null);
            newDataChunkCache.addWaitingNode(determinedLocalNode.getName());
            this.dataChunkCacheStore.put(hash, newDataChunkCache.toBytes());
            this.lastUpdateTime = DateTimeUtils.getTimestamp();
            
            LOG.debug(String.format("Sending a prefetching request for - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));

            // send to remote
            raiseEventForPrefetchTransfer(metadata.getURI(), hash, determinedLocalNode.getName());

            Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
            TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, determinedLocalNode.getName(), primaryAndBackupNodesForData);
            LOG.debug(String.format("Scheduled a prefetching for - %s, %s at %s", metadata.getURI().toUri().toASCIIString(), hash, determinedLocalNode.getName()));
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
        RecipeChunk chunk = recipe.getChunk(hash);
        
        LOG.debug(String.format("Scheduling a prefetching - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));
        
        if(chunk == null) {
            throw new IllegalArgumentException(String.format("cannot find recipe chunk for hash %s", hash));
        }
        
        try {
            StargateService stargateService = getStargateService();

            // check local recipes
            RecipeManager recipeManager = stargateService.getRecipeManager();
            
            LOG.debug(String.format("Checking local recipes for a prefetching for - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));
            
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
                    TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, nodeName, nodeNames);
                    LOG.debug(String.format("Found a local recipe (%s) for - %s, %s at %s", localMetadata.getURI().toUri().toASCIIString(), metadata.getURI().toUri().toASCIIString(), hash, nodeName));
                    return new PendingPrefetchSchedule(assignment);
                }
            }
            
            LOG.debug(String.format("Checking a pending request for a prefetching for - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));
            
            // check cache and go remote
            byte[] existingData = (byte[]) this.dataChunkCacheStore.get(hash);
            if(existingData != null) {
                DataChunkCache existingCache = DataChunkCache.fromBytes(existingData);
                if(existingCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PENDING) {
                    Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
                    TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, existingCache.getTransferNode(), primaryAndBackupNodesForData);
                    LOG.debug(String.format("Found a pending prefetch schedule for - %s, %s at %s", metadata.getURI().toUri().toASCIIString(), hash, existingCache.getTransferNode()));
                    return new PendingPrefetchSchedule(assignment);
                } else if(existingCache.getType() == DataChunkCacheType.DATA_CHUNK_CACHE_PRESENT) {
                    Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
                    String primaryNodeForData = primaryAndBackupNodesForData.iterator().next();
                    TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, primaryNodeForData, primaryAndBackupNodesForData);
                    LOG.debug(String.format("Found a local cache for - %s, %s at %s", metadata.getURI().toUri().toASCIIString(), hash, primaryNodeForData));
                    return new PendingPrefetchSchedule(assignment);
                }
            }
            
            LOG.debug(String.format("Making a pending request for a prefetching for - %s, %s", metadata.getURI().toUri().toASCIIString(), hash));
            
            // determine where to copy 
            Node determinedLocalNode = this.transferLayoutAlgorithm.determineLocalNode(localCluster, recipe, hash);

            // make the pending chunk cache
            DataChunkCache newDataChunkCache = new DataChunkCache(DataChunkCacheType.DATA_CHUNK_CACHE_PENDING, hash, 1, determinedLocalNode.getName(), null);
            newDataChunkCache.addWaitingNode(determinedLocalNode.getName());
            
            Collection<String> primaryAndBackupNodesForData = this.dataChunkCacheStore.getPrimaryAndBackupNodesForData(hash);
            TransferAssignment assignment = new TransferAssignment(metadata.getURI(), hash, determinedLocalNode.getName(), primaryAndBackupNodesForData);
            return new PendingPrefetchSchedule(assignment, newDataChunkCache);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public void processPendingPrefetchSchedules(Collection<PendingPrefetchSchedule> pendingPrefetchSchedules) throws IOException, DriverNotInitializedException {
        if(pendingPrefetchSchedules == null) {
            throw new IllegalArgumentException("pendingPrefetchSchedules is null");
        }
        
        LOG.debug("Processing pending prefetching schedules");
        List<PrefetchTransferEvent> events = new ArrayList<PrefetchTransferEvent>();
        
        for(PendingPrefetchSchedule schedule : pendingPrefetchSchedules) {
            TransferAssignment transferAssignment = schedule.getTransferAssignment();
            DataChunkCache dataChunkCache = schedule.getDataChunkCache();
            
            LOG.debug(String.format("Putting a pending request for a prefetching for - %s, %s", transferAssignment.getDataObjectURI().toUri().toASCIIString(), dataChunkCache.getHash()));
            this.dataChunkCacheStore.put(dataChunkCache.getHash(), dataChunkCache.toBytes());
            
            PrefetchTransferEvent event = new PrefetchTransferEvent(transferAssignment.getDataObjectURI(), transferAssignment.getHash(), transferAssignment.getTransferNode());
            events.add(event);
        }
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
        
        // send to remote
        LOG.debug("Sending a prefetching request");
        raiseEventForPrefetchTransfer(events);
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
            LOG.error("Manager is not instantiated", ex);
        }
    }
    
    private void raiseEventForPrefetchTransfer(Collection<PrefetchTransferEvent> events) throws IOException, DriverNotInitializedException {
        List<StargateEvent> stargateEvents = new ArrayList<StargateEvent>();
        
        try {
            StargateService stargateService = getStargateService();
            EventManager eventManager = stargateService.getEventManager();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Node localNode = clusterManager.getLocalNode();
            
            for(PrefetchTransferEvent event : events) {
                TransferEvent transferEvent = new TransferEvent(TransferEventType.TRANSFER_EVENT_TYPE_PREFETCH, event.getURI(), event.getHash());
                StargateEvent stargateEvent = new StargateEvent(StargateEventType.STARGATE_EVENT_TYPE_TRANSPORT, event.getNodeName(), localNode.getName(), transferEvent.toJson());
                stargateEvents.add(stargateEvent);
            }
            
            eventManager.raiseEvents(stargateEvents);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
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
            LOG.error("Manager is not instantiated", ex);
        }
    }
    
    private void processPrefetchEvent(DataObjectURI uri, String hash) {
        PrefetchTask task = new PrefetchTask(this, uri, hash);
        this.prefetchThreadPool.execute(task);
    }
    
    private void processTransferEvent(TransferEvent event) throws IOException, DriverNotInitializedException {
        switch(event.getEventType()) {
            case TRANSFER_EVENT_TYPE_ONDEMAND:
                LOG.debug(String.format("On-demand transfser is requested : %s - %s", event.getURI().toUri().toASCIIString(), event.getHash()));
                break;
            case TRANSFER_EVENT_TYPE_PREFETCH:
                LOG.debug(String.format("A prefetching is requested : %s - %s", event.getURI().toUri().toASCIIString(), event.getHash()));
                processPrefetchEvent(event.getURI(), event.getHash());
                break;
            case TRANSFER_EVENT_TYPE_COMPLETE:
                LOG.debug(String.format("Transfser is finished : %s - %s", event.getURI().toUri().toASCIIString(), event.getHash()));
                TransferReference reference = this.waitObjects.get(event.getHash());
                if(reference != null) {
                    reference.finishTransfer();
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
