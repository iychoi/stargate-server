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
    private AbstractKeyValueStore remoteRecipeCacheStore; // <DataObjectURI, Recipe>
    private AbstractKeyValueStore blockCacheStore; // <String, byte[]> key = hashstring
    private AbstractKeyValueStore responsibleNodeMappingStore; // <String, ResponsibleNodeMapping>
    private AbstractQueue ondemandTransferQueue; // <TransferEvent>
    private AbstractQueue prefetchTransferQueue; // <TransferEvent>
    private AbstractKeyValueStore transferInQueueStore; //<String, TransferAssignment> key = hashstring
    private AbstractKeyValueStore workloadStore; //<String, NodeWorkload> key = nodeName
    protected long lastUpdateTime;
    
    private static final String REMOTE_DIRECTORY_CACHE_STORE = "remote_dir_cache";
    private static final String REMOTE_RECIPE_CACHE_STORE = "remote_recipe_cache";
    private static final String BLOCK_CACHE_STORE = "block_cache";
    private static final String RESPONSIBLE_NODE_MAPPING_STORE = "responsible_node_mapping";
    private static final String ONDEMAND_TRANSFER_QUEUE = "ondemand_transfer_queue";
    private static final String PREFETCH_TRANSFER_QUEUE = "prefetch_transfer_queue";
    private static final String TRANSFER_IN_QUEUE_STORE = "transfer_in_queue";
    private static final String WORKLOAD_STORE = "local_workload";
    
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
    
    private void safeInitRemoteRecipeCacheStore() throws IOException {
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
    
    private void safeInitBlockCacheStore() throws IOException {
        if(this.blockCacheStore == null) {
            try {
                StargateService stargateService = getStargateService();
                DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                this.blockCacheStore = keyValueStoreManager.getDriver().getKeyValueStore(BLOCK_CACHE_STORE, byte[].class, EnumDataStoreProperty.DATASTORE_PROP_PERSISTENT_DISTRIBUTED);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
    
    private void safeInitResponsibleNodeMappingStore() throws IOException {
        if(this.responsibleNodeMappingStore == null) {
            try {
                StargateService stargateService = getStargateService();
                DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                this.responsibleNodeMappingStore = keyValueStoreManager.getDriver().getKeyValueStore(RESPONSIBLE_NODE_MAPPING_STORE, ResponsibleNodeMapping.class, EnumDataStoreProperty.DATASTORE_PROP_VOLATILE_REPLICATED, TimeUnit.MINUTES, 5);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
    
    private void safeInitTransferQueueStore() throws IOException {
        if(this.ondemandTransferQueue == null) {
            try {
                StargateService stargateService = getStargateService();
                DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                this.ondemandTransferQueue = keyValueStoreManager.getDriver().getQueue(ONDEMAND_TRANSFER_QUEUE, TransferEvent.class, EnumDataStoreProperty.DATASTORE_PROP_VOLATILE_REPLICATED);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
        
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
    
    private void safeInitWorkloadStore() throws IOException {
        if(this.workloadStore == null) {
            try {
                StargateService stargateService = getStargateService();
                DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                this.workloadStore = keyValueStoreManager.getDriver().getKeyValueStore(WORKLOAD_STORE, NodeWorkload.class, EnumDataStoreProperty.DATASTORE_PROP_VOLATILE_REPLICATED);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
    
    public Node getResponsibleRemoteNode(Cluster remoteCluster) throws IOException {
        if(remoteCluster == null) {
            throw new IllegalArgumentException("remoteCluster is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();
            
            Node localNode = clusterManager.getLocalNode();
            ResponsibleNodeMapping responsibleRemoteNodeMappings = getResponsibleRemoteNodeMappings(remoteCluster);
            
            NodeMapping mapping = responsibleRemoteNodeMappings.getNodeMapping(localNode.getName());
            Collection<String> targetNodeNames = mapping.getTargetNodeNames();
            
            for(String targetNodeName : targetNodeNames) {
                Node remoteNode = remoteCluster.getNode(targetNodeName);
                if(remoteNode != null) {
                    return remoteNode;
                }
            }
            throw new IOException("Could not find a responsible remote node");
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public synchronized ResponsibleNodeMapping getResponsibleRemoteNodeMappings(Cluster remoteCluster) throws IOException {
        if(remoteCluster == null) {
            throw new IllegalArgumentException("remoteCluster is null");
        }
        
        if(remoteCluster.getNodeNum() == 0) {
            throw new IOException(String.format("There's no node in a remote cluster : %s", remoteCluster.getName()));
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitResponsibleNodeMappingStore();
        
        ResponsibleNodeMapping mappings = (ResponsibleNodeMapping) this.responsibleNodeMappingStore.get(remoteCluster.getName());
        if(mappings == null) {
            // make a new mapping
            try {
                StargateService stargateService = getStargateService();
                ClusterManager clusterManager = stargateService.getClusterManager();

                Cluster localCluster = clusterManager.getLocalCluster();
                
                List<Node> localNodes = new ArrayList<Node>();
                List<Node> remoteNodes = new ArrayList<Node>();

                localNodes.addAll(localCluster.getNodes());
                int localNodeNum = localNodes.size();
                remoteNodes.addAll(remoteCluster.getNodes());
                int remoteNodeNum = remoteNodes.size();
                
                // round-robin mapping
                mappings = new ResponsibleNodeMapping(remoteCluster.getName());
                
                if(localNodeNum >= remoteNodeNum) {
                    for(int i=0;i<localNodeNum;i++) {
                        Node localNode = localNodes.get(i);
                        Node remoteNode = remoteNodes.get(i % remoteNodeNum);

                        LOG.debug(String.format("Determined a responsible remote node of %s -> %s", localNode.getName(), remoteNode.getName()));

                        mappings.addNodeMapping(localNode.getName(), remoteNode.getName());
                    }
                } else {
                    for(int i=0;i<remoteNodeNum;i++) {
                        Node localNode = localNodes.get(i % localNodeNum);
                        Node remoteNode = remoteNodes.get(i);

                        LOG.debug(String.format("Determined a responsible remote node of %s -> %s", localNode.getName(), remoteNode.getName()));

                        mappings.addNodeMapping(localNode.getName(), remoteNode.getName());
                    }
                }
                
                this.responsibleNodeMappingStore.put(remoteCluster.getName(), mappings);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
        
        return mappings;
    }
    
    public synchronized byte[] getDataChunkCache(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitBlockCacheStore();
        
        return (byte[]) this.blockCacheStore.get(hash);
    }
    
    public synchronized boolean hasDataChunkCache(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitBlockCacheStore();
        
        return this.blockCacheStore.containsKey(hash);
    }
    
    public synchronized void clearDataChunkCaches() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitBlockCacheStore();
        
        this.blockCacheStore.clear();
    }
    
    public synchronized void addDataChunkCache(String hash, byte[] data) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(data == null) {
            throw new IllegalArgumentException("data is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitBlockCacheStore();
        
        this.blockCacheStore.putIfAbsent(hash, data);
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
    }
    
    public synchronized void removeDataChunkCache(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitBlockCacheStore();
        
        this.blockCacheStore.remove(hash);
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
    }
    
    public Cluster getRemoteCluster(Cluster remoteCluster) throws IOException {
        if(remoteCluster == null) {
            throw new IllegalArgumentException("remoteCluster is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        Node remoteNode = getResponsibleRemoteNode(remoteCluster);
        if(remoteNode == null) {
            throw new IOException(String.format("cannot determine a remote node for a remote cluster %s", remoteCluster.getName()));
        }

        return getRemoteCluster(remoteNode);
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
    
    public void transferDataChunk(Node remoteNode, String hash) throws IOException {
        if(remoteNode == null) {
            throw new IllegalArgumentException("remoteNode is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        AbstractTransportDriver driver = getDriver();
        AbstractTransportClient client = driver.getClient(remoteNode);
        InputStream dataChunkInputStream = client.getDataChunk(hash);
        if (dataChunkInputStream == null) {
            throw new IOException("dataChunkInputStream is null");
        }
        
        // check if there is unaccessed data
        // if so, read fully and cache
        byte[] buffer = new byte[4096];
        ByteArrayOutputStream cacheData = new ByteArrayOutputStream();
        
        int read = 0;
        while((read = dataChunkInputStream.read(buffer, 0, 4096)) > 0) {
            cacheData.write(buffer, 0, read);
        }
        
        cacheData.close();
        byte[] cacheDataBytes = cacheData.toByteArray();
        dataChunkInputStream.close();
        cacheData.close();
        
        // put to the cache
        addDataChunkCache(hash, cacheDataBytes);
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
            RecipeChunk chunk = recipe.getChunk(hash);
            Collection<Integer> nodeIDs = chunk.getNodeIDs();
            Collection<String> nodeNames = recipe.getNodeNames(nodeIDs);
            
            List<Node> remoteNodes = new ArrayList<Node>();
            for(String nodeName : nodeNames) {
                Node remoteNode = remoteCluster.getNode(nodeName);
                remoteNodes.add(remoteNode);
            }
            
            Node remoteNode = determineRemoteNode(remoteNodes, recipe, hash);
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
        
        safeInitWorkloadStore();
        
        // step 1. check cache
        if(hasDataChunkCache(hash)) {
            byte[] dataChunkCache = getDataChunkCache(hash);
            if (dataChunkCache != null) {
                ByteArrayInputStream bais = new ByteArrayInputStream(dataChunkCache);
                return bais;
            }
        }
        
        // step 2. check local recipes
        try {
            StargateService stargateService = getStargateService();
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
        
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Node localNode = clusterManager.getLocalNode();
            
            NodeWorkload workload = (NodeWorkload) this.workloadStore.get(localNode.getName());
            if(workload == null) {
                workload = new NodeWorkload(localNode.getName());
            }
            
            workload.increaseWorkload(1);
            this.workloadStore.put(localNode.getName(), workload);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
        
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
    
    public TransferAssignment scheduleOndemandTransfer(Node localNode, DataObjectURI uri, String hash) throws IOException {
        if(localNode == null) {
            throw new IllegalArgumentException("localNode is null");
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
        return scheduleOndemandTransfer(localNode, recipe, hash);
    }

    public TransferAssignment scheduleOndemandTransfer(Node localNode, Recipe recipe, String hash) throws IOException {
        if(localNode == null) {
            throw new IllegalArgumentException("localNode is null");
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
        
        DataObjectMetadata metadata = recipe.getMetadata();
        RecipeChunk chunk = recipe.getChunk(hash);
        
        Collection<Integer> nodeIDs = chunk.getNodeIDs();
        Collection<String> dataSourceNodeNames = recipe.getNodeNames(nodeIDs);
        
        if(chunk == null) {
            throw new IllegalArgumentException(String.format("cannot find recipe chunk for hash %s", hash));
        }
        
        TransferAssignment existingAssignment = (TransferAssignment) this.transferInQueueStore.get(hash);
        if(existingAssignment != null) {
            // if there's a prefetch transfer is scheduled, add a new ondemand transfer schedule to prioritize
            TransferEventType eventType = existingAssignment.getEventType();
            if(eventType == TransferEventType.TRANSFER_EVENT_TYPE_ONDEMAND) {
                // exist already
                return existingAssignment;
            }
        }
        
        long timestamp = DateTimeUtils.getTimestamp();
        long order = this.ondemandTransferQueue.size();
        
        TransferEvent event = new TransferEvent(TransferEventType.TRANSFER_EVENT_TYPE_ONDEMAND, metadata.getURI(), chunk.getOffset(), chunk.getLength(), hash, dataSourceNodeNames, localNode.getName());
        this.ondemandTransferQueue.enqueue(event);
        
        TransferAssignment assignment = new TransferAssignment(TransferEventType.TRANSFER_EVENT_TYPE_ONDEMAND, event, timestamp, order);
        this.transferInQueueStore.put(hash, assignment);
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
        return assignment;
    }
    
    private Node determineRemoteNode(Collection<Node> remoteNodes, Recipe recipe, String hash) throws IOException {
        if(remoteNodes == null || remoteNodes.isEmpty()) {
            throw new IllegalArgumentException("remoteNodes is null or empty");
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
        
        return determineRemoteNodeStaticDataLocality(remoteNodes, recipe, hash);
    }
    
    private Node determineLocalNode(Collection<Node> localNodes, Recipe recipe, String hash) throws IOException {
        if(localNodes == null || localNodes.isEmpty()) {
            throw new IllegalArgumentException("localNodes is null or empty");
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
        
        return determineLocalNodeStaticRR(localNodes, recipe, hash);
    }
    
    private Node determineRemoteNodeStaticDataLocality(Collection<Node> remoteNodes, Recipe recipe, String hash) throws IOException {
        if(remoteNodes == null || remoteNodes.isEmpty()) {
            throw new IllegalArgumentException("remoteNodes is null or empty");
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
        
        for(Node remoteNode : remoteNodes) {
            return remoteNode;
        }
        return null;
    }
    
    private Node determineLocalNodeStaticRR(Collection<Node> localNodes, Recipe recipe, String hash) throws IOException {
        if(localNodes == null || localNodes.isEmpty()) {
            throw new IllegalArgumentException("localNodes is null or empty");
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
        
        safeInitWorkloadStore();
        
        Node lowestWorkloadNode = null;
        NodeWorkload lowestWorkload = null;
        for(Node node : localNodes) {
            NodeWorkload workload = (NodeWorkload) this.workloadStore.get(node.getName());
            if(workload == null) {
                workload = new NodeWorkload(node.getName(), 0);
                
                lowestWorkloadNode = node;
                lowestWorkload = workload;
            } else {
                if(lowestWorkloadNode == null) {
                    lowestWorkloadNode = node;
                    lowestWorkload = workload;
                } else {
                    if(lowestWorkload.getWorkload() > workload.getWorkload()) {
                        lowestWorkloadNode = node;
                        lowestWorkload = workload;
                    }
                }
            }
            
            if(lowestWorkload.getWorkload() == 0) {
                // we found idle node
                break;
            }
        }
        
        lowestWorkload.increaseWorkload(1); // we assign 1 transfer
        this.workloadStore.put(lowestWorkloadNode.getName(), lowestWorkload);
        return lowestWorkloadNode;
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
            Collection<Node> localNodes = localCluster.getNodes();
            
            Recipe recipe = getRecipe(uri);
            return schedulePrefetch(localNodes, recipe, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public TransferAssignment schedulePrefetch(Collection<Node> localNodes, DataObjectURI uri, String hash) throws IOException {
        if(localNodes == null || localNodes.isEmpty()) {
            throw new IllegalArgumentException("localNodes is null or empty");
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
        return schedulePrefetch(localNodes, recipe, hash);
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
            Collection<Node> localNodes = localCluster.getNodes();
            
            return schedulePrefetch(localNodes, recipe, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public TransferAssignment schedulePrefetch(Collection<Node> localNodes, Recipe recipe, String hash) throws IOException {
        if(localNodes == null || localNodes.isEmpty()) {
            throw new IllegalArgumentException("localNodes is null or empty");
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
        
        DataObjectMetadata metadata = recipe.getMetadata();
        RecipeChunk chunk = recipe.getChunk(hash);
        
        Collection<Integer> nodeIDs = chunk.getNodeIDs();
        Collection<String> dataSourceNodeNames = recipe.getNodeNames(nodeIDs);
        
        if(chunk == null) {
            throw new IllegalArgumentException(String.format("cannot find recipe chunk for hash %s", hash));
        }
        
        TransferAssignment existingAssignment = (TransferAssignment) this.transferInQueueStore.get(hash);
        if(existingAssignment != null) {
            TransferEventType eventType = existingAssignment.getEventType();
            if(eventType == TransferEventType.TRANSFER_EVENT_TYPE_ONDEMAND || eventType == TransferEventType.TRANSFER_EVENT_TYPE_PREFETCH) {
                // exist already
                return existingAssignment;
            }
        }
        
        long timestamp = DateTimeUtils.getTimestamp();
        long order = this.ondemandTransferQueue.size() + this.prefetchTransferQueue.size();
        
        // determine where to copy 
        Node determinedLocalNode = determineLocalNode(localNodes, recipe, hash);
        
        TransferEvent event = new TransferEvent(TransferEventType.TRANSFER_EVENT_TYPE_PREFETCH, metadata.getURI(), chunk.getOffset(), chunk.getLength(), hash, dataSourceNodeNames, determinedLocalNode.getName());
        this.prefetchTransferQueue.enqueue(event);
        
        TransferAssignment assignment = new TransferAssignment(TransferEventType.TRANSFER_EVENT_TYPE_PREFETCH, event, timestamp, order);
        this.transferInQueueStore.put(hash, assignment);
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
        return assignment;
    }
    
    public Directory getDirectory(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRemoteDirectoryCacheStore();
        
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

                Node remoteNode = getResponsibleRemoteNode(remoteCluster);
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
            
            Node remoteNode = getResponsibleRemoteNode(remoteCluster);
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
                
                Node remoteNode = getResponsibleRemoteNode(remoteCluster);
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
