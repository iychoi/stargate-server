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
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
import stargate.commons.keyvaluestore.AbstractKeyValueStore;
import stargate.commons.keyvaluestore.EnumKeyValueStoreProperty;
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
import stargate.managers.keyvaluestore.KeyValueStoreManager;
import stargate.managers.recipe.RecipeManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class TransportManager extends AbstractManager<AbstractTransportDriver> {

    private static final Log LOG = LogFactory.getLog(TransportManager.class);
    
    private static TransportManager instance;
    
    private AbstractKeyValueStore blockCacheStore; // <String, byte[]>
    protected long lastUpdateTime;
    
    private static final String BLOCK_CACHE_STORE = "bcache";
    
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
        return new TransportServiceInfo(driver.getClass().getCanonicalName(), serviceURI);
    }
    
    private synchronized void safeInitBlockCacheStore() throws IOException {
        if(this.blockCacheStore == null) {
            try {
                StargateService stargateService = getStargateService();
                KeyValueStoreManager keyValueStoreManager = stargateService.getKeyValueStoreManager();
                this.blockCacheStore = keyValueStoreManager.getDriver().getKeyValueStore(BLOCK_CACHE_STORE, byte[].class, EnumKeyValueStoreProperty.KEY_VALUE_STORE_PROP_PERSISTENT_DISTRIBUTED);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
    
    public Node getResponsibleRemoteNode(String remoteClusterName) throws IOException {
        if(remoteClusterName == null || remoteClusterName.isEmpty()) {
            throw new IllegalArgumentException("remoteClusterName is null or empty");
        }
        
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Cluster remoteCluster = clusterManager.getRemoteCluster(remoteClusterName);
            if(remoteCluster == null) {
                throw new IOException(String.format("remote cluster %s does not exist", remoteClusterName));
            }
            
            return getResponsibleRemoteNode(remoteCluster);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public Node getResponsibleRemoteNode(Cluster remoteCluster) throws IOException {
        if(remoteCluster == null) {
            throw new IllegalArgumentException("remoteCluster is null");
        }
        
        try {
            StargateService stargateService = getStargateService();
            ClusterManager clusterManager = stargateService.getClusterManager();
            Node localNode = clusterManager.getLocalNode();
            Cluster localCluster = clusterManager.getLocalCluster();
            return getResponsibleRemoteNode(remoteCluster, localCluster, localNode);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public Node getResponsibleRemoteNode(Cluster remoteCluster, Cluster localCluster, Node myNode) throws IOException {
        if(remoteCluster == null) {
            throw new IllegalArgumentException("remoteCluster is null");
        }
        
        if(localCluster == null) {
            throw new IllegalArgumentException("localCluster is null");
        }
        
        if(myNode == null) {
            throw new IllegalArgumentException("myNode is null");
        }
        
        Collection<Node> localNodes = localCluster.getNodes();
        Collection<Node> remoteNodes = remoteCluster.getNodes();
        
        // simple mapping
        int idxMyNode = 0;
        for(Node node : localNodes) {
            if(node.getName().equals(myNode.getName())) {
                // my node
                break;
            }
            idxMyNode++;
        }
        
        int idxRemoteNode = idxMyNode % remoteNodes.size();
        int idx = 0;
        for(Node node : remoteNodes) {
            if(idx == idxRemoteNode) {
                return node;
            }
            idx++;
        }
        return null;
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
    
    public InputStream getDataChunk(String clusterName, String hash) throws IOException, IOException, IOException {
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }
        
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
            
            return getDataChunk(remoteNode, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public InputStream getDataChunk(Node node, String hash) throws IOException {
        if(node == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
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
        AbstractTransportClient client = driver.getClient(node);
        InputStream dataChunkInputStream = client.getDataChunk(hash);
        if (dataChunkInputStream == null) {
            throw new IOException("dataChunkInputStream is null");
        }
        
        return new CacheableInputStream(this, dataChunkInputStream, hash);
    }
    
    public void reportNodeUnreachable(Node node) throws IOException {
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

    public boolean schedulePrefetch(DataObjectURI uri, String hash) {
        //TODO: implement this
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public Directory getDirectory(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
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
            
            return getDirectory(remoteNode, uri);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public Directory getDirectory(Node node, DataObjectURI uri) throws IOException {
        if(node == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        AbstractTransportDriver driver = getDriver();
        AbstractTransportClient client = driver.getClient(node);
        return client.getDirectory(uri);
    }

    public Collection<DataObjectMetadata> listDataObjectMetadata(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
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
    
    public Collection<DataObjectMetadata> listDataObjectMetadata(Node node, DataObjectURI uri) throws IOException {
        if(node == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        AbstractTransportDriver driver = getDriver();
        AbstractTransportClient client = driver.getClient(node);
        return client.listDataObjectMetadata(uri);
    }
    
    public Recipe getRecipe(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
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
            
            return getRecipe(remoteNode, uri);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public Recipe getRecipe(Node node, DataObjectURI uri) throws IOException {
        if(node == null) {
            throw new IllegalArgumentException("node is null");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        AbstractTransportDriver driver = getDriver();
        AbstractTransportClient client = driver.getClient(node);
        return client.getRecipe(uri);
    }
}
