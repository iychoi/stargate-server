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
package stargate.drivers.userinterface.http;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.recipe.Recipe;
import stargate.commons.restful.RestfulClient;
import stargate.commons.service.FSServiceInfo;
import stargate.commons.statistics.StatisticsEntry;
import stargate.commons.statistics.StatisticsType;
import stargate.commons.transport.TransferAssignment;
import stargate.commons.userinterface.AbstractUserInterfaceClient;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.utils.PathUtils;

/**
 *
 * @author iychoi
 */
public class HTTPUserInterfaceClient extends AbstractUserInterfaceClient {

    private URI serviceUri;
    private String username;
    private String password;
    private RestfulClient restfulClient;
    private long connectionEstablishedTime;
    private long lastActiveTime;
    private boolean connected = false;
    
    public HTTPUserInterfaceClient(URI serviceURI, String username, String password) throws IOException {
        if(serviceURI == null) {
            throw new IllegalArgumentException("serviceURI is null");
        }
        
        // username and password can be null
        
        this.serviceUri = serviceURI;
        this.username = username;
        this.password = password;
        this.connected = false;
    }
    
    @Override
    public synchronized void connect() throws IOException {
        if(!this.connected) {
            this.restfulClient = new RestfulClient(this.serviceUri, this.username, this.password);

            this.connectionEstablishedTime = DateTimeUtils.getTimestamp();
            this.lastActiveTime = this.connectionEstablishedTime;
            this.connected = true;
        }
    }
    
    @Override
    public synchronized void disconnect() {
        if(this.connected) {
            this.restfulClient.close();
            this.connected = false;
        }
    }
    
    @Override
    public boolean isConnected() {
        return this.connected;
    }
    
    private String makeAPIPath(String path) {
        return PathUtils.concatPath(HTTPUserInterfaceRestfulConstants.API_PATH, path);
    }
    
    private String makeAPIPath(String path1, String path2) {
        String api_path = PathUtils.concatPath(HTTPUserInterfaceRestfulConstants.API_PATH, path1);
        return PathUtils.concatPath(api_path, path2);
    }
    
    @Override
    public URI getServiceURI() {
        return this.serviceUri;
    }
    
    @Override
    public long getConnectionEstablishedTime() {
        return this.connectionEstablishedTime;
    }
    
    @Override
    public long getLastActiveTime() {
        return this.lastActiveTime;
    }
    
    private void updateLastActivetime() {
        this.lastActiveTime = DateTimeUtils.getTimestamp();
    }
    
    @Override
    public boolean isLive() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/live
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_CHECK_LIVE_PATH);
        Boolean live = (Boolean) this.restfulClient.get(url);

        updateLastActivetime();
        return live;
    }
    
    @Override
    public String getServiceConfig() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/svcconfig
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_SERVICE_CONFIG_PATH);
        String config = (String) this.restfulClient.get(url);

        updateLastActivetime();
        return config;
    }
    
    @Override
    public FSServiceInfo getFSServiceInfo() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/fssvcinfo
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_FS_SERVICE_INFO_PATH);
        FSServiceInfo info = (FSServiceInfo) this.restfulClient.get(url);

        updateLastActivetime();
        return info;
    }

    @Override
    public Cluster getCluster(String name) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/cluster/clustername
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_CLUSTER_PATH, name);
        Cluster cluster = (Cluster) this.restfulClient.get(url);

        updateLastActivetime();
        return cluster;
    }
    
    @Override
    public Cluster getLocalCluster() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/lcluster
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_LOCAL_CLUSTER_PATH);
        Cluster cluster = (Cluster) this.restfulClient.get(url);

        updateLastActivetime();
        return cluster;
    }
    
    public static final String API_CHECK_ACTIVE_CLUSTER_PATH = "active";
    @Override
    public void activateCluster() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/activate
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_ACTIVATE_CLUSTER_PATH);
        Boolean response = (Boolean) this.restfulClient.post(url, null);

        updateLastActivetime();
    }
    
    @Override
    public boolean isClusterActive() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/active
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_CHECK_ACTIVE_CLUSTER_PATH);
        Boolean active = (Boolean) this.restfulClient.get(url);

        updateLastActivetime();
        return active;
    }
    
    @Override
    public Node getLocalNode() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/lnode
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_LOCAL_NODE_PATH);
        Node node = (Node) this.restfulClient.get(url);

        updateLastActivetime();
        return node;
    }
    
    @Override
    public Node getLeaderNode() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/leadernode
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_LEADER_NODE_PATH);
        Node node = (Node) this.restfulClient.get(url);

        updateLastActivetime();
        return node;
    }
    
    @Override
    public Cluster getRemoteCluster(String name) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/rcluster/clustername
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_REMOTE_CLUSTER_PATH, name);
        Cluster cluster = (Cluster) this.restfulClient.get(url);

        updateLastActivetime();
        return cluster;
    }
    
    @Override
    public Collection<String> listRemoteClusters() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/lrcluster
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_LIST_REMOTE_CLUSTERS_PATH);
        String[] clusters = (String[]) this.restfulClient.get(url);

        updateLastActivetime();
        return Arrays.asList(clusters);
    }
    
    @Override
    public Collection<Cluster> getRemoteClusters() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/grclusters
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_REMOTE_CLUSTERS_PATH);
        Cluster[] clusters = (Cluster[]) this.restfulClient.get(url);

        updateLastActivetime();
        return Arrays.asList(clusters);
    }
    
    @Override
    public void addRemoteCluster(Cluster cluster) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(cluster == null) {
            throw new IllegalArgumentException("cluster is null");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/rcluster
        // form param = object
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_ADD_REMOTE_CLUSTER_PATH);
        Boolean result = (Boolean) this.restfulClient.put(url, cluster);

        updateLastActivetime();
    }

    @Override
    public void removeRemoteCluster(String name) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/rcluster/name
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_REMOVE_REMOTE_CLUSTER_PATH, name);
        Boolean response = (Boolean) this.restfulClient.delete(url);

        updateLastActivetime();
    }
    
    @Override
    public DataObjectMetadata getDataObjectMetadata(DataObjectURI uri) throws FileNotFoundException, IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/metadata/path/to/resource
        String path = PathUtils.concatPath(uri.getClusterName(), uri.getPath());
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_METADATA_PATH, path);
        DataObjectMetadata metadata = (DataObjectMetadata) this.restfulClient.get(url);

        updateLastActivetime();
        return metadata;
    }
    
    @Override
    public Collection<DataObjectMetadata> listDataObjectMetadata(DataObjectURI uri) throws FileNotFoundException, IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/lmetadata/path/to/resource
        String path = PathUtils.concatPath(uri.getClusterName(), uri.getPath());
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_LIST_METADATA_PATH, path);
        DataObjectMetadata[] metadataList = (DataObjectMetadata[]) this.restfulClient.get(url);

        updateLastActivetime();
        return Arrays.asList(metadataList);
    }
    
    @Override
    public Recipe getRecipe(DataObjectURI uri) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/recipe/path/to/resource
        String path = PathUtils.concatPath(uri.getClusterName(), uri.getPath());
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_RECIPE_PATH, path);
        Recipe recipe = (Recipe) this.restfulClient.get(url);

        updateLastActivetime();
        return recipe;
    }
    
    @Override
    public Collection<String> listRecipes() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/lrecipe
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_LIST_RECIPES_PATH);
        String[] recipes = (String[]) this.restfulClient.get(url);

        updateLastActivetime();
        return Arrays.asList(recipes);
    }
    
    @Override
    public void removeRecipe(DataObjectURI uri) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/recipe/path/to/file
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_REMOVE_RECIPE_PATH, uri.getPath());
        Boolean result = (Boolean) this.restfulClient.delete(url);

        updateLastActivetime();
    }
    
    @Override
    public void syncRecipes() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/srecipes
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_SYNC_RECIPES_PATH);
        Boolean response = (Boolean) this.restfulClient.post(url, null);

        updateLastActivetime();
    }

    @Override
    public InputStream getDataChunk(DataObjectURI uri, String hash) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/data/path/hash
        String path = PathUtils.concatPath(uri.getClusterName(), uri.getPath());
        String pathHash = PathUtils.concatPath(path, hash);
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_DATA_CHUNK_PATH, pathHash);
        InputStream is = this.restfulClient.download(url);

        updateLastActivetime();
        return is;
    }
    
    @Override
    public TransferAssignment schedulePrefetch(DataObjectURI uri, String hash) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/prefetch/path/hash
        String path = PathUtils.concatPath(uri.getClusterName(), uri.getPath());
        String pathHash = PathUtils.concatPath(path, hash);
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_SCHEDULE_PREFETCH_PATH, pathHash);
        TransferAssignment assignment = (TransferAssignment) this.restfulClient.post(url, null);

        updateLastActivetime();
        return assignment;
    }
    
    @Override
    public Recipe getRemoteRecipeWithTransferSchedule(DataObjectURI uri) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/rrecipewts/path/to/file
        String path = PathUtils.concatPath(uri.getClusterName(), uri.getPath());
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_REMOTE_RECIPE_WITH_TRANSFER_SCHEDULE_PATH, path);
        Recipe recipe = (Recipe) this.restfulClient.post(url, null);

        updateLastActivetime();
        return recipe;
    }

    @Override
    public DataExportEntry getDataExportEntry(DataObjectURI uri) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/export/path/to/file
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_DATA_EXPORT_ENTRY_PATH, uri.getPath());
        DataExportEntry entry = (DataExportEntry) this.restfulClient.get(url);

        updateLastActivetime();
        return entry;
    }
    
    @Override
    public Collection<String> listDataExportEntries() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/lexport
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_LIST_DATA_EXPORT_ENTRIES_PATH);
        String[] entries = (String[]) this.restfulClient.get(url);

        updateLastActivetime();
        return Arrays.asList(entries);
    }

    @Override
    public Collection<DataExportEntry> getDataExportEntries() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/exports
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_DATA_EXPORT_ENTRIES_PATH);
        DataExportEntry[] entries = (DataExportEntry[]) this.restfulClient.get(url);

        updateLastActivetime();
        return Arrays.asList(entries);
    }

    @Override
    public void addDataExportEntry(DataExportEntry entry) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/exports
        // form param = object
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_ADD_DATA_EXPORT_ENTRY_PATH);
        Boolean result = (Boolean) this.restfulClient.put(url, entry);

        updateLastActivetime();
    }

    @Override
    public void removeDataExportEntry(DataObjectURI uri) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/export/path/to/file
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_REMOVE_DATA_EXPORT_ENTRY_PATH, uri.getPath());
        Boolean result = (Boolean) this.restfulClient.delete(url);

        updateLastActivetime();
    }
    
    @Override
    public Collection<String> listDataSources() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/lsources
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_LIST_DATA_SOURCES_PATH);
        String[] sources = (String[]) this.restfulClient.get(url);

        updateLastActivetime();
        return Arrays.asList(sources);
    }

    @Override
    public Collection<StatisticsEntry> getStatistics(StatisticsType type) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(type == null) {
            throw new IllegalArgumentException("type is null");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/stat/type
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_GET_STATISTICS_PATH, type.name());
        StatisticsEntry[] stats = (StatisticsEntry[]) this.restfulClient.get(url);

        updateLastActivetime();
        return Arrays.asList(stats);
    }

    @Override
    public void clearStatistics(StatisticsType type) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(type == null) {
            throw new IllegalArgumentException("type is null");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/stat/type
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_CLEAR_STATISTICS_PATH, type.name());
        Boolean result = (Boolean) this.restfulClient.delete(url);

        updateLastActivetime();
    }

    @Override
    public void clearAllStatistics() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/allstat
        String url = makeAPIPath(HTTPUserInterfaceRestfulConstants.API_CLEAR_ALL_STATISTICS_PATH);
        Boolean result = (Boolean) this.restfulClient.delete(url);

        updateLastActivetime();
    }
}
