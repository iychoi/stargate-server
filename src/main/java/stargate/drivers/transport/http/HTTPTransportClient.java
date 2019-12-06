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
package stargate.drivers.transport.http;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.dataobject.Directory;
import stargate.commons.recipe.Recipe;
import stargate.commons.restful.RestfulClient;
import stargate.commons.service.FSServiceInfo;
import stargate.commons.transport.AbstractTransportClient;
import stargate.commons.transport.TransportServiceInfo;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.utils.PathUtils;

/**
 *
 * @author iychoi
 */
public class HTTPTransportClient extends AbstractTransportClient {

    private static final Log LOG = LogFactory.getLog(HTTPTransportClient.class);
    
    private URI serviceUri;
    private String username;
    private String password;
    private RestfulClient restfulClient;
    private long connectionEstablishedTime;
    private long lastActiveTime;
    private boolean connected = false;
    
    public HTTPTransportClient(TransportServiceInfo serviceInfo, String username, String password) throws IOException {
        if(serviceInfo == null) {
            throw new IllegalArgumentException("serviceInfo is null");
        }
        
        // username and password can be null
        
        this.serviceUri = serviceInfo.getServiceURI();
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
            LOG.debug("Connected to " + this.serviceUri.toString());
        }
    }
    
    @Override
    public synchronized void disconnect() {
        if(this.connected) {
            this.restfulClient.close();
            this.connected = false;
            LOG.debug("Disconnected to " + this.serviceUri.toString());
        }
    }
    
    @Override
    public synchronized boolean isConnected() {
        return this.connected;
    }
    
    private String makeAPIPath(String path) {
        return PathUtils.concatPath(HTTPTransportRestfulConstants.API_PATH, path);
    }
    
    private String makeAPIPath(String path1, String path2) {
        String api_path = PathUtils.concatPath(HTTPTransportRestfulConstants.API_PATH, path1);
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
        String url = makeAPIPath(HTTPTransportRestfulConstants.API_CHECK_LIVE_PATH);
        Boolean live = (Boolean) this.restfulClient.get(url);

        updateLastActivetime();
        return live;
    }

        @Override
    public FSServiceInfo getFSServiceInfo() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/fssvcinfo
        String url = makeAPIPath(HTTPTransportRestfulConstants.API_GET_FS_SERVICE_INFO_PATH);
        FSServiceInfo info = (FSServiceInfo) this.restfulClient.get(url);

        updateLastActivetime();
        return info;
    }
    
    @Override
    public Cluster getLocalCluster() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/lcluster
        String url = makeAPIPath(HTTPTransportRestfulConstants.API_GET_LOCAL_CLUSTER_PATH);
        Cluster cluster = (Cluster) this.restfulClient.get(url);

        updateLastActivetime();
        return cluster;
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
        String url = makeAPIPath(HTTPTransportRestfulConstants.API_GET_METADATA_PATH, uri.getPath());
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
        String url = makeAPIPath(HTTPTransportRestfulConstants.API_LIST_METADATA_PATH, uri.getPath());
        DataObjectMetadata[] metadataList = (DataObjectMetadata[]) this.restfulClient.get(url);

        updateLastActivetime();
        return Arrays.asList(metadataList);
    }
    
    @Override
    public Directory getDirectory(DataObjectURI uri) throws FileNotFoundException, IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/directory/path/to/resource
        String url = makeAPIPath(HTTPTransportRestfulConstants.API_GET_DIRECTORY_PATH, uri.getPath());
        Directory directory = (Directory) this.restfulClient.get(url);

        updateLastActivetime();
        return directory;
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
        String url = makeAPIPath(HTTPTransportRestfulConstants.API_GET_RECIPE_PATH, uri.getPath());
        Recipe recipe = (Recipe) this.restfulClient.get(url);

        updateLastActivetime();
        return recipe;
    }
    
    @Override
    public InputStream getDataChunk(String hash) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        // URL pattern = http://xxx.xxx.xxx.xxx/api/data/hash
        String url = makeAPIPath(HTTPTransportRestfulConstants.API_GET_DATA_CHUNK_PATH, hash);
        
        InputStream is = this.restfulClient.download(url);
        updateLastActivetime();
        return is;
    }
}
