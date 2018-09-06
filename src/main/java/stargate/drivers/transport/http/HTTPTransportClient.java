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

import com.sun.jersey.api.client.GenericType;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.dataobject.Directory;
import stargate.commons.recipe.Recipe;
import stargate.commons.restful.AuthenticationException;
import stargate.commons.restful.RestfulClient;
import stargate.commons.restful.RestfulException;
import stargate.commons.restful.RestfulResponse;
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
    
    HTTPTransportClient(TransportServiceInfo serviceInfo, String username, String password) throws IOException {
        if(serviceInfo == null) {
            throw new IllegalArgumentException("serviceInfo is null");
        }
        
        this.serviceUri = serviceInfo.getServiceURI();
        this.username = username;
        this.password = password;
        this.connected = false;
    }
    
    @Override
    public void connect() throws IOException {
        if(!this.connected) {
            this.restfulClient = new RestfulClient(this.serviceUri, this.username, this.password);

            this.connectionEstablishedTime = DateTimeUtils.getTimestamp();
            this.lastActiveTime = this.connectionEstablishedTime;
            this.connected = true;
        }
    }
    
    @Override
    public void disconnect() {
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
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        return PathUtils.concatPath(HTTPTransportRestfulConstants.API_PATH, path);
    }
    
    private String makeGetMetadataPath(String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        return PathUtils.concatPath(HTTPTransportRestfulConstants.GET_METADATA_PATH, path);
    }
    
    private String makeListMetadataPath(String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        return PathUtils.concatPath(HTTPTransportRestfulConstants.LIST_METADATA_PATH, path);
    }
    
    private String makeGetDirectoryPath(String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        return PathUtils.concatPath(HTTPTransportRestfulConstants.GET_DIRECTORY_PATH, path);
    }
    
    private String makeGetRecipePath(String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        return PathUtils.concatPath(HTTPTransportRestfulConstants.GET_RECIPE_PATH, path);
    }
    
    private String makeGetDataChunkPath(String hash) {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        return PathUtils.concatPath(HTTPTransportRestfulConstants.GET_DATA_CHUNK_PATH, hash);
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
        
        try {
            // URL pattern = http://xxx.xxx.xxx.xxx/api/live
            String url = makeAPIPath(HTTPTransportRestfulConstants.API_CHECK_LIVE_PATH);
            RestfulResponse<Boolean> response = (RestfulResponse<Boolean>) this.restfulClient.get(url, new GenericType<RestfulResponse<Boolean>>(){});
            
            if(response.getException() != null) {
                return false;
            } else {
                updateLastActivetime();
                return response.getResponse().booleanValue();
            }
        } catch (RestfulException ex) {
            throw new IOException(ex);
        } catch (AuthenticationException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public Cluster getCluster() throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        try {
            // URL pattern = http://xxx.xxx.xxx.xxx/api/cluster
            String url = makeAPIPath(HTTPTransportRestfulConstants.API_GET_CLUSTER_PATH);
            RestfulResponse<Cluster> response = (RestfulResponse<Cluster>) this.restfulClient.get(url, new GenericType<RestfulResponse<Cluster>>(){});
            
            if(response.getException() != null) {
                throw new IOException(response.getException());
            } else {
                updateLastActivetime();
                return response.getResponse();
            }
        } catch (RestfulException ex) {
            throw new IOException(ex);
        } catch (AuthenticationException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public DataObjectMetadata getDataObjectMetadata(DataObjectURI uri) throws FileNotFoundException, IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            // URL pattern = http://xxx.xxx.xxx.xxx/metadata/path/to/resource
            String url = makeGetMetadataPath(uri.getPath());
            RestfulResponse<DataObjectMetadata> response = (RestfulResponse<DataObjectMetadata>) this.restfulClient.get(url, new GenericType<RestfulResponse<DataObjectMetadata>>(){});
            
            if(response.getException() != null) {
                throw new IOException(response.getException());
            } else {
                updateLastActivetime();
                return response.getResponse();
            }
        } catch (RestfulException ex) {
            throw new IOException(ex);
        } catch (AuthenticationException ex) {
            throw new IOException(ex);
        }
    }
    
    @Override
    public Collection<DataObjectMetadata> listDataObjectMetadata(DataObjectURI uri) throws FileNotFoundException, IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            // URL pattern = http://xxx.xxx.xxx.xxx/lmetadata/path/to/resource
            String url = makeListMetadataPath(uri.getPath());
            RestfulResponse<Collection<DataObjectMetadata>> response = (RestfulResponse<Collection<DataObjectMetadata>>) this.restfulClient.get(url, new GenericType<RestfulResponse<Collection<DataObjectMetadata>>>(){});
            
            if(response.getException() != null) {
                throw new IOException(response.getException());
            } else {
                updateLastActivetime();
                return response.getResponse();
            }
        } catch (RestfulException ex) {
            throw new IOException(ex);
        } catch (AuthenticationException ex) {
            throw new IOException(ex);
        }
    }
    
    @Override
    public Directory getDirectory(DataObjectURI uri) throws FileNotFoundException, IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            // URL pattern = http://xxx.xxx.xxx.xxx/lmetadata/path/to/resource
            String url = makeGetDirectoryPath(uri.getPath());
            RestfulResponse<Directory> response = (RestfulResponse<Directory>) this.restfulClient.get(url, new GenericType<RestfulResponse<Directory>>(){});
            
            if(response.getException() != null) {
                throw new IOException(response.getException());
            } else {
                updateLastActivetime();
                return response.getResponse();
            }
        } catch (RestfulException ex) {
            throw new IOException(ex);
        } catch (AuthenticationException ex) {
            throw new IOException(ex);
        }
    }
    
    @Override
    public Recipe getRecipe(DataObjectURI uri) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            // URL pattern = http://xxx.xxx.xxx.xxx/recipe/path/to/resource
            String url = makeGetRecipePath(uri.getPath());
            RestfulResponse<Recipe> response = (RestfulResponse<Recipe>) this.restfulClient.get(url, new GenericType<RestfulResponse<Recipe>>(){});
            
            if(response.getException() != null) {
                throw new IOException(response.getException());
            } else {
                updateLastActivetime();
                return response.getResponse();
            }
        } catch (RestfulException ex) {
            throw new IOException(ex);
        } catch (AuthenticationException ex) {
            throw new IOException(ex);
        }
    }

    @Override
    public InputStream getDataChunk(String hash) throws IOException {
        if(!this.connected) {
            throw new IOException("Client is not connected");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        try {
            // URL pattern = http://xxx.xxx.xxx.xxx/data/hash
            String url = makeGetDataChunkPath(hash);
            InputStream is = this.restfulClient.download(url);
            
            updateLastActivetime();
            return is;
        } catch (RestfulException ex) {
            throw new IOException(ex);
        } catch (AuthenticationException ex) {
            throw new IOException(ex);
        }
    }
}
