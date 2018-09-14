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
import java.io.OutputStream;
import java.util.Collection;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.dataobject.Directory;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.Recipe;
import stargate.commons.restful.RestfulResponse;
import stargate.commons.transport.AbstractTransportServer;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.volume.VolumeManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
@Path(HTTPTransportRestfulConstants.BASE_PATH)
public class HTTPTransportServlet extends AbstractTransportServer {

    private static final Log LOG = LogFactory.getLog(HTTPTransportServlet.class);

    private static HTTPTransportDriver driver = null;
    
    public class StreamingOutputData implements StreamingOutput {

        private static final int BUFFER_SIZE = 8*1024; // 8k
        private InputStream is;
        private byte[] buffer;
        
        StreamingOutputData(InputStream is) {
            this.is = is;
            this.buffer = new byte[BUFFER_SIZE];
        }
        
        @Override
        public void write(OutputStream out) throws IOException, WebApplicationException {
            try {
                int read = 0;
                while ((read = this.is.read(this.buffer)) > 0) {
                    out.write(this.buffer, 0, read);
                }
                this.is.close();
            } catch (Exception ex) {
                throw new WebApplicationException(ex);
            }
        }
    }
    
    static void setDriver(HTTPTransportDriver driver) {
        HTTPTransportServlet.driver = driver;
    }
    
    static HTTPTransportDriver getDriver() {
        if(driver == null) {
            throw new IllegalStateException("Driver is not set");
        }
        return driver;
    }
    
    private StargateService getStargateService() {
        HTTPTransportDriver driver = getDriver();
        AbstractManager manager = driver.getManager();
        return (StargateService) manager.getService();
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.API_PATH + "/" + HTTPTransportRestfulConstants.API_CHECK_LIVE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response isLiveRestful() throws IOException {
        try {
            RestfulResponse rres = new RestfulResponse(isLive());
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public boolean isLive() {
        return true;
    }

    @GET
    @Path(HTTPTransportRestfulConstants.API_PATH + "/" + HTTPTransportRestfulConstants.API_GET_CLUSTER_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getClusterRestful() throws IOException {
        try {
            Cluster cluster = getCluster();
            RestfulResponse rres = new RestfulResponse(cluster);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Cluster getCluster() throws IOException {
        try {
            StargateService service = getStargateService();
            ClusterManager clusterManager = service.getClusterManager();
            return clusterManager.getLocalCluster();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }

    @GET
    @Path(HTTPTransportRestfulConstants.GET_METADATA_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDataObjectMetadataRestful(
        @DefaultValue("") @PathParam("path") String path) throws IOException {
        //if(path == null || path.isEmpty()) {
        //    throw new IllegalArgumentException("path is null or empty");
        //}
        
        String new_path = path;
        if(path == null || path.isEmpty()) {
            // this is the case when cluster + path is "/"
            // e.g. sgfs:///
            new_path = "/";
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI(new_path);
            DataObjectMetadata objectMetadata = getDataObjectMetadata(objectUri);
            RestfulResponse rres = new RestfulResponse(objectMetadata);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(FileNotFoundException ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.NOT_FOUND).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public DataObjectMetadata getDataObjectMetadata(DataObjectURI uri) throws IOException, FileNotFoundException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            StargateService service = getStargateService();
            VolumeManager volumeManager = service.getVolumeManager();
            return volumeManager.getDataObjectMetadata(uri);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.LIST_METADATA_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listDataObjectMetadataRestful(
            @DefaultValue("") @PathParam("path") String path) throws IOException {
        //if(path == null || path.isEmpty()) {
        //    throw new IllegalArgumentException("path is null or empty");
        //}
        
        String new_path = path;
        if(path == null || path.isEmpty()) {
            // this is the case when cluster + path is "/"
            // e.g. sgfs:///
            new_path = "/";
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI(new_path);
            Collection<DataObjectMetadata> objectMetadataList = listDataObjectMetadata(objectUri);
            RestfulResponse rres = new RestfulResponse(objectMetadataList.toArray(new DataObjectMetadata[0]));
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Collection<DataObjectMetadata> listDataObjectMetadata(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            StargateService service = getStargateService();
            VolumeManager volumeManager = service.getVolumeManager();
            Directory directory = volumeManager.getDirectory(uri);
            return directory.getEntries();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.GET_RECIPE_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRecipeRestful(
        @DefaultValue("") @PathParam("path") String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI(path);
            Recipe recipe = getRecipe(objectUri);
            RestfulResponse rres = new RestfulResponse(recipe);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Recipe getRecipe(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            StargateService service = getStargateService();
            VolumeManager volumeManager = service.getVolumeManager();
            return volumeManager.getRecipe(uri);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.GET_DIRECTORY_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDirectoryRestful(
            @DefaultValue("") @PathParam("path") String path) throws IOException {
        //if(path == null || path.isEmpty()) {
        //    throw new IllegalArgumentException("path is null or empty");
        //}
        
        String new_path = path;
        if(path == null || path.isEmpty()) {
            // this is the case when cluster + path is "/"
            // e.g. sgfs:///
            new_path = "/";
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI(new_path);
            Directory directory = getDirectory(objectUri);
            RestfulResponse rres = new RestfulResponse(directory);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Directory getDirectory(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            StargateService service = getStargateService();
            VolumeManager volumeManager = service.getVolumeManager();
            return volumeManager.getDirectory(uri);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.GET_DATA_CHUNK_PATH + "/{hash:\\w*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getDataChunkRestful(
            @DefaultValue("") @PathParam("hash") String hash) throws Exception {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        try {
            final InputStream is = getDataChunk(hash);
            if(is == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            
            StreamingOutputData stream = new StreamingOutputData(is);
            return Response.ok(stream).header("content-disposition", "attachment; filename = " + hash).build();
        } catch (Exception ex) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public InputStream getDataChunk(String hash) throws IOException {
        return getDataChunk(DataObjectURI.WILDCARD_LOCAL_CLUSTER_NAME, hash);
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.GET_DATA_CHUNK_PATH + "/{cluster:.*}/{hash:\\w+}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getDataChunkRestful(
            @DefaultValue("") @PathParam("cluster") String cluster,
            @DefaultValue("") @PathParam("hash") String hash) throws Exception {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is null or empty");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        try {
            final InputStream is = getDataChunk(cluster, hash);
            if(is == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            
            StreamingOutputData stream = new StreamingOutputData(is);
            return Response.ok(stream).header("content-disposition", "attachment; filename = " + hash).build();
        } catch (Exception ex) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public InputStream getDataChunk(String clusterName, String hash) throws IOException {
        if(clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("clusterName is null or empty");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        try {
            StargateService service = getStargateService();
            VolumeManager volumeManager = service.getVolumeManager();
            return volumeManager.getDataChunk(clusterName, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
}
