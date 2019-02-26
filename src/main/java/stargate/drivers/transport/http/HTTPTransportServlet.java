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
import java.util.Collection;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.dataobject.Directory;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.AbstractRecipeDriver;
import stargate.commons.recipe.Recipe;
import stargate.commons.restful.RestfulResponse;
import stargate.commons.service.FSServiceInfo;
import stargate.commons.transport.AbstractTransportServer;
import stargate.commons.utils.PathUtils;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.recipe.RecipeManager;
import stargate.managers.statistics.StatisticsManager;
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
    
    static void setDriver(HTTPTransportDriver driver) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
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
        LOG.info("REQ - isLiveRestful");
        
        try {
            boolean live = isLive();
            RestfulResponse rres = new RestfulResponse(live);
            Response res = Response.status(Response.Status.OK).entity(rres).build();
            
            LOG.info("RES - isLiveRestful");
            
            return res;
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            Response res = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
            
            LOG.info("RES (ERR) - isLiveRestful");
            
            return res;
        }
    }
    
    @Override
    public boolean isLive() {
        return true;
    }

    @GET
    @Path(HTTPTransportRestfulConstants.API_PATH + "/" + HTTPTransportRestfulConstants.API_GET_FS_SERVICE_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFSServiceInfoRestful() throws IOException {
        LOG.info("REQ - getFSServiceInfoRestful");
        
        try {
            FSServiceInfo info = getFSServiceInfo();
            RestfulResponse rres = new RestfulResponse(info);
            Response res = Response.status(Response.Status.OK).entity(rres).build();
            
            LOG.info("RES - getFSServiceInfoRestful");
            
            return res;
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            Response res = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
            
            LOG.info("RES (ERR) - getFSServiceInfoRestful");
            
            return res;
        }
    }
    
    @Override
    public FSServiceInfo getFSServiceInfo() throws IOException {
        try {
            StargateService stargateService = getStargateService();
            RecipeManager recipeManager = stargateService.getRecipeManager();
            AbstractRecipeDriver recipeDriver = recipeManager.getDriver();
            int chunkSize = recipeDriver.getChunkSize();
            String hashAlgorithm = recipeDriver.getHashAlgorithm();
            
            FSServiceInfo serviceInfo = new FSServiceInfo(chunkSize, hashAlgorithm);
            return serviceInfo;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        } catch (Exception ex) {
            LOG.error("Unknown exception", ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.API_PATH + "/" + HTTPTransportRestfulConstants.API_GET_LOCAL_CLUSTER_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getLocalClusterRestful() throws IOException {
        LOG.info("REQ - getLocalClusterRestful");
        
        try {
            Cluster localCluster = getLocalCluster();
            RestfulResponse rres = new RestfulResponse(localCluster);
            Response res = Response.status(Response.Status.OK).entity(rres).build();
            
            LOG.info("RES - getLocalClusterRestful");
            
            return res;
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            Response res = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
            
            LOG.info("RES (ERR) - getLocalClusterRestful");
            
            return res;
        }
    }
    
    @Override
    public Cluster getLocalCluster() throws IOException {
        try {
            StargateService service = getStargateService();
            ClusterManager clusterManager = service.getClusterManager();
            return clusterManager.getLocalCluster();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        } catch (DriverNotInitializedException ex) {
            LOG.error("Driver is not initialized", ex);
            throw new IOException(ex);
        } catch (Exception ex) {
            LOG.error("Unknown exception", ex);
            throw new IOException(ex);
        }
    }

    @GET
    @Path(HTTPTransportRestfulConstants.API_PATH + "/" + HTTPTransportRestfulConstants.API_GET_METADATA_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDataObjectMetadataRestful(
        @DefaultValue("") @PathParam("path") String path) throws IOException {
        //if(path == null || path.isEmpty()) {
        //    throw new IllegalArgumentException("path is null or empty");
        //}
        
        LOG.info(String.format("REQ - getDataObjectMetadataRestful - %s", path));
        
        
        try {
            Cluster cluster = getLocalCluster();
            DataObjectURI objectUri = new DataObjectURI(cluster.getName(), PathUtils.makeAbsolutePath(path));
            DataObjectMetadata objectMetadata = getDataObjectMetadata(objectUri);
            RestfulResponse rres = new RestfulResponse(objectMetadata);
            Response res = Response.status(Response.Status.OK).entity(rres).build();
        
            LOG.info(String.format("RES - getDataObjectMetadataRestful - %s", path));
            
            return res;
        } catch(FileNotFoundException ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            Response res = Response.status(Response.Status.NOT_FOUND).entity(rres).build();
            
            LOG.info(String.format("RES (ERR) - getDataObjectMetadataRestful - %s", path));
            
            return res;
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            Response res = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
            
            LOG.info(String.format("RES (ERR) - getDataObjectMetadataRestful - %s", path));
            
            return res;
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
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        } catch (DriverNotInitializedException ex) {
            LOG.error("Driver is not initialized", ex);
            throw new IOException(ex);
        } catch (Exception ex) {
            LOG.error("Unknown exception", ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.API_PATH + "/" + HTTPTransportRestfulConstants.API_LIST_METADATA_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listDataObjectMetadataRestful(
            @DefaultValue("") @PathParam("path") String path) throws IOException {
        //if(path == null || path.isEmpty()) {
        //    throw new IllegalArgumentException("path is null or empty");
        //}
        
        LOG.info(String.format("REQ - listDataObjectMetadataRestful - %s", path));
        
        try {
            Cluster cluster = getLocalCluster();
            DataObjectURI objectUri = new DataObjectURI(cluster.getName(), PathUtils.makeAbsolutePath(path));
            Collection<DataObjectMetadata> objectMetadataList = listDataObjectMetadata(objectUri);
            RestfulResponse rres = new RestfulResponse(objectMetadataList.toArray(new DataObjectMetadata[0]));
            Response res = Response.status(Response.Status.OK).entity(rres).build();
            
            LOG.info(String.format("RES - listDataObjectMetadataRestful - %s", path));
            
            return res;
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            Response res = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
            
            LOG.info(String.format("RES (ERR) - listDataObjectMetadataRestful - %s", path));
            
            return res;
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
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        } catch (DriverNotInitializedException ex) {
            LOG.error("Driver is not initialized", ex);
            throw new IOException(ex);
        } catch (Exception ex) {
            LOG.error("Unknown exception", ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.API_PATH + "/" + HTTPTransportRestfulConstants.API_GET_RECIPE_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRecipeRestful(
        @DefaultValue("") @PathParam("path") String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        LOG.info(String.format("REQ - getRecipeRestful - %s", path));
        
        try {
            Cluster cluster = getLocalCluster();
            DataObjectURI objectUri = new DataObjectURI(cluster.getName(), PathUtils.makeAbsolutePath(path));
            Recipe recipe = getRecipe(objectUri);
            RestfulResponse rres = new RestfulResponse(recipe);
            Response res = Response.status(Response.Status.OK).entity(rres).build();
            
            LOG.info(String.format("RES - getRecipeRestful - %s", path));
            
            return res;
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            Response res = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
            
            LOG.info(String.format("RES (ERR) - getRecipeRestful - %s", path));
            
            return res;
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
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        } catch (DriverNotInitializedException ex) {
            LOG.error("Driver is not initialized", ex);
            throw new IOException(ex);
        } catch (Exception ex) {
            LOG.error("Unknown exception", ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.API_PATH + "/" + HTTPTransportRestfulConstants.API_GET_DIRECTORY_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDirectoryRestful(
            @DefaultValue("") @PathParam("path") String path) throws IOException {
        //if(path == null || path.isEmpty()) {
        //    throw new IllegalArgumentException("path is null or empty");
        //}
        
        LOG.info(String.format("REQ - getDirectoryRestful - %s", path));
        
        try {
            Cluster cluster = getLocalCluster();
            DataObjectURI objectUri = new DataObjectURI(cluster.getName(), PathUtils.makeAbsolutePath(path));
            Directory directory = getDirectory(objectUri);
            RestfulResponse rres = new RestfulResponse(directory);
            Response res = Response.status(Response.Status.OK).entity(rres).build();
            
            LOG.info(String.format("RES - getDirectoryRestful - %s", path));
            
            return res;
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            Response res = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
            
            LOG.info(String.format("RES (ERR) - getDirectoryRestful - %s", path));
            
            return res;
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
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        } catch (DriverNotInitializedException ex) {
            LOG.error("Driver is not initialized", ex);
            throw new IOException(ex);
        } catch (Exception ex) {
            LOG.error("Unknown exception", ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPTransportRestfulConstants.API_PATH + "/" + HTTPTransportRestfulConstants.API_GET_DATA_CHUNK_PATH + "/{hash:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getDataChunkRestful(
            @DefaultValue("") @PathParam("hash") String hash) throws Exception {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        LOG.info(String.format("REQ - getDataChunkRestful - %s", hash));
        
        try {
            final InputStream is = getDataChunk(hash);
            if(is == null) {
                LOG.info(String.format("RES (ERR) - getDataChunkRestful - %s", hash));
                return Response.status(Response.Status.NOT_FOUND).build();
            }
            
            StreamingOutputData stream = new StreamingOutputData(is);
            Response res = Response.ok(stream).header("content-disposition", "attachment; filename = " + hash).build();
            
            LOG.info(String.format("RES - getDataChunkRestful - %s", hash));
            
            return res;
        } catch (Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            Response res = Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
            
            LOG.info(String.format("RES (ERR) - getDataChunkRestful - %s", hash));
            
            return res;
        }
    }

    @Override
    public InputStream getDataChunk(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        try {
            StargateService service = getStargateService();
            VolumeManager volumeManager = service.getVolumeManager();
            InputStream is = volumeManager.getLocalDataChunk(hash);
            
            StatisticsManager statisticsManager = service.getStatisticsManager();
            statisticsManager.addDataChunkTransferSendStatistics(hash);
            return is;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        } catch (DriverNotInitializedException ex) {
            LOG.error("Driver is not initialized", ex);
            throw new IOException(ex);
        } catch (Exception ex) {
            LOG.error("Unknown exception", ex);
            throw new IOException(ex);
        }
    }
}
