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
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.AbstractClusterDriver;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.dataobject.Directory;
import stargate.commons.datasource.AbstractDataSourceDriver;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.datasource.SourceFileMetadata;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.AbstractRecipeDriver;
import stargate.commons.recipe.Recipe;
import stargate.commons.restful.RestfulResponse;
import stargate.commons.service.FSServiceInfo;
import stargate.commons.userinterface.AbstractUserInterfaceServer;
import stargate.commons.utils.PathUtils;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.cluster.ClusterManagerException;
import stargate.managers.dataexport.DataExportManager;
import stargate.managers.dataexport.DataExportManagerException;
import stargate.managers.datasource.DataSourceManager;
import stargate.managers.recipe.RecipeManager;
import stargate.managers.recipe.RecipeManagerException;
import stargate.managers.transport.TransferAssignment;
import stargate.managers.transport.TransferEvent;
import stargate.managers.transport.TransportManager;
import stargate.managers.volume.VolumeManager;
import stargate.service.StargateService;
import stargate.service.StargateServiceConfig;

/**
 *
 * @author iychoi
 */
@Path(HTTPUserInterfaceRestfulConstants.BASE_PATH)
public class HTTPUserInterfaceServlet extends AbstractUserInterfaceServer {

    private static final Log LOG = LogFactory.getLog(HTTPUserInterfaceServlet.class);

    private static HTTPUserInterfaceDriver driver = null;

    public class StreamingOutputData implements StreamingOutput {

        private static final int BUFFER_SIZE = 64*1024; // 64k
        private InputStream is;
        private byte[] buffer;
        
        StreamingOutputData(InputStream is) {
            if(is == null) {
                throw new IllegalArgumentException("is is null");
            }
            
            this.is = is;
            this.buffer = new byte[BUFFER_SIZE];
        }
        
        @Override
        public void write(OutputStream out) throws IOException, WebApplicationException {
            if(out == null) {
                throw new IllegalArgumentException("out is null");
            }
            
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
    
    static void setDriver(HTTPUserInterfaceDriver driver) {
        if(driver == null) {
            throw new IllegalArgumentException("driver is null");
        }
        
        HTTPUserInterfaceServlet.driver = driver;
    }
    
    static HTTPUserInterfaceDriver getDriver() {
        if(driver == null) {
            throw new IllegalStateException("Driver is not set");
        }
        return driver;
    }
    
    private StargateService getStargateService() {
        HTTPUserInterfaceDriver driver = getDriver();
        AbstractManager manager = driver.getManager();
        return (StargateService) manager.getService();
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_CHECK_LIVE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response isLiveRestful() throws IOException {
        try {
            boolean live = isLive();
            RestfulResponse rres = new RestfulResponse(live);
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
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_SERVICE_CONFIG_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getServiceConfigRestful() throws IOException {
        try {
            String config = getServiceConfig();
            RestfulResponse rres = new RestfulResponse(config);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public String getServiceConfig() throws IOException {
        StargateService stargateService = getStargateService();
        StargateServiceConfig config = stargateService.getConfig();
        return config.toJson();
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_FS_SERVICE_INFO_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getFSServiceInfoRestful() throws IOException {
        try {
            FSServiceInfo info = getFSServiceInfo();
            RestfulResponse rres = new RestfulResponse(info);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
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
            LOG.error(ex);
            throw new IOException(ex);
        }
    }

    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_CLUSTER_PATH + "/{name:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getClusterRestful(
        @DefaultValue("") @PathParam("name") String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        try {
            Cluster cluster = getCluster(name);
            RestfulResponse rres = new RestfulResponse(cluster);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Cluster getCluster(String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        try {
            StargateService service = getStargateService();
            ClusterManager clusterManager = service.getClusterManager();
            String localClusterName = clusterManager.getLocalClusterName();
            if(localClusterName.equals(name) || name.equalsIgnoreCase(DataObjectURI.WILDCARD_LOCAL_CLUSTER_NAME)) {
                return clusterManager.getLocalCluster();
            } else {
                return clusterManager.getRemoteCluster(name);
            }
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_LOCAL_CLUSTER_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getLocalClusterRestful() throws IOException {
        try {
            Cluster localCluster = getLocalCluster();
            RestfulResponse rres = new RestfulResponse(localCluster);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Cluster getLocalCluster() throws IOException {
        try {
            StargateService service = getStargateService();
            ClusterManager clusterManager = service.getClusterManager();
            return clusterManager.getLocalCluster();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @POST
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_ACTIVATE_CLUSTER_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response activateClusterRestful() throws IOException {
        try {
            activateCluster();
            
            RestfulResponse rres = new RestfulResponse(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public void activateCluster() throws IOException {
        try {
            StargateService service = getStargateService();
            ClusterManager clusterManager = service.getClusterManager();
            AbstractClusterDriver clusterDriver = clusterManager.getDriver();
            clusterDriver.activateCluster();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_CHECK_ACTIVE_CLUSTER_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response isClusterActiveRestful() throws IOException {
        try {
            boolean active = isClusterActive();
            RestfulResponse rres = new RestfulResponse(active);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public boolean isClusterActive() throws IOException {
        try {
            StargateService service = getStargateService();
            ClusterManager clusterManager = service.getClusterManager();
            AbstractClusterDriver clusterDriver = clusterManager.getDriver();
            return clusterDriver.isClusterActive();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_LOCAL_NODE_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getLocalNodeRestful() throws IOException {
        try {
            Node localNode = getLocalNode();
            RestfulResponse rres = new RestfulResponse(localNode);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Node getLocalNode() throws IOException {
        try {
            StargateService service = getStargateService();
            ClusterManager clusterManager = service.getClusterManager();
            return clusterManager.getLocalNode();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_REMOTE_CLUSTER_PATH + "/{name:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRemoteClusterRestful(
        @DefaultValue("") @PathParam("name") String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        try {
            Cluster cluster = getRemoteCluster(name);
            RestfulResponse rres = new RestfulResponse(cluster);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Cluster getRemoteCluster(String name) throws IOException {
        if(name == null || name.isEmpty()) {
            throw new IllegalArgumentException("name is null or empty");
        }
        
        try {
            StargateService service = getStargateService();
            ClusterManager clusterManager = service.getClusterManager();
            return clusterManager.getRemoteCluster(name);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_LIST_REMOTE_CLUSTERS_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response listRemoteClustersRestful() throws IOException {
        try {
            Collection<String> remoteClusters = listRemoteClusters();
            RestfulResponse rres = new RestfulResponse(remoteClusters.toArray(new String[0]));
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Collection<String> listRemoteClusters() throws IOException {
        try {
            StargateService service = getStargateService();
            ClusterManager clusterManager = service.getClusterManager();
            return clusterManager.getRemoteClusterNames();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_REMOTE_CLUSTERS_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRemoteClustersRestful() throws IOException {
        try {
            Collection<Cluster> remoteClusters = getRemoteClusters();
            RestfulResponse rres = new RestfulResponse(remoteClusters.toArray(new Cluster[0]));
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }

    @Override
    public Collection<Cluster> getRemoteClusters() throws IOException {
        try {
            StargateService service = getStargateService();
            ClusterManager clusterManager = service.getClusterManager();
            return clusterManager.getRemoteClusters();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @PUT
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_ADD_REMOTE_CLUSTER_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addRemoteClusterRestful(Cluster cluster) throws IOException {
        try {
            addRemoteCluster(cluster);
            
            RestfulResponse rres = new RestfulResponse(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public void addRemoteCluster(Cluster cluster) throws IOException {
        try {
            StargateService service = getStargateService();
            ClusterManager clusterManager = service.getClusterManager();
            clusterManager.addRemoteCluster(cluster);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        } catch (ClusterManagerException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @DELETE
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_REMOVE_REMOTE_CLUSTER_PATH + "/{cluster: .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeRemoteClusterRestful(
            @DefaultValue("") @PathParam("cluster") String cluster) throws IOException {
        if(cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster is null or empty");
        }
        
        try {
            removeRemoteCluster(cluster);
            RestfulResponse rres = new RestfulResponse(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public void removeRemoteCluster(String name) throws IOException {
        try {
            StargateService service = getStargateService();
            ClusterManager clusterManager = service.getClusterManager();
            clusterManager.removeRemoteCluster(name);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_METADATA_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDataObjectMetadataRestful(
        @DefaultValue("") @PathParam("path") String path) throws IOException {
        //if(path == null || path.isEmpty()) {
        //    throw new IllegalArgumentException("path is null or empty");
        //}
        
        try {
            DataObjectURI objectUri = new DataObjectURI(PathUtils.makeAbsolutePath(path));
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
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_LIST_METADATA_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listDataObjectMetadataRestful(
            @DefaultValue("") @PathParam("path") String path) throws IOException {
        //if(path == null || path.isEmpty()) {
        //    throw new IllegalArgumentException("path is null or empty");
        //}
        
        try {
            DataObjectURI objectUri = new DataObjectURI(PathUtils.makeAbsolutePath(path));
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
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_RECIPE_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRecipeRestful(
        @DefaultValue("") @PathParam("path") String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI(PathUtils.makeAbsolutePath(path));
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
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_LIST_RECIPES_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response listRecipesRestful() throws IOException {
        try {
            Collection<String> recipes = listRecipes();
            RestfulResponse rres = new RestfulResponse(recipes.toArray(new String[0]));
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Collection<String> listRecipes() throws IOException {
        try {
            StargateService service = getStargateService();
            RecipeManager recipeManager = service.getRecipeManager();
            return recipeManager.getRecipeKeys();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @DELETE
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_REMOVE_RECIPE_PATH + "/{path: .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeRecipeRestful(
            @DefaultValue("") @PathParam("path") String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            // local
            DataObjectURI objectUri = new DataObjectURI("", PathUtils.makeAbsolutePath(path));
            removeRecipe(objectUri);
            RestfulResponse rres = new RestfulResponse(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public void removeRecipe(DataObjectURI uri) throws IOException {
        try {
            StargateService service = getStargateService();
            RecipeManager recipeManager = service.getRecipeManager();
            recipeManager.removeRecipe(uri.getPath());
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @POST
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_SYNC_RECIPES_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response syncRecipesRestful() throws IOException {
        try {
            syncRecipes();
            RestfulResponse rres = new RestfulResponse(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public void syncRecipes() throws IOException {
        try {
            StargateService service = getStargateService();
            RecipeManager recipeManager = service.getRecipeManager();
            recipeManager.syncRecipes();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        } catch (RecipeManagerException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_DATA_CHUNK_PATH + "/{path:.*}/{hash:.*}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getDataChunkRestful(
            @DefaultValue("") @PathParam("path") String path,
            @DefaultValue("") @PathParam("hash") String hash) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI(PathUtils.makeAbsolutePath(path));
            final InputStream is = getDataChunk(objectUri, hash);
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
    public InputStream getDataChunk(DataObjectURI uri, String hash) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        try {
            StargateService service = getStargateService();
            VolumeManager volumeManager = service.getVolumeManager();
            return volumeManager.getDataChunk(uri, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @POST
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_SCHEDULE_PREFETCH_PATH + "/{path:.*}/{hash:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response schedulePrefetchRestful(
        @DefaultValue("") @PathParam("path") String path,
        @DefaultValue("") @PathParam("hash") String hash) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI(PathUtils.makeAbsolutePath(path));
            String assignedNodeName = schedulePrefetch(objectUri, hash);
            RestfulResponse rres = new RestfulResponse(assignedNodeName);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public String schedulePrefetch(DataObjectURI uri, String hash) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        try {
            StargateService service = getStargateService();
            TransportManager transportManager = service.getTransportManager();
            TransferAssignment assignment = transportManager.schedulePrefetch(uri, hash);
            TransferEvent event = assignment.getEvent();
            return event.getTargetNodeName();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @POST
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_REMOTE_RECIPE_WITH_TRANSFER_SCHEDULE_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRemoteRecipeWithTransferScheduleRestful(
        @DefaultValue("") @PathParam("path") String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI(PathUtils.makeAbsolutePath(path));
            Recipe recipe = getRemoteRecipeWithTransferSchedule(objectUri);
            RestfulResponse rres = new RestfulResponse(recipe);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Recipe getRemoteRecipeWithTransferSchedule(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            StargateService service = getStargateService();
            VolumeManager volumeManager = service.getVolumeManager();
            return volumeManager.getRemoteRecipeWithTransferSchedule(uri);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_DATA_EXPORT_ENTRY_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDataExportEntryRestful(
            @DefaultValue("") @PathParam("path") String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            // local
            DataObjectURI objectUri = new DataObjectURI("", PathUtils.makeAbsolutePath(path));
            DataExportEntry mapping = getDataExportEntry(objectUri);    
            RestfulResponse rres = new RestfulResponse(mapping);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public DataExportEntry getDataExportEntry(DataObjectURI uri) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        try {
            StargateService service = getStargateService();
            DataExportManager dataExportManager = service.getDataExportManager();
            return dataExportManager.getDataExportEntry(uri.getPath());
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_LIST_DATA_EXPORT_ENTRIES_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response listDataExportEntriesRestful() throws IOException {
        try {
            Collection<String> dataExportEntries = listDataExportEntries();    
            RestfulResponse rres = new RestfulResponse(dataExportEntries.toArray(new String[0]));
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Collection<String> listDataExportEntries() throws IOException {
        try {
            StargateService service = getStargateService();
            DataExportManager dataExportManager = service.getDataExportManager();
            Collection<DataExportEntry> dataExportEntries = dataExportManager.getDataExportEntries();
            
            List<String> entries = new ArrayList<String>();
            for(DataExportEntry entry : dataExportEntries) {
                String stargatePath = entry.getStargatePath();
                entries.add(stargatePath);
            }
            
            return Collections.unmodifiableCollection(entries);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_DATA_EXPORT_ENTRIES_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDataExportEntriesRestful() throws IOException {
        try {
            Collection<DataExportEntry> dataExportEntries = getDataExportEntries();    
            RestfulResponse rres = new RestfulResponse(dataExportEntries.toArray(new DataExportEntry[0]));
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }

    @Override
    public Collection<DataExportEntry> getDataExportEntries() throws IOException {
        try {
            StargateService service = getStargateService();
            DataExportManager dataExportManager = service.getDataExportManager();
            return dataExportManager.getDataExportEntries();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @PUT
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_ADD_DATA_EXPORT_ENTRY_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addDataExportEntryRestful(DataExportEntry entry) throws IOException {
        try {
            addDataExportEntry(entry);
            
            RestfulResponse rres = new RestfulResponse(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public void addDataExportEntry(DataExportEntry entry) throws IOException {
        try {
            StargateService service = getStargateService();
            DataSourceManager dataSourceManager = service.getDataSourceManager();
            DataExportManager dataExportManager = service.getDataExportManager();
            
            URI sourceURI = entry.getSourceURI();
            AbstractDataSourceDriver dataSourceDriver = dataSourceManager.getDriver(sourceURI);
            SourceFileMetadata sourceFileMetadata = dataSourceDriver.getMetadata(sourceURI);
            if(sourceFileMetadata.isDirectory()) {
                addDataExportEntryRecursively(dataExportManager, entry.getStargatePath(), dataSourceDriver, sourceFileMetadata);
            } else {
                // entry.stargatePath is a directory
                // so we need to auto-complete the filename to be exposed
                String stargateFileName = PathUtils.getFileName(sourceFileMetadata.getURI());
                String concatPath = PathUtils.concatPath(entry.getStargatePath(), stargateFileName);
                entry.setStargatePath(concatPath);
                dataExportManager.addDataExportEntry(entry);
            }
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        } catch (DataExportManagerException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    private void addDataExportEntryRecursively(DataExportManager dataExportManager, String stargatePath, AbstractDataSourceDriver dataSourceDriver, SourceFileMetadata sourceDirectoryMetadata) throws IOException, DataExportManagerException {
        Collection<SourceFileMetadata> listDirectoryWithMetadata = dataSourceDriver.listDirectoryWithMetadata(sourceDirectoryMetadata.getURI());
        for(SourceFileMetadata fileMetadata : listDirectoryWithMetadata) {
            String stargateFileName = PathUtils.getFileName(fileMetadata.getURI());
            String concatPath = PathUtils.concatPath(stargatePath, stargateFileName);
                
            if(fileMetadata.isFile()) {
                // make a new DataExportEntry to register
                DataExportEntry entry = new DataExportEntry(fileMetadata.getURI(), concatPath);
                dataExportManager.addDataExportEntry(entry);
            } else {
                // call recursively
                addDataExportEntryRecursively(dataExportManager, concatPath, dataSourceDriver, fileMetadata);
            }
        }
    }
    
    @DELETE
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_REMOVE_DATA_EXPORT_ENTRY_PATH + "/{path: .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeDataExportEntryRestful(
            @DefaultValue("") @PathParam("path") String path) throws IOException {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            // local
            DataObjectURI objectUri = new DataObjectURI("", PathUtils.makeAbsolutePath(path));
            removeDataExportEntry(objectUri);
            RestfulResponse rres = new RestfulResponse(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }

    @Override
    public void removeDataExportEntry(DataObjectURI uri) throws IOException {
        try {
            StargateService service = getStargateService();
            DataExportManager dataExportManager = service.getDataExportManager();
            dataExportManager.removeDataExportEntry(uri.getPath());
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_LIST_DATA_SOURCES_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response listDataSourcesRestful() throws IOException {
        try {
            Collection<String> dataSources = listDataSources();    
            RestfulResponse rres = new RestfulResponse(dataSources.toArray(new String[0]));
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse rres = new RestfulResponse(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public Collection<String> listDataSources() throws IOException {
        try {
            StargateService service = getStargateService();
            DataSourceManager dataSourceManager = service.getDataSourceManager();
            return dataSourceManager.getRegisteredSchemes();
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
}
