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
import stargate.commons.cluster.Cluster;
import stargate.commons.config.AbstractImmutableConfig;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.dataobject.Directory;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.Recipe;
import stargate.commons.restful.RestfulResponse;
import stargate.commons.userinterface.AbstractUserInterfaceServer;
import stargate.commons.utils.PathUtils;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.cluster.ClusterManagerException;
import stargate.managers.dataexport.DataExportManager;
import stargate.managers.dataexport.DataExportManagerException;
import stargate.managers.datasource.DataSourceManager;
import stargate.managers.recipe.RecipeManager;
import stargate.managers.recipe.RecipeManagerException;
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
    
    static void setDriver(HTTPUserInterfaceDriver driver) {
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
    public Response isLiveRestful() {
        try {
            boolean live = isLive();
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(live);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(ex);
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
    public Response getServiceConfigRestful() {
        try {
            StargateServiceConfig config = (StargateServiceConfig) getServiceConfig();
            RestfulResponse<StargateServiceConfig> rres = new RestfulResponse<StargateServiceConfig>(config);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<StargateServiceConfig> rres = new RestfulResponse<StargateServiceConfig>(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public AbstractImmutableConfig getServiceConfig() throws IOException {
        StargateService stargateService = getStargateService();
        return stargateService.getConfig();
    }

    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_CLUSTER_PATH)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getClusterRestful() {
        try {
            Cluster cluster = getCluster();
            RestfulResponse<Cluster> rres = new RestfulResponse<Cluster>(cluster);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Cluster> rres = new RestfulResponse<Cluster>(ex);
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
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_REMOTE_CLUSTER_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRemoteClusterRestful(
        @DefaultValue("") @PathParam("path") String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            Cluster remoteCluster = getRemoteCluster(path);
            RestfulResponse<Cluster> rres = new RestfulResponse<Cluster>(remoteCluster);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Cluster> rres = new RestfulResponse<Cluster>(ex);
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
    public Response listRemoteClustersRestful() {
        try {
            Collection<String> remoteClusters = listRemoteClusters();
            RestfulResponse<Collection<String>> rres = new RestfulResponse<Collection<String>>(remoteClusters);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Collection<String>> rres = new RestfulResponse<Collection<String>>(ex);
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
    public Response getRemoteClustersRestful() {
        try {
            Collection<Cluster> remoteClusters = getRemoteClusters();
            RestfulResponse<Collection<Cluster>> rres = new RestfulResponse<Collection<Cluster>>(remoteClusters);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Collection<Cluster>> rres = new RestfulResponse<Collection<Cluster>>(ex);
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
    public Response addRemoteClusterRestful(Cluster cluster) {
        try {
            addRemoteCluster(cluster);
            
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(ex);
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
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_REMOVE_REMOTE_CLUSTER_PATH + "/{path: .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeRemoteClusterRestful(
            @DefaultValue("") @PathParam("path") String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            removeRemoteCluster(path);
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(ex);
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
    @Path(HTTPUserInterfaceRestfulConstants.GET_METADATA_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDataObjectMetadataRestful(
        @DefaultValue("") @PathParam("path") String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI("", PathUtils.concatPath("/", path));
            DataObjectMetadata objectMetadata = getDataObjectMetadata(objectUri);
            RestfulResponse<DataObjectMetadata> rres = new RestfulResponse<DataObjectMetadata>(objectMetadata);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(FileNotFoundException ex) {
            return Response.status(Response.Status.NOT_FOUND).entity(ex.toString()).build();
        } catch(Exception ex) {
            RestfulResponse<DataObjectMetadata> rres = new RestfulResponse<DataObjectMetadata>(ex);
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
    @Path(HTTPUserInterfaceRestfulConstants.GET_RECIPE_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRecipeRestful(
        @DefaultValue("") @PathParam("path") String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI("", PathUtils.concatPath("/", path));
            Recipe recipe = getRecipe(objectUri);
            RestfulResponse<Recipe> rres = new RestfulResponse<Recipe>(recipe);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Recipe> rres = new RestfulResponse<Recipe>(ex);
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
    public Response listRecipesRestful() {
        try {
            Collection<String> recipes = listRecipes();
            RestfulResponse<Collection<String>> rres = new RestfulResponse<Collection<String>>(recipes);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Collection<String>> rres = new RestfulResponse<Collection<String>>(ex);
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
            @DefaultValue("") @PathParam("path") String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI("", PathUtils.concatPath("/", path));
            removeRecipe(objectUri);
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(ex);
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
    public Response syncRecipesRestful() {
        try {
            syncRecipes();
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(ex);
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
    @Path(HTTPUserInterfaceRestfulConstants.LIST_METADATAS_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listDataObjectMetadataRestful(
            @DefaultValue("") @PathParam("path") String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI("", PathUtils.concatPath("/", path));
            Collection<DataObjectMetadata> objectMetadataList = listDataObjectMetadatas(objectUri);        
            RestfulResponse<Collection<DataObjectMetadata>> rres = new RestfulResponse<Collection<DataObjectMetadata>>(objectMetadataList);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Collection<DataObjectMetadata>> rres = new RestfulResponse<Collection<DataObjectMetadata>>(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }

    @Override
    public Collection<DataObjectMetadata> listDataObjectMetadatas(DataObjectURI uri) throws IOException {
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
    @Path(HTTPUserInterfaceRestfulConstants.GET_DATA_CHUNK_PATH + "/{hash:.*}")
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
            
            StreamingOutput stream = new StreamingOutput() {
                @Override
                public void write(OutputStream out) throws IOException, WebApplicationException {
                    try {
                        int buffersize = 100 * 1024;
                        byte[] buffer = new byte[buffersize];

                        int read = 0;
                        while ((read = is.read(buffer)) > 0) {
                            out.write(buffer, 0, read);
                        }
                        is.close();

                    } catch (Exception ex) {
                        throw new WebApplicationException(ex);
                    }
                }
            };

            return Response.ok(stream).header("content-disposition", "attachment; filename = " + hash).build();
        } catch (Exception ex) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
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
            return volumeManager.getDataChunk(hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_SCHEDULE_PREFETCH_PATH + "/{path:.*}/{hash:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response schedulePrefetchRestful(
        @DefaultValue("") @PathParam("path") String path,
        @DefaultValue("") @PathParam("hash") String hash) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI("", PathUtils.concatPath("/", path));
            boolean prefetch = schedulePrefetch(objectUri, hash);
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(prefetch);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public boolean schedulePrefetch(DataObjectURI uri, String hash) throws IOException {
        if(uri == null) {
            throw new IllegalArgumentException("uri is null");
        }
        
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        try {
            StargateService service = getStargateService();
            TransportManager transportManager = service.getTransportManager();
            return transportManager.schedulePrefetch(uri, hash);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @GET
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_GET_DATA_EXPORT_ENTRY_PATH + "/{path:.*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getDataExportEntryRestful(
            @DefaultValue("") @PathParam("path") String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI("", PathUtils.concatPath("/", path));
            DataExportEntry mapping = getDataExportEntry(objectUri);    
            RestfulResponse<DataExportEntry> rres = new RestfulResponse<DataExportEntry>(mapping);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<DataExportEntry> rres = new RestfulResponse<DataExportEntry>(ex);
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
    public Response listDataExportEntriesRestful() {
        try {
            Collection<String> dataExportEntries = listDataExportEntries();    
            RestfulResponse<Collection<String>> rres = new RestfulResponse<Collection<String>>(dataExportEntries);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Collection<String>> rres = new RestfulResponse<Collection<String>>(ex);
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
    public Response getDataExportEntriesRestful() {
        try {
            Collection<DataExportEntry> dataExportEntries = getDataExportEntries();    
            RestfulResponse<Collection<DataExportEntry>> rres = new RestfulResponse<Collection<DataExportEntry>>(dataExportEntries);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Collection<DataExportEntry>> rres = new RestfulResponse<Collection<DataExportEntry>>(ex);
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
    public Response addDataExportEntryRestful(DataExportEntry entry) {
        try {
            addDataExportEntry(entry);
            
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(rres).build();
        }
    }
    
    @Override
    public void addDataExportEntry(DataExportEntry entry) throws IOException {
        try {
            StargateService service = getStargateService();
            DataExportManager dataExportManager = service.getDataExportManager();
            dataExportManager.addDataExportEntry(entry);
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        } catch (DataExportManagerException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    @DELETE
    @Path(HTTPUserInterfaceRestfulConstants.API_PATH + "/" + HTTPUserInterfaceRestfulConstants.API_REMOVE_DATA_EXPORT_ENTRY_PATH + "/{path: .*}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response removeDataExportEntryRestful(
            @DefaultValue("") @PathParam("path") String path) {
        if(path == null || path.isEmpty()) {
            throw new IllegalArgumentException("path is null or empty");
        }
        
        try {
            DataObjectURI objectUri = new DataObjectURI("", PathUtils.concatPath("/", path));
            removeDataExportEntry(objectUri);
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(true);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Boolean> rres = new RestfulResponse<Boolean>(ex);
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
    public Response listDataSourcesRestful() {
        try {
            Collection<String> dataSources = listDataSources();    
            RestfulResponse<Collection<String>> rres = new RestfulResponse<Collection<String>>(dataSources);
            return Response.status(Response.Status.OK).entity(rres).build();
        } catch(Exception ex) {
            RestfulResponse<Collection<String>> rres = new RestfulResponse<Collection<String>>(ex);
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
