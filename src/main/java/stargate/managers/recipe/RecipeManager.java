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
package stargate.managers.recipe;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.AbstractClusterDriver;
import stargate.commons.cluster.Cluster;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.datasource.AbstractDataSourceDriver;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.datasource.SourceFileMetadata;
import stargate.commons.driver.AbstractDriver;
import stargate.commons.driver.DriverFailedToLoadException;
import stargate.commons.datastore.AbstractKeyValueStore;
import stargate.commons.datastore.EnumDataStoreProperty;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerConfig;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.AbstractRecipeDriver;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.schedule.AbstractScheduleDriver;
import stargate.commons.utils.DateTimeUtils;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.dataexport.DataExportManager;
import stargate.managers.datasource.DataSourceManager;
import stargate.managers.datastore.DataStoreManager;
import stargate.managers.schedule.ScheduleManager;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class RecipeManager extends AbstractManager<AbstractRecipeDriver> {

    private static final Log LOG = LogFactory.getLog(RecipeManager.class);
    
    private static RecipeManager instance;
    
    private AbstractKeyValueStore recipeStore; // <String, Recipe>
    private AbstractKeyValueStore hashStore; // <String, ReverseRecipeMapping>
    protected long lastUpdateTime;
    
    private static final String RECIPE_STORE = "recipe";
    private static final String HASH_STORE = "hash";
    
    public static RecipeManager getInstance(StargateService service, Collection<AbstractRecipeDriver> drivers) throws ManagerNotInstantiatedException {
        synchronized (RecipeManager.class) {
            if(instance == null) {
                instance = new RecipeManager(service, drivers);
            }
            return instance;
        }
    }
    
    public static RecipeManager getInstance(StargateService service, ManagerConfig config) throws ManagerNotInstantiatedException {
        synchronized (RecipeManager.class) {
            if(instance == null) {
                if(config == null) {
                    throw new IllegalArgumentException("config is null");
                }
                
                try {
                    // type cast
                    Collection<AbstractDriver> drivers = (Collection<AbstractDriver>) config.getDrivers();
                    List<AbstractRecipeDriver> recipeDrivers = new ArrayList<AbstractRecipeDriver>();
                    for(AbstractDriver driver : drivers) {
                        recipeDrivers.add((AbstractRecipeDriver) driver);
                    }
                    instance = new RecipeManager(service, recipeDrivers);
                } catch (DriverFailedToLoadException ex) {
                    LOG.error(ex);
                    throw new ManagerNotInstantiatedException(ex.toString());
                }
            }
            return instance;
        }
    }
    
    public static RecipeManager getInstance() throws ManagerNotInstantiatedException {
        synchronized (RecipeManager.class) {
            if(instance == null) {
                throw new ManagerNotInstantiatedException("RecipeManager is not started");
            }
            return instance;
        }
    }
    
    RecipeManager(StargateService service, Collection<AbstractRecipeDriver> drivers) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(drivers == null || drivers.isEmpty()) {
            throw new IllegalArgumentException("drivers is null or empty");
        }
        
        this.setService(service);
        
        for(AbstractRecipeDriver driver : drivers) {
            this.drivers.add(driver);
        }
    }
    
    public AbstractRecipeDriver getDriver() {
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
    }
    
    @Override
    public synchronized void stop() throws IOException {
        super.stop();
    }
    
    private synchronized void safeInitRecipeStore() throws IOException {
        if(this.recipeStore == null) {
            try {
                StargateService stargateService = getStargateService();
                DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                this.recipeStore = keyValueStoreManager.getDriver().getKeyValueStore(RECIPE_STORE, Recipe.class, EnumDataStoreProperty.DATASTORE_PROP_PERSISTENT_DISTRIBUTED);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
    
    private synchronized void safeInitHashStore() throws IOException {
        if(this.hashStore == null) {
            try {
                StargateService stargateService = getStargateService();
                DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                this.hashStore = keyValueStoreManager.getDriver().getKeyValueStore(HASH_STORE, ReverseRecipeMapping.class, EnumDataStoreProperty.DATASTORE_PROP_PERSISTENT_DISTRIBUTED);
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error(ex);
                throw new IOException(ex);
            }
        }
    }
    
    private synchronized void addHashes(Recipe recipe) throws IOException {
        Collection<RecipeChunk> chunks = recipe.getChunks();
        
        for(RecipeChunk chunk : chunks) {
            String hash = chunk.getHashString();
            
            ReverseRecipeMapping mapping = (ReverseRecipeMapping) this.hashStore.get(hash);
            if(mapping == null) {
                mapping = new ReverseRecipeMapping(hash);
            }
            
            mapping.addRecipeName(recipe.getMetadata().getURI().getPath());
            
            // update
            this.hashStore.put(hash, mapping);
        }
    }
    
    private synchronized void removeHashes(Recipe recipe) throws IOException {
        Collection<RecipeChunk> chunks = recipe.getChunks();
        
        for(RecipeChunk chunk : chunks) {
            String hash = chunk.getHashString();
            
            ReverseRecipeMapping mapping = (ReverseRecipeMapping) this.hashStore.get(hash);
            if(mapping != null) {
                boolean result = mapping.removeRecipeName(recipe.getMetadata().getURI().getPath());
            
                if(result) {
                    // update - modified
                    this.hashStore.put(hash, mapping);
                }
            }
        }
    }
    
    private Collection<String> getHashes(Collection<RecipeChunk> recipeChunks) {
        List<String> hashes = new ArrayList<String>();
        for(RecipeChunk chunk : recipeChunks) {
            hashes.add(chunk.getHashString());
        }
        return hashes;
    }
    
    private synchronized void updateHashes(Recipe oldRecipe, Recipe newRecipe) throws IOException {
        Collection<RecipeChunk> oldChunks = oldRecipe.getChunks();
        Collection<RecipeChunk> newChunks = newRecipe.getChunks();
        
        Collection<String> oldHashes = getHashes(oldChunks);
        Collection<String> newHashes = getHashes(newChunks);
        
        // to be removed
        Collection<String> toBeRemovedHashes = getHashes(oldChunks);
        toBeRemovedHashes.removeAll(newHashes);
        
        for(String hash : toBeRemovedHashes) {
            ReverseRecipeMapping mapping = (ReverseRecipeMapping) this.hashStore.get(hash);
            if(mapping != null) {
                boolean result = mapping.removeRecipeName(oldRecipe.getMetadata().getURI().getPath());
                
                if(result) {
                    // update - modified
                    this.hashStore.put(hash, mapping);
                }
            }
        }
        
        // to be added
        newHashes.removeAll(oldHashes);
        for(String hash : newHashes) {
            ReverseRecipeMapping mapping = (ReverseRecipeMapping) this.hashStore.get(hash);
            if(mapping == null) {
                mapping = new ReverseRecipeMapping(hash);
            }

            mapping.addRecipeName(oldRecipe.getMetadata().getURI().getPath());

            // update
            this.hashStore.put(hash, mapping);
        }
    }
    
    public synchronized Collection<Recipe> getRecipes() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        List<Recipe> recipes = new ArrayList<Recipe>();
        Collection<String> keys = this.recipeStore.keys();
        for(String key : keys) {
            Recipe recipe = (Recipe) this.recipeStore.get(key);
            if(recipe != null) {
                recipes.add(recipe);
            }
        }
        
        return Collections.unmodifiableCollection(recipes);
    }
    
    public synchronized Collection<String> getRecipeKeys() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        return Collections.unmodifiableCollection(this.recipeStore.keys());
    }
    
    public synchronized Recipe getRecipe(String stargatePath) throws IOException {
        if(stargatePath == null || stargatePath.isEmpty()) {
            throw new IllegalArgumentException("stargatePath is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        return (Recipe) this.recipeStore.get(stargatePath);
    }
    
    public synchronized boolean hasRecipe(String stargatePath) throws IOException {
        if(stargatePath == null || stargatePath.isEmpty()) {
            throw new IllegalArgumentException("stargatePath is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        return this.recipeStore.containsKey(stargatePath);
    }
    
    public synchronized Recipe getRecipeByHash(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitHashStore();
        safeInitRecipeStore();
        
        ReverseRecipeMapping reverseRecipeMapping = (ReverseRecipeMapping) this.hashStore.get(hash);
        if(reverseRecipeMapping == null) {
            return null;
        }
        
        Collection<String> recipeNames = reverseRecipeMapping.getRecipeNames();
        for(String recipeName : recipeNames) {
            Recipe recipe = (Recipe) this.recipeStore.get(recipeName);
            RecipeChunk chunk = recipe.getChunk(hash);
            if(chunk != null) {
                return recipe;
            }
        }
        
        return null;
    }
    
    public synchronized boolean hasRecipeByHash(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitHashStore();
        
        return this.hashStore.containsKey(hash);
    }
    
    public synchronized void clearRecipes() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        Collection<String> keys = this.recipeStore.keys();
        for(String key : keys) {
            // this is to raise a recipe removal event
            removeRecipe(key);
        }
    }
    
    public synchronized void addRecipes(Collection<Recipe> recipes) throws RecipeManagerException, IOException {
        if(recipes == null) {
            throw new IllegalArgumentException("recipes is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        safeInitHashStore();
        
        List<Recipe> failed = new ArrayList<Recipe>();
        
        for(Recipe recipe : recipes) {
            try {
                addRecipe(recipe);
            } catch(RecipeManagerException ex) {
                failed.add(recipe);
            }
        }
        
        if(!failed.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for(Recipe recipe : failed) {
                if(sb.length() > 0) {
                    sb.append(",");
                }
                sb.append(recipe.getMetadata().getURI().getPath());
            }
            throw new RecipeManagerException(String.format("recipes (%s) cannot be added (maybe already exist?)", sb.toString()));
        }
    }
    
    public synchronized void addRecipe(Recipe recipe) throws RecipeManagerException, IOException {
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        safeInitHashStore();
        
        String key = recipe.getMetadata().getURI().getPath();
        
        if(this.recipeStore.containsKey(key)) {
            throw new RecipeManagerException(String.format("recipe %s is already added", key));
        }
        
        this.recipeStore.put(key, recipe);
        
        addHashes(recipe);
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
    }
    
    public synchronized void removeRecipe(Recipe recipe) throws IOException {
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        safeInitHashStore();
        
        removeRecipe(recipe.getMetadata().getURI().getPath());
    }
    
    public synchronized void removeRecipe(String stargatePath) throws IOException {
        if(stargatePath == null || stargatePath.isEmpty()) {
            throw new IllegalArgumentException("stargatePath is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        safeInitHashStore();
        
        Recipe recipe = (Recipe) this.recipeStore.get(stargatePath);
        if(recipe != null) {
            this.recipeStore.remove(stargatePath);
            
            removeHashes(recipe);
            
            this.lastUpdateTime = DateTimeUtils.getTimestamp();
        }
    }
    
    public synchronized void updateRecipe(Recipe newRecipe) throws IOException {
        if(newRecipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        safeInitHashStore();
        
        String key = newRecipe.getMetadata().getURI().getPath();
        
        Recipe oldRecipe = (Recipe) this.recipeStore.get(key);
        this.recipeStore.put(key, newRecipe);
        
        this.lastUpdateTime = DateTimeUtils.getTimestamp();
        
        if(oldRecipe == null) {
            addHashes(newRecipe);
        } else {
            updateHashes(oldRecipe, newRecipe);
        }
    }
    
    public synchronized void syncRecipes() throws IOException, ManagerNotInstantiatedException, RecipeManagerException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        StargateService stargateService = getStargateService();
        DataExportManager dataExportManager = stargateService.getDataExportManager();
        DataSourceManager dataSourceManager = stargateService.getDataSourceManager();
        
        // find missing recipes
        Collection<String> recipeKeys = this.recipeStore.keys();
        
        Collection<DataExportEntry> dataExportEntries = dataExportManager.getDataExportEntries();
        for(DataExportEntry entry : dataExportEntries) {
            if(!recipeKeys.contains(entry.getStargatePath())) {
                // recipe does not exist
                Recipe recipe = createRecipe(entry);
                addRecipe(recipe);
            } else {
                //check stale recipe
                Recipe recipe = (Recipe) this.recipeStore.get(entry.getStargatePath());
                if(recipe == null) {
                    removeRecipe(entry.getStargatePath());
                    continue;
                }
                
                AbstractDataSourceDriver dataSourceDriver = dataSourceManager.getDriver(entry.getSourceURI());
                
                DataObjectMetadata recipeMetadata = recipe.getMetadata();
                SourceFileMetadata sourceFileMetadata = dataSourceDriver.getMetadata(entry.getSourceURI());
                if(recipeMetadata == null || sourceFileMetadata == null) {
                    removeRecipe(entry.getStargatePath());
                    continue;
                }
                
                if(recipeMetadata.getLastModifiedTime() != sourceFileMetadata.getLastModifiedTime() ||
                        recipeMetadata.getSize() != sourceFileMetadata.getFileSize()) {
                    removeRecipe(entry.getStargatePath());
                    Recipe newRecipe = createRecipe(entry);
                    addRecipe(newRecipe);
                }
            }
        }
        
        recipeKeys = this.recipeStore.keys();
        
        // remove broken recipes
        List<String> brokenRecipeList = new ArrayList<String>();
        brokenRecipeList.addAll(recipeKeys);
        for(DataExportEntry entry : dataExportEntries) {
            brokenRecipeList.remove(entry.getStargatePath());
        }
        
        for(String key : brokenRecipeList) {
            removeRecipe(key);
        }
    }
    
    public synchronized long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public synchronized void setLastUpdateTime(long time) {
        this.lastUpdateTime = time;
    }
    
    public Recipe createRecipe(DataExportEntry entry) throws IOException {
        //return createRecipeLocal(entry);
        return createRecipeParallel(entry);
    }
    
    private Recipe createRecipeLocal(DataExportEntry entry) throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        LOG.info(String.format("createRecipe - %s", entry.getStargatePath()));
        try {
            StargateService stargateService = getStargateService();

            ClusterManager clusterManager = stargateService.getClusterManager();
            AbstractClusterDriver clusterDriver = clusterManager.getDriver();
            Cluster cluster = clusterDriver.getLocalCluster();

            DataSourceManager dataSourceManager = stargateService.getDataSourceManager();
            AbstractDataSourceDriver dataSourceDriver = dataSourceManager.getDriver(entry.getSourceURI());
            SourceFileMetadata sourceMetadata = dataSourceDriver.getMetadata(entry.getSourceURI());

            DataObjectURI dataObjectURI = new DataObjectURI(cluster.getName(), entry.getStargatePath());
            DataObjectMetadata objMetadata = new DataObjectMetadata(dataObjectURI, sourceMetadata.getFileSize(), sourceMetadata.isDirectory(), sourceMetadata.getLastModifiedTime());

            // create recipe
            AbstractRecipeDriver driver = getDriver();

            long offset = 0;
            int chunkSize = driver.getChunkSize();
            RecipeChunk recipeChunk = null;

            InputStream is = dataSourceDriver.openFile(sourceMetadata.getURI());
            Recipe recipe = new Recipe(objMetadata, driver.getHashAlgorithm(), chunkSize, cluster.getNodeNames());

            // create recipe chunks
            while((recipeChunk = driver.produceRecipeChunk(is, offset)) != null) {
                // update host
                Collection<String> blockLocations = dataSourceDriver.listBlockLocations(cluster, sourceMetadata.getURI(), offset, chunkSize);
                if(blockLocations.contains("*")) {
                    recipeChunk.setAccessibleFromAllNode();
                } else {
                    for(String nodeName : blockLocations) {
                        int nodeID = recipe.getNodeID(nodeName);
                        recipeChunk.addNodeID(nodeID);
                    }
                }

                // add to recipe
                recipe.addChunk(recipeChunk);
                offset += recipeChunk.getLength();
            }

            is.close();
            return recipe;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    public RecipeChunk createRecipeChunk(RecipeChunkGenerateEvent event) throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        try {
            DataExportEntry dataExportEntry = event.getDataExportEntry();
            SourceFileMetadata sourceFileMetadata = event.getSourceFileMetadata();
            
            StargateService stargateService = getStargateService();
            DataSourceManager dataSourceManager = stargateService.getDataSourceManager();
            AbstractDataSourceDriver dataSourceDriver = dataSourceManager.getDriver(dataExportEntry.getSourceURI());
            
            // create recipe chunk
            AbstractRecipeDriver driver = getDriver();

            InputStream is = dataSourceDriver.openFile(sourceFileMetadata.getURI(), event.getOffset(), event.getLength());
            // create recipe chunks
            RecipeChunk recipeChunk = driver.produceRecipeChunk(is, 0);
            recipeChunk.setOffset(event.getOffset());
            
            is.close();
            
            return recipeChunk;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
    
    private synchronized Recipe createRecipeParallel(DataExportEntry entry) throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        LOG.info(String.format("createRecipe - %s", entry.getStargatePath()));
        try {
            StargateService stargateService = getStargateService();

            ClusterManager clusterManager = stargateService.getClusterManager();
            AbstractClusterDriver clusterDriver = clusterManager.getDriver();
            Cluster cluster = clusterDriver.getLocalCluster();

            DataSourceManager dataSourceManager = stargateService.getDataSourceManager();
            AbstractDataSourceDriver dataSourceDriver = dataSourceManager.getDriver(entry.getSourceURI());
            SourceFileMetadata sourceMetadata = dataSourceDriver.getMetadata(entry.getSourceURI());

            DataObjectURI dataObjectURI = new DataObjectURI(cluster.getName(), entry.getStargatePath());
            DataObjectMetadata objMetadata = new DataObjectMetadata(dataObjectURI, sourceMetadata.getFileSize(), sourceMetadata.isDirectory(), sourceMetadata.getLastModifiedTime());

            // create recipe
            AbstractRecipeDriver driver = getDriver();

            int chunkSize = driver.getChunkSize();
            Collection<String> nodeNames = cluster.getNodeNames();
            
            Recipe recipe = new Recipe(objMetadata, driver.getHashAlgorithm(), chunkSize, nodeNames);
            long offset = 0;
            long remaining = sourceMetadata.getFileSize();
            int recipeChunkNum = 0;
            
            // distribute tasks
            List<RecipeChunkGenerateEvent> anyNodeTasks = new ArrayList<RecipeChunkGenerateEvent>();
            
            int numNodes = nodeNames.size();
            List<RecipeChunkGenerateEvent> nodeTasks[] = new List[numNodes];
            for(int i=0;i<numNodes;i++) {
                nodeTasks[i] = new ArrayList<RecipeChunkGenerateEvent>();
            }
            
            while(remaining > 0) {
                int blockSize = (int) Math.min(chunkSize, remaining);
                RecipeChunkGenerateEvent event = new RecipeChunkGenerateEvent(entry, sourceMetadata, offset, blockSize);
                
                Collection<String> blockLocations = dataSourceDriver.listBlockLocations(cluster, sourceMetadata.getURI(), offset, chunkSize);
                if(blockLocations.contains("*")) {
                    // distribute to any
                    anyNodeTasks.add(event);
                } else {
                    // simply use the primary host
                    for(String nodeName : blockLocations) {
                        int nodeID = recipe.getNodeID(nodeName);
                        if(nodeID >= 0) {
                            nodeTasks[nodeID].add(event);
                        } else {
                            anyNodeTasks.add(event);
                        }
                        break;
                    }
                }
                
                offset += blockSize;
                remaining -= blockSize;
                recipeChunkNum++;
            }
            
            int remainingTasks = recipeChunkNum;
            int remainingNodes = numNodes;
            int optimalTasksPerNode = remainingTasks / remainingNodes;
            
            for(int i=0;i<numNodes;i++) {
                if(nodeTasks[i].size() > optimalTasksPerNode) {
                    remainingTasks -= nodeTasks[i].size();
                    remainingNodes--;
                }
            }
            
            optimalTasksPerNode = remainingTasks / remainingNodes;
            for(int i=0;i<numNodes;i++) {
                int toAdd = optimalTasksPerNode - nodeTasks[i].size();
                for(int j=0;j<toAdd;j++) {
                    if(anyNodeTasks.size() > 0) {
                        RecipeChunkGenerateEvent event = anyNodeTasks.remove(0);
                        nodeTasks[i].add(event);
                        remainingTasks--;
                    } else {
                        break;
                    }
                }
                    
                if(anyNodeTasks.size() <= 0) {
                    break;
                }
            }
            
            // remaining
            if(anyNodeTasks.size() > 0) {
                nodeTasks[numNodes - 1].addAll(anyNodeTasks);
            }
            
            //// now launch tasks
            ScheduleManager scheduleManager = stargateService.getScheduleManager();
            AbstractScheduleDriver scheduleDriver = scheduleManager.getDriver();
            
            Iterator<String> nodeNameIterator = nodeNames.iterator();
            int nodeId = 0;
            
            List<RecipeChunkGenerateTask> recipeChunkGenerateTasks = new ArrayList<RecipeChunkGenerateTask>();
            while(nodeNameIterator.hasNext()) {
                String nodeName = nodeNameIterator.next();
                
                if(nodeTasks[nodeId].size() > 0) {
                    List<String> nodeNameList = new ArrayList<String>();
                    nodeNameList.add(nodeName);

                    RecipeChunkGenerateTask recipeChunkGenerateTask = new RecipeChunkGenerateTask(nodeNameList, nodeTasks[nodeId]);
                    scheduleDriver.scheduleTask(recipeChunkGenerateTask);

                    recipeChunkGenerateTasks.add(recipeChunkGenerateTask);
                }
                
                nodeId++;
            }
            
            
            // collect
            for(RecipeChunkGenerateTask task : recipeChunkGenerateTasks) {
                Collection<RecipeChunk> recipeChunks = task.getRecipeChunks();
                for(RecipeChunk recipeChunk : recipeChunks) {
                    LOG.info(String.format("Received chunk - %s", recipeChunk.toString()));
                    
                    Collection<String> blockLocations = dataSourceDriver.listBlockLocations(cluster, sourceMetadata.getURI(), recipeChunk.getOffset(), recipeChunk.getLength());
                    if(blockLocations.contains("*")) {
                        recipeChunk.setAccessibleFromAllNode();
                    } else {
                        for(String nodeName : blockLocations) {
                            int nodeID = recipe.getNodeID(nodeName);
                            if(nodeID >= 0) {
                                recipeChunk.addNodeID(nodeID);
                            } else {
                                recipeChunk.setAccessibleFromAllNode();
                            }
                        }
                    }

                    //TODO: need to add chunks in order
                    recipe.addChunk(recipeChunk);
                }
            }
            
            return recipe;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error(ex);
            throw new IOException(ex);
        }
    }
}
