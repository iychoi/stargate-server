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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
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
import stargate.commons.datastore.DataStoreProperties;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.manager.AbstractManager;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.AbstractRecipeDriver;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.utils.DateTimeUtils;
import stargate.managers.cluster.ClusterManager;
import stargate.managers.dataexport.DataExportManager;
import stargate.managers.datasource.DataSourceManager;
import stargate.managers.datastore.DataStoreManager;
import stargate.managers.schedule.ScheduleManager;
import stargate.managers.statistics.StatisticsManager;
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
    private final Object recipeStoreSyncObj = new Object();
    private final Object parallelRecipeCreationSyncObj = new Object();
    protected long lastUpdateTime;
    
    private static final String RECIPE_STORE = "recipe";
    private static final String HASH_STORE = "hash";
    
    public static RecipeManager getInstance(StargateService service, RecipeManagerConfig config, Collection<AbstractRecipeDriver> drivers) throws ManagerNotInstantiatedException {
        synchronized (RecipeManager.class) {
            if(instance == null) {
                instance = new RecipeManager(service, config, drivers);
            }
            return instance;
        }
    }
    
    public static RecipeManager getInstance(StargateService service, RecipeManagerConfig config) throws ManagerNotInstantiatedException {
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
                    instance = new RecipeManager(service, config, recipeDrivers);
                } catch (DriverFailedToLoadException ex) {
                    LOG.error("Could not load driver", ex);
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
    
    RecipeManager(StargateService service, RecipeManagerConfig config, Collection<AbstractRecipeDriver> drivers) throws ManagerNotInstantiatedException {
        if(service == null) {
            throw new IllegalArgumentException("service is null");
        }
        
        if(config == null) {
            throw new IllegalArgumentException("config is null");
        }
        
        if(drivers == null || drivers.isEmpty()) {
            throw new IllegalArgumentException("drivers is null or empty");
        }
        
        this.setService(service);
        this.setConfig(config);
        
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
    
    private void safeInitRecipeStore() throws IOException {
        synchronized(this.recipeStoreSyncObj) {
            if(this.recipeStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    
                    RecipeManagerConfig managerConfig = (RecipeManagerConfig) this.getConfig();
                    
                    DataStoreProperties properties = new DataStoreProperties();
                    properties.setSharded(true);
                    properties.setReplicaNum(managerConfig.getRecipeReplicaNum());
                    properties.setPersistent(true);
                    this.recipeStore = keyValueStoreManager.getDriver().getKeyValueStore(RECIPE_STORE, Recipe.class, properties);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error("Manager is not instantiated", ex);
                    throw new IOException(ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error("Driver is not initialized", ex);
                    throw new IOException(ex);
                }
            }

            if(this.hashStore == null) {
                try {
                    StargateService stargateService = getStargateService();
                    DataStoreManager keyValueStoreManager = stargateService.getDataStoreManager();
                    
                    RecipeManagerConfig managerConfig = (RecipeManagerConfig) this.getConfig();
                    
                    DataStoreProperties properties = new DataStoreProperties();
                    properties.setSharded(true);
                    properties.setReplicaNum(managerConfig.getRecipeReplicaNum());
                    properties.setPersistent(true);
                    this.hashStore = keyValueStoreManager.getDriver().getKeyValueStore(HASH_STORE, ReverseRecipeMapping.class, properties);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error("Manager is not instantiated", ex);
                    throw new IOException(ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error("Driver is not initialied", ex);
                    throw new IOException(ex);
                }
            }
        }
    }
    
    private void addHashes(Recipe recipe) throws IOException {
        Collection<RecipeChunk> chunks = recipe.getChunks();
        
        synchronized(this.recipeStoreSyncObj) {
            for(RecipeChunk chunk : chunks) {
                String hash = chunk.getHash();

                ReverseRecipeMapping mapping = (ReverseRecipeMapping) this.hashStore.get(hash);
                if(mapping == null) {
                    mapping = new ReverseRecipeMapping(hash);
                }

                mapping.addRecipeName(recipe.getMetadata().getURI().getPath());

                // update
                this.hashStore.put(hash, mapping);
            }
        }
    }
    
    private void removeHashes(Recipe recipe) throws IOException {
        Collection<RecipeChunk> chunks = recipe.getChunks();
        
        synchronized(this.recipeStoreSyncObj) {
            for(RecipeChunk chunk : chunks) {
                String hash = chunk.getHash();

                ReverseRecipeMapping mapping = (ReverseRecipeMapping) this.hashStore.get(hash);
                if(mapping != null) {
                    boolean result = mapping.removeRecipeName(recipe.getMetadata().getURI().getPath());

                    if(result) {
                        if(mapping.getRecipeNameCount() == 0) {
                            this.hashStore.remove(hash);
                        } else {
                            // update - modified
                            this.hashStore.put(hash, mapping);
                        }
                    }
                }
            }
        }
    }
    
    private Collection<String> getHashes(Collection<RecipeChunk> recipeChunks) {
        List<String> hashes = new ArrayList<String>();
        for(RecipeChunk chunk : recipeChunks) {
            hashes.add(chunk.getHash());
        }
        return hashes;
    }
    
    private void updateHashes(Recipe oldRecipe, Recipe newRecipe) throws IOException {
        Collection<RecipeChunk> oldChunks = oldRecipe.getChunks();
        Collection<RecipeChunk> newChunks = newRecipe.getChunks();
        
        Collection<String> oldHashes = getHashes(oldChunks);
        Collection<String> newHashes = getHashes(newChunks);
        
        // to be removed
        Collection<String> toBeRemovedHashes = getHashes(oldChunks);
        toBeRemovedHashes.removeAll(newHashes);
        
        synchronized(this.recipeStoreSyncObj) {
            for(String hash : toBeRemovedHashes) {
                ReverseRecipeMapping mapping = (ReverseRecipeMapping) this.hashStore.get(hash);
                if(mapping != null) {
                    boolean result = mapping.removeRecipeName(oldRecipe.getMetadata().getURI().getPath());

                    if(result) {
                        if(mapping.getRecipeNameCount() == 0) {
                            this.hashStore.remove(hash);
                        } else {
                            // update - modified
                            this.hashStore.put(hash, mapping);
                        }
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
    }
    
    public Collection<Recipe> getRecipes() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        synchronized(this.recipeStoreSyncObj) {
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
    }
    
    public Collection<String> getRecipeKeys() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        synchronized(this.recipeStoreSyncObj) {
            return Collections.unmodifiableCollection(this.recipeStore.keys());
        }
    }
    
    public Recipe getRecipe(String stargatePath) throws IOException {
        if(stargatePath == null || stargatePath.isEmpty()) {
            throw new IllegalArgumentException("stargatePath is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        synchronized(this.recipeStoreSyncObj) {
            return (Recipe) this.recipeStore.get(stargatePath);
        }
    }
    
    public boolean hasRecipe(String stargatePath) throws IOException {
        if(stargatePath == null || stargatePath.isEmpty()) {
            throw new IllegalArgumentException("stargatePath is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        synchronized(this.recipeStoreSyncObj) {
            return this.recipeStore.containsKey(stargatePath);
        }
    }
    
    public Recipe getRecipeByHash(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        synchronized(this.recipeStoreSyncObj) {
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
    }
    
    public boolean hasRecipeByHash(String hash) throws IOException {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        synchronized(this.recipeStoreSyncObj) {
            return this.hashStore.containsKey(hash);
        }
    }
    
    public void clearRecipes() throws IOException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        synchronized(this.recipeStoreSyncObj) {
            Collection<String> keys = this.recipeStore.keys();
            for(String key : keys) {
                // this is to raise a recipe removal event
                removeRecipe(key);
            }
        }
    }
    
    public void addRecipes(Collection<Recipe> recipes) throws RecipeManagerException, IOException {
        if(recipes == null) {
            throw new IllegalArgumentException("recipes is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        List<Recipe> failed = new ArrayList<Recipe>();
        
        synchronized(this.recipeStoreSyncObj) {
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
    }
    
    public void addRecipe(Recipe recipe) throws RecipeManagerException, IOException {
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        String key = recipe.getMetadata().getURI().getPath();
        
        synchronized(this.recipeStoreSyncObj) {
            if(this.recipeStore.containsKey(key)) {
                throw new RecipeManagerException(String.format("recipe %s is already added", key));
            }

            this.recipeStore.put(key, recipe);

            addHashes(recipe);

            this.lastUpdateTime = DateTimeUtils.getTimestamp();
        }
    }
    
    public void removeRecipe(Recipe recipe) throws IOException {
        if(recipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        synchronized(this.recipeStoreSyncObj) {
            removeRecipe(recipe.getMetadata().getURI().getPath());
        }
    }
    
    public void removeRecipe(String stargatePath) throws IOException {
        if(stargatePath == null || stargatePath.isEmpty()) {
            throw new IllegalArgumentException("stargatePath is null or empty");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        synchronized(this.recipeStoreSyncObj) {
            Recipe recipe = (Recipe) this.recipeStore.get(stargatePath);
            if(recipe != null) {
                this.recipeStore.remove(stargatePath);

                removeHashes(recipe);

                this.lastUpdateTime = DateTimeUtils.getTimestamp();
            }
        }
    }
    
    public void updateRecipe(Recipe newRecipe) throws IOException {
        if(newRecipe == null) {
            throw new IllegalArgumentException("recipe is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        String key = newRecipe.getMetadata().getURI().getPath();
        
        synchronized(this.recipeStoreSyncObj) {
            Recipe oldRecipe = (Recipe) this.recipeStore.get(key);
            this.recipeStore.put(key, newRecipe);

            this.lastUpdateTime = DateTimeUtils.getTimestamp();

            if(oldRecipe == null) {
                addHashes(newRecipe);
            } else {
                updateHashes(oldRecipe, newRecipe);
            }
        }
    }
    
    public void syncRecipes() throws IOException, FileNotFoundException, ManagerNotInstantiatedException, RecipeManagerException, DriverNotInitializedException {
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        safeInitRecipeStore();
        
        StargateService stargateService = getStargateService();
        DataExportManager dataExportManager = stargateService.getDataExportManager();
        DataSourceManager dataSourceManager = stargateService.getDataSourceManager();
        
        synchronized(this.recipeStoreSyncObj) {
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
    }
    
    public Recipe createRecipe(DataExportEntry entry) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(entry == null) {
            throw new IllegalArgumentException("entry is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        LOG.info(String.format("createRecipe - %s", entry.getStargatePath()));
        //return createRecipeLocal(entry);
        return createRecipeParallel(entry);
    }
    
    private Recipe createRecipeLocal(DataExportEntry entry) throws IOException, FileNotFoundException, DriverNotInitializedException {
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
            
            InputStream is = dataSourceDriver.openFile(sourceMetadata.getURI());
            Recipe recipe = new Recipe(objMetadata, driver.getHashAlgorithm(), chunkSize, cluster.getNodeNames());

            // create recipe chunks
            Collection<RecipeChunk> chunks = driver.produceRecipeChunks(is);
            for(RecipeChunk chunk : chunks) {
                chunk.setOffset(offset);
                
                // update host
                Collection<String> blockLocations = dataSourceDriver.listBlockLocations(cluster, sourceMetadata.getURI(), offset, Math.min(chunkSize, chunk.getLength()));
                if(blockLocations.contains("*")) {
                    chunk.setAccessibleFromAllNode();
                } else {
                    for(String nodeName : blockLocations) {
                        int nodeID = recipe.getNodeID(nodeName);
                        chunk.addNodeID(nodeID);
                    }
                }
                
                // add to recipe
                recipe.addChunk(chunk);
                offset += chunkSize;
            }
            
            is.close();
            return recipe;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    public RecipeChunk createRecipeChunk(RecipeChunkGenerationTaskParameter event) throws IOException, FileNotFoundException, DriverNotInitializedException {
        if(event == null) {
            throw new IllegalArgumentException("event is null");
        }
        
        if(!this.started) {
            throw new IllegalStateException("Manager is not started");
        }
        
        try {
            DataExportEntry dataExportEntry = event.getDataExportEntry();
            SourceFileMetadata sourceFileMetadata = event.getSourceFileMetadata();
            
            StargateService stargateService = getStargateService();
            DataSourceManager dataSourceManager = stargateService.getDataSourceManager();
            StatisticsManager statisticsManager = stargateService.getStatisticsManager();
            
            AbstractDataSourceDriver dataSourceDriver = dataSourceManager.getDriver(dataExportEntry.getSourceURI());
            
            // create recipe chunk
            AbstractRecipeDriver driver = getDriver();

            InputStream is = dataSourceDriver.openFile(sourceFileMetadata.getURI(), event.getOffset(), sourceFileMetadata.getFileSize());
            
            // create recipe chunk
            RecipeChunk recipeChunk = driver.produceRecipeChunk(is);
            recipeChunk.setOffset(event.getOffset());
            
            is.close();
            
            statisticsManager.addRecipeChunkCreationStatistics(sourceFileMetadata.getURI().toASCIIString(), recipeChunk.getOffset(), recipeChunk.getLength(), recipeChunk.getHash());
            
            return recipeChunk;
        } catch (ManagerNotInstantiatedException ex) {
            LOG.error("Manager is not instantiated", ex);
            throw new IOException(ex);
        }
    }
    
    private Recipe createRecipeParallel(DataExportEntry entry) throws IOException, FileNotFoundException, DriverNotInitializedException {
        
        synchronized(this.parallelRecipeCreationSyncObj) {
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
                List<RecipeChunkGenerationTaskParameter> anyNodeTasks = new ArrayList<RecipeChunkGenerationTaskParameter>();

                int numNodes = nodeNames.size();
                List<RecipeChunkGenerationTaskParameter> nodeTasks[] = new List[numNodes];
                for(int i=0;i<numNodes;i++) {
                    nodeTasks[i] = new ArrayList<RecipeChunkGenerationTaskParameter>();
                }

                while(remaining > 0) {
                    int blockSize = (int) Math.min(chunkSize, remaining);
                    RecipeChunkGenerationTaskParameter parameter = new RecipeChunkGenerationTaskParameter(entry, sourceMetadata, offset, blockSize);

                    Collection<String> blockLocations = dataSourceDriver.listBlockLocations(cluster, sourceMetadata.getURI(), offset, chunkSize);
                    if(blockLocations.contains("*")) {
                        // distribute to any
                        anyNodeTasks.add(parameter);
                    } else {
                        // simply use the primary host
                        for(String nodeName : blockLocations) {
                            int nodeID = recipe.getNodeID(nodeName);
                            if(nodeID >= 0) {
                                nodeTasks[nodeID].add(parameter);
                            } else {
                                anyNodeTasks.add(parameter);
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
                            RecipeChunkGenerationTaskParameter event = anyNodeTasks.remove(0);
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

                Iterator<String> nodeNameIterator = nodeNames.iterator();
                int nodeId = 0;

                List<RecipeChunkGenerationTask> recipeChunkGenerateTasks = new ArrayList<RecipeChunkGenerationTask>();
                while(nodeNameIterator.hasNext()) {
                    String nodeName = nodeNameIterator.next();

                    if(nodeTasks[nodeId].size() > 0) {
                        List<String> nodeNameList = new ArrayList<String>();
                        nodeNameList.add(nodeName);

                        RecipeChunkGenerationTask recipeChunkGenerateTask = new RecipeChunkGenerationTask(nodeNameList, nodeTasks[nodeId]);
                        scheduleManager.scheduleTask(recipeChunkGenerateTask);

                        recipeChunkGenerateTasks.add(recipeChunkGenerateTask);
                    }

                    nodeId++;
                }

                // collect
                List<RecipeChunk> collectedRecipeChunks = new ArrayList<RecipeChunk>();

                for(RecipeChunkGenerationTask task : recipeChunkGenerateTasks) {
                    Collection<RecipeChunk> recipeChunks = task.getRecipeChunks();
                    for(RecipeChunk recipeChunk : recipeChunks) {
                        LOG.debug(String.format("Received chunk - %s", recipeChunk.toString()));

                        Collection<String> blockLocations = dataSourceDriver.listBlockLocations(cluster, sourceMetadata.getURI(), recipeChunk.getOffset(), Math.min(chunkSize, recipeChunk.getLength()));
                        if(blockLocations.contains("*")) {
                            LOG.debug("block location of recipechunk has *");
                            recipeChunk.setAccessibleFromAllNode();
                        } else {
                            for(String nodeName : blockLocations) {
                                int nodeID = recipe.getNodeID(nodeName);
                                if(nodeID >= 0) {
                                    LOG.debug(String.format("block location of recipechunk is mapped to a node : %d", nodeID));
                                    recipeChunk.addNodeID(nodeID);
                                } else {
                                    LOG.debug("block location of recipechunk cannot be mapped to a node");
                                    recipeChunk.setAccessibleFromAllNode();
                                }
                            }
                        }

                        collectedRecipeChunks.add(recipeChunk);
                    }
                }

                // sort
                Collections.sort(collectedRecipeChunks, new Comparator<RecipeChunk>() {
                    @Override
                    public int compare(RecipeChunk c1, RecipeChunk c2) {
                        long diff = c1.getOffset() - c2.getOffset();
                        if(diff < 0) {
                            return -1;
                        } else if(diff == 0) {
                            return 0;
                        } else {
                            return 1;
                        }
                    } 
                });

                for(RecipeChunk recipeChunk : collectedRecipeChunks) {
                    recipe.addChunk(recipeChunk);
                }

                return recipe;
            } catch (ManagerNotInstantiatedException ex) {
                LOG.error("Manager is not instantiated", ex);
                throw new IOException(ex);
            }
        }
    }
    
    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
    
    public void setLastUpdateTime(long time) {
        if(time < 0) {
            throw new IllegalArgumentException("time is negative");
        }
        
        this.lastUpdateTime = time;
    }
}
