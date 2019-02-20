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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.driver.DriverNotInitializedException;
import stargate.commons.manager.ManagerNotInstantiatedException;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.schedule.DistributedTask;
import stargate.commons.service.ServiceNotStartedException;
import stargate.service.StargateService;

/**
 *
 * @author iychoi
 */
public class RecipeChunkGenerateTask extends DistributedTask {
    
    private static final Log LOG = LogFactory.getLog(RecipeChunkGenerateTask.class);
    
    class RecipeChunkGenerateTaskCallable implements Callable<Collection<RecipeChunk>> {

        private Collection<RecipeChunkGenerateTaskParameter> parameters;
                
        RecipeChunkGenerateTaskCallable(Collection<RecipeChunkGenerateTaskParameter> parameters) {
            if(parameters == null) {
                throw new IllegalArgumentException("parameters is null");
            }
            
            this.parameters = parameters;
        }

        @Override
        public Collection<RecipeChunk> call() {
            List<RecipeChunk> generatedChunks = new ArrayList<RecipeChunk>();
            
            for(RecipeChunkGenerateTaskParameter parameter : this.parameters) {
                LOG.debug(String.format("Processing chunk generation request for - %s", parameter.toString()));
                try {
                    StargateService stargateInstance = StargateService.getInstance();
                    RecipeManager recipeManager = stargateInstance.getRecipeManager();
                    RecipeChunk recipeChunk = recipeManager.createRecipeChunk(parameter);
                    LOG.debug(String.format("Generated chunk %s - %s", parameter.getDataExportEntry().getSourceURI().toASCIIString(), recipeChunk.getHash()));
                    generatedChunks.add(recipeChunk);
                } catch (ServiceNotStartedException ex) {
                    LOG.error("Service is not started", ex);
                } catch (ManagerNotInstantiatedException ex) {
                    LOG.error("Manager is not instantiated", ex);
                } catch (IOException ex) {
                    LOG.error("IOException", ex);
                } catch (DriverNotInitializedException ex) {
                    LOG.error("Driver is not initialized", ex);
                }
            }
            return generatedChunks;
        }
    }
    
    public RecipeChunkGenerateTask(Collection<String> nodeNames, Collection<RecipeChunkGenerateTaskParameter> parameters) {
        super("RecipeChunkGenerateTask", parameters, nodeNames);
        
        RecipeChunkGenerateTaskCallable runnable = new RecipeChunkGenerateTaskCallable(parameters);
        super.setCallable(runnable);
    }
    
    public Collection<RecipeChunk> getRecipeChunks() throws IOException {
        Future<?> future = super.getFuture();
        
        try {
            // wait
            Collection<Collection<RecipeChunk>> chunks = (Collection<Collection<RecipeChunk>>) future.get();
            LOG.debug("Waiting for the task is done");
            
            List<RecipeChunk> generatedChunks = new ArrayList<RecipeChunk>();
            for(Collection<RecipeChunk> cchunks : chunks) {
                generatedChunks.addAll(cchunks);
            }
            
            return generatedChunks;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
