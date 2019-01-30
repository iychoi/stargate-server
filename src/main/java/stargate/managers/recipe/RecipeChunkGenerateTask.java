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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.schedule.Task;

/**
 *
 * @author iychoi
 */
public class RecipeChunkGenerateTask extends Task {
    
    private static final Log LOG = LogFactory.getLog(RecipeChunkGenerateTask.class);
    
    class RecipeChunkGenerateTaskRunnable implements Runnable {

        private Collection<RecipeChunkGenerateEvent> events;
        private List<RecipeChunk> generatedChunks = new ArrayList<RecipeChunk>();
                
        RecipeChunkGenerateTaskRunnable(Collection<RecipeChunkGenerateEvent> events) {
            this.events = events;
        }

        @Override
        public void run() {
            for(RecipeChunkGenerateEvent event : this.events) {
                LOG.info(event.toString());
                
            }
        }
        
        public Collection<RecipeChunk> getRecipeChunks() {
            return Collections.unmodifiableCollection(this.generatedChunks);
        }
    }
    
    public RecipeChunkGenerateTask(Collection<String> nodeNames, Collection<RecipeChunkGenerateEvent> events) {
        super("RecipeChunkGenerateTask", null, events, nodeNames);
        
        RecipeChunkGenerateTaskRunnable runnable = new RecipeChunkGenerateTaskRunnable(events);
        super.setRunnable(runnable);
    }
    
    public Collection<RecipeChunk> getRecipeChunks() throws IOException {
        Future<Object> future = super.getFuture();
        
        try {
            // wait
            future.get(); // runnable does not return
            LOG.info("Finished - future waiting");
            
            RecipeChunkGenerateTaskRunnable runnable = (RecipeChunkGenerateTaskRunnable) super.getRunnable();
            return runnable.getRecipeChunks();
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }
}
