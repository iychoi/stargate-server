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
package stargate.tasks;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.recipe.Recipe;
import stargate.managers.dataexport.AbstractDataExportEventHandler;
import stargate.managers.dataexport.DataExportManager;
import stargate.managers.recipe.RecipeManager;

/**
 *
 * @author iychoi
 */
public class DataExportUpdateEventHandler extends AbstractDataExportEventHandler {
    
    private static final Log LOG = LogFactory.getLog(DataExportUpdateEventHandler.class);
    
    private RecipeManager recipeManager;
    
    DataExportUpdateEventHandler(RecipeManager recipeManager) {
        if(recipeManager == null) {
            throw new IllegalArgumentException("recipeManager is null");
        }
        
        this.recipeManager = recipeManager;
    }
    
    @Override
    public void added(DataExportManager manager, DataExportEntry entry) {
        try {
            // generate recipe
            Recipe recipe = this.recipeManager.createRecipe(entry);
            this.recipeManager.addRecipe(recipe);
        } catch (Exception ex) {
            LOG.error(String.format("Exception occurred while creating a recipe from a data export entry - %s", entry.getSourceURI().toASCIIString()), ex);
        }
    }

    @Override
    public void removed(DataExportManager manager, DataExportEntry entry) {
        String stargatePath = entry.getStargatePath();
        try {
            this.recipeManager.removeRecipe(stargatePath);
        } catch (IOException ex) {
            LOG.error(String.format("Exception occurred while removing a recipe - %s", stargatePath), ex);
        }
    }

    @Override
    public void updated(DataExportManager manager, DataExportEntry entry) {
        try {
            // regenerate recipe
            Recipe recipe = this.recipeManager.createRecipe(entry);
            this.recipeManager.updateRecipe(recipe);
        } catch (Exception ex) {
            LOG.error(String.format("Exception occurred while creating a recipe from a data export entry - %s", entry.getSourceURI().toASCIIString()), ex);
        }
    }
}
