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

import java.io.File;
import java.io.IOException;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.manager.ManagerConfig;

/**
 *
 * @author iychoi
 */
public class RecipeManagerConfig extends ManagerConfig {
    
    private static final int DEFAULT_RECIPE_REPLICA_NUM = 2;
    
    private int recipeReplicaNum = DEFAULT_RECIPE_REPLICA_NUM;
    
    public static RecipeManagerConfig createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (RecipeManagerConfig) JsonSerializer.fromJsonFile(file, RecipeManagerConfig.class);
    }
    
    public static RecipeManagerConfig createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (RecipeManagerConfig) JsonSerializer.fromJson(json, RecipeManagerConfig.class);
    }
    
    public RecipeManagerConfig() {
    }
    
    @JsonProperty("recipe_replica_num")
    public void setRecipeReplicaNum(int replicaNum) {
        super.checkMutableAndRaiseException();
        
        if(replicaNum < 0) {
            this.recipeReplicaNum = DEFAULT_RECIPE_REPLICA_NUM;
        } else {
            this.recipeReplicaNum = replicaNum;
        }
    }
    
    @JsonProperty("recipe_replica_num")
    public int getRecipeReplicaNum() {
        return this.recipeReplicaNum;
    }
}
