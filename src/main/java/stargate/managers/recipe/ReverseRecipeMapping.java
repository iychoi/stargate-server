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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import stargate.commons.utils.JsonSerializer;

/**
 *
 * @author iychoi
 */
public class ReverseRecipeMapping {
    
    private static final Log LOG = LogFactory.getLog(ReverseRecipeMapping.class);
    
    private String hash;
    private Set<String> recipeNames = new HashSet<String>();
    
    public static ReverseRecipeMapping createInstance(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }

        return (ReverseRecipeMapping) JsonSerializer.fromJsonFile(file, ReverseRecipeMapping.class);
    }
    
    public static ReverseRecipeMapping createInstance(String json) throws IOException {
        if(json == null || json.isEmpty()) {
            throw new IllegalArgumentException("json is null or empty");
        }
        
        return (ReverseRecipeMapping) JsonSerializer.fromJson(json, ReverseRecipeMapping.class);
    }
    
    ReverseRecipeMapping() {
    }
    
    public ReverseRecipeMapping(String hash) {
        if(hash == null || hash.isEmpty()) {
            throw new IllegalArgumentException("hash is null or empty");
        }
    }
    
    @JsonProperty("hash")
    public String getHash() {
        return this.hash;
    }
    
    @JsonProperty("hash")
    public void setHash(String hash) {
        this.hash = hash;
    }
    
    @JsonProperty("recipe_names")
    public Collection<String> getRecipeNames() {
        return Collections.unmodifiableCollection(this.recipeNames);
    }
    
    @JsonProperty("recipe_names")
    public void addRecipeNames(Collection<String> recipeNames) {
        this.recipeNames.addAll(recipeNames);
    }
    
    @JsonIgnore
    public void addRecipeName(String recipeName) {
        this.recipeNames.add(recipeName);
    }
    
    @JsonIgnore
    public boolean removeRecipeName(String recipeName) {
        return this.recipeNames.remove(recipeName);
    }
    
    @JsonIgnore
    public void clearRecipeNames() {
        this.recipeNames.clear();
    }
    
    @Override
    public String toString() {
        return this.hash;
    }
    
    @JsonIgnore
    public String toJson() throws IOException {
        return JsonSerializer.toJson(this);
    }
    
    @JsonIgnore
    public void saveTo(File file) throws IOException {
        if(file == null) {
            throw new IllegalArgumentException("file is null");
        }
        
        JsonSerializer.toJsonFile(file, this);
    }
}
