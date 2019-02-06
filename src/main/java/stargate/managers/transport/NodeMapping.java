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
package stargate.managers.transport;

import java.util.Objects;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class NodeMapping {

    private String nodeName1;
    private String nodeName2;
    
    public NodeMapping(String nodeName1, String nodeName2) {
        if(nodeName1 == null || nodeName1.isEmpty()) {
            throw new IllegalArgumentException("nodeName1 is null or empty");
        }
        
        if(nodeName2 == null || nodeName2.isEmpty()) {
            throw new IllegalArgumentException("nodeName2 is null or empty");
        }
        
        this.nodeName1 = nodeName1;
        this.nodeName2 = nodeName2;
    }
    
    @JsonProperty("node_name_1")
    public String getNodeName1() {
        return this.nodeName1;
    }
    
    @JsonProperty("node_name_1")
    public void setNodeName1(String nodeName1) {
        this.nodeName1 = nodeName1;
    }
    
    @JsonProperty("node_name_2")
    public String getNodeName2() {
        return this.nodeName2;
    }
    
    @JsonProperty("node_name_2")
    public void setNodeName2(String nodeName2) {
        this.nodeName2 = nodeName2;
    }
    
    @Override
    public int hashCode() {
        int hash = 5;
        hash = 17 * hash + Objects.hashCode(this.nodeName1);
        hash = 17 * hash + Objects.hashCode(this.nodeName2);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final NodeMapping other = (NodeMapping) obj;
        if (!Objects.equals(this.nodeName1, other.nodeName1)) {
            return false;
        }
        if (!Objects.equals(this.nodeName2, other.nodeName2)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
        return "NodeMapping{" + "node1=" + nodeName1 + ", node2=" + nodeName2 + '}';
    }
}