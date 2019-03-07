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
package stargate.managers.transport.layout;

/**
 *
 * @author iychoi
 */
public enum ContactNodeSelectionAlgorithms {
    CONTACT_NODE_SELECTION_ALGORITHM_ROUNDROBIN ("roundrobin"),
    CONTACT_NODE_SELECTION_ALGORITHM_RANDOM ("random");
    
    private String strVal;
    
    ContactNodeSelectionAlgorithms(String strVal) {
        this.strVal = strVal;
    }
    
    public String getStringVal() {
        return this.strVal;
    }
    
    public static ContactNodeSelectionAlgorithms fromStringVal(String strVal) {
        for(ContactNodeSelectionAlgorithms type : ContactNodeSelectionAlgorithms.values()) {
            if(type.getStringVal().equalsIgnoreCase(strVal)) {
                return type;
            }
        }
        return null;
    }
}
