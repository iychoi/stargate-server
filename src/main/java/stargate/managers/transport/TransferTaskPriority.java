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

/**
 *
 * @author iychoi
 */
public enum TransferTaskPriority {
    PREFETCH_TASK_PRIORITY_HIGH ("high"),
    PREFETCH_TASK_PRIORITY_LOW ("low");
    
        
    private String strVal;
    
    TransferTaskPriority(String strVal) {
        this.strVal = strVal;
    }
    
    public String getStrVal() {
        return this.strVal;
    }
    
    public int getIntVal() {
        return this.ordinal(); // lower is high priority
    }
    
    public static TransferTaskPriority fromStrVal(String strVal) {
        for(TransferTaskPriority type : TransferTaskPriority.values()) {
            if(type.getStrVal().equalsIgnoreCase(strVal)) {
                return type;
            }
            
            if(type.name().equalsIgnoreCase(strVal)) {
                return type;
            }
        }
        return null;
    }
}
