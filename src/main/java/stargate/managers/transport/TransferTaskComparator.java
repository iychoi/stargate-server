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

import java.util.Comparator;

/**
 *
 * @author iychoi
 */
public class TransferTaskComparator implements Comparator<Runnable> {

    public int compare(AbstractTransferTask t1, AbstractTransferTask t2) {
        if(t1 == t2) {
            return 0;
        }
        
        if(t1 == null || t2 == null) {
            if(t1 == null) {
                return -1;
            }
            
            if(t2 == null) {
                return 1;
            }
        }
        
        return t1.getPriority().getIntVal() - t2.getPriority().getIntVal();
    }

    @Override
    public int compare(Runnable o1, Runnable o2) {
        if(o1 == o2) {
            return 0;
        }
        
        if(o1 == null || o2 == null) {
            if(o1 == null) {
                return -1;
            }
            
            if(o2 == null) {
                return 1;
            }
        }
        
        if(o1 instanceof AbstractTransferTask && o2 instanceof AbstractTransferTask) {
            return compare((AbstractTransferTask) o1, (AbstractTransferTask) o2);
        }
        
        return 0;
    }
}
