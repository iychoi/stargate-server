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
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.transport.TransferAssignment;

/**
 *
 * @author iychoi
 */
public class PendingPrefetchScheduleComparator implements Comparator<PendingPrefetchSchedule> {
    @Override
    public int compare(PendingPrefetchSchedule o1, PendingPrefetchSchedule o2) {
        if(o1 == null && o2 == null) {
            return 0;
        }
        
        if(o1 == null) {
            return -1;
        }
            
        if(o2 == null) {
            return 1;
        }
        
        TransferAssignment o1assignment = o1.getTransferAssignment();
        TransferAssignment o2assignment = o2.getTransferAssignment();
        
        DataObjectURI o1uri = o1assignment.getDataObjectURI();
        DataObjectURI o2uri = o2assignment.getDataObjectURI();
        
        int comp = o1uri.compareTo(o2uri);
        if(comp == 0) {
            long o1offset = o1assignment.getOffset();
            long o2offset = o2assignment.getOffset();
            return Long.compare(o1offset, o2offset);
        } else {
            return comp;
        }
    }
}
