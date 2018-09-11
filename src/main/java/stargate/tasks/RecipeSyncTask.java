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

import stargate.commons.schedule.TaskRunnable;

/**
 *
 * @author iychoi
 */
public class RecipeSyncTask extends TaskRunnable {
    
    @Override
    public void run() {
        // need to call server via user interface
        // because we cannot obtain stargate service instance
    }
}
