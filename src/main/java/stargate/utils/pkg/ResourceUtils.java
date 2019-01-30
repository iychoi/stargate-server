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
package stargate.utils.pkg;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.service.AbstractService;

/**
 *
 * @author iychoi
 */
public class ResourceUtils {
    private static final Log LOG = LogFactory.getLog(ResourceUtils.class);
    
    public static File getStargateRoot() {
        try {
            URI lib_uri = AbstractService.class.getProtectionDomain().getCodeSource().getLocation().toURI();
            File lib_file = new File(lib_uri).getParentFile();
            if(lib_file.getName().equals("dependency")) {
                // source
                return lib_file.getParentFile().getParentFile();
            } else if(lib_file.getName().equals("libs")) {
                // release
                return lib_file.getParentFile();
            } else {
                // 
                return null;
            }
        } catch (URISyntaxException ex) {
            LOG.error(ex);
            return null;
        }
    }
}
