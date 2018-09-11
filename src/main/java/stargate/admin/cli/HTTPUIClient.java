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
package stargate.admin.cli;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import stargate.drivers.userinterface.http.HTTPUserInterfaceClient;
import stargate.drivers.userinterface.http.HTTPUserInterfaceDriverConfig;

/**
 *
 * @author iychoi
 */
public class HTTPUIClient {
    public static HTTPUserInterfaceClient getClient(String uri) throws IOException {
        URI serviceURI = null;
        if(uri != null && !uri.isEmpty()) {
            try {
                serviceURI = new URI(uri);
            } catch (URISyntaxException ex) {
                throw new IOException(ex);
            }
        }
        
        return getClient(serviceURI);
    }
    
    public static HTTPUserInterfaceClient getClient(URI uri) throws IOException {
        try {
            URI serviceURI = uri;
            if(serviceURI == null) {
                serviceURI = new URI(String.format("http://localhost:%d", HTTPUserInterfaceDriverConfig.DEFAULT_SERVICE_PORT));
            }
        
            HTTPUserInterfaceClient client = new HTTPUserInterfaceClient(serviceURI, null, null);
            return client;
        } catch (URISyntaxException ex) {
            throw new IOException(ex);
        }
    }
}
