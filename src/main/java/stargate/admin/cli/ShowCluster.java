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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.utils.JsonSerializer;
import stargate.drivers.userinterface.http.HTTPUserInterfaceClient;
import stargate.drivers.userinterface.http.HTTPUserInterfaceDriverConfig;

/**
 *
 * @author iychoi
 */
public class ShowCluster {
    private static final Log LOG = LogFactory.getLog(ShowCluster.class);
    
    public static void main(String[] args) {
        try {
            URI serviceURI = null;
            if(args.length != 0) {
                String serviceURIString = args[0];
                serviceURI = new URI(serviceURIString);
            }
            
            if(serviceURI == null) {
                serviceURI = new URI(String.format("http://localhost:%d", HTTPUserInterfaceDriverConfig.DEFAULT_SERVICE_PORT));
            }
        
            HTTPUserInterfaceClient client = new HTTPUserInterfaceClient(serviceURI, null, null);
            client.connect();
            Cluster cluster = client.getCluster();
            
            JsonSerializer serializer = new JsonSerializer();
            String json = serializer.formatPretty(cluster.toJson());
            System.out.println(json);
            
            client.disconnect();
            System.exit(0);
        } catch (URISyntaxException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
