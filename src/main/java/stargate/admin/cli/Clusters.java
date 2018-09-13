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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.utils.JsonSerializer;
import stargate.drivers.userinterface.http.HTTPUserInterfaceClient;

/**
 *
 * @author iychoi
 */
public class Clusters {
    private static final Log LOG = LogFactory.getLog(Clusters.class);

    private enum COMMAND_LV1 {
        CMD_LV1_LOCAL_CLUSTER("local"),
        CMD_LV1_REMOTE_CLUSTERS("remote"),
        CMD_LV1_UNKNOWN("unknown");
        
        private String value;
        
        COMMAND_LV1(String value) {
            this.value = value;
        }
        
        public static COMMAND_LV1 fromString(String value) {
            for(COMMAND_LV1 v : COMMAND_LV1.values()) {
                if(value.equalsIgnoreCase(v.value)) {
                    return v;
                }
            }
            return CMD_LV1_UNKNOWN;
        }
        
        public String getValue() {
            return this.value;
        }
    }
    
    private enum COMMAND_LV2_LOCAL_CLUSTER {
        CMD_LV2_LOCAL_CLUSTER_SHOW("show"),
        CMD_LV2_LOCAL_CLUSTER_UNKNOWN("unknown");
        
        private String value;
        
        COMMAND_LV2_LOCAL_CLUSTER(String value) {
            this.value = value;
        }
        
        public static COMMAND_LV2_LOCAL_CLUSTER fromString(String value) {
            for(COMMAND_LV2_LOCAL_CLUSTER v : COMMAND_LV2_LOCAL_CLUSTER.values()) {
                if(value.equalsIgnoreCase(v.value)) {
                    return v;
                }
            }
            return CMD_LV2_LOCAL_CLUSTER_UNKNOWN;
        }
        
        public String getValue() {
            return this.value;
        }
    }
    
    private enum COMMAND_LV2_REMOTE_CLUSTER {
        CMD_LV2_REMOTE_CLUSTER_SHOW("show"),
        CMD_LV2_REMOTE_CLUSTER_LIST("list"),
        CMD_LV2_REMOTE_CLUSTER_ADD("add"),
        CMD_LV2_REMOTE_CLUSTER_REMOVE("remove"),
        CMD_LV2_REMOTE_CLUSTER_SYNC("sync"),
        CMD_LV2_REMOTE_CLUSTER_UNKNOWN("unknown");
        
        private String value;
        
        COMMAND_LV2_REMOTE_CLUSTER(String value) {
            this.value = value;
        }
        
        public static COMMAND_LV2_REMOTE_CLUSTER fromString(String value) {
            for(COMMAND_LV2_REMOTE_CLUSTER v : COMMAND_LV2_REMOTE_CLUSTER.values()) {
                if(value.equalsIgnoreCase(v.value)) {
                    return v;
                }
            }
            return CMD_LV2_REMOTE_CLUSTER_UNKNOWN;
        }
        
        public String getValue() {
            return this.value;
        }
    }
    
    public static void main(String[] args) {
        try {
            CommandParser parser = new CommandParser();
            parser.parse(args);
            
            String[] positionalArgs = parser.getPositionalArgs();
            if(positionalArgs.length >= 1) {
                String cmd_lv1 = positionalArgs[0];
                COMMAND_LV1 cmd = COMMAND_LV1.fromString(cmd_lv1);

                switch(cmd) {
                    case CMD_LV1_LOCAL_CLUSTER:
                        local_cluster(parser);
                        break;
                    case CMD_LV1_REMOTE_CLUSTERS:
                        remote_clusters(parser);
                        break;
                    case CMD_LV1_UNKNOWN:
                        throw new UnsupportedOperationException(String.format("Unknown command - %s", cmd_lv1));
                    default:
                        throw new UnsupportedOperationException(String.format("Unknown command - %s", cmd_lv1));

                }
            } else {
                StringBuilder sb = new StringBuilder();
                for(COMMAND_LV1 cmd : COMMAND_LV1.values()) {
                    if(cmd != COMMAND_LV1.CMD_LV1_UNKNOWN) {
                        if(sb.length() != 0) {
                            sb.append(" ");
                        }
                        sb.append(cmd.getValue());
                    }
                }
                System.out.println(String.format("Available commands - %s", sb.toString()));
            }
        } catch(Exception ex) {
            System.err.println(ex.getMessage());
        }
    }
    
    private static void local_cluster(CommandParser parser) {
        String[] positionalArgs = parser.getPositionalArgs();
        if(positionalArgs.length >= 2) {
            String cmd_lv2 = positionalArgs[1];
            COMMAND_LV2_LOCAL_CLUSTER cmd = COMMAND_LV2_LOCAL_CLUSTER.fromString(cmd_lv2);
            
            switch(cmd) {
                case CMD_LV2_LOCAL_CLUSTER_SHOW:
                    process_local_cluster_show(parser.getServiceURI());
                    break;
                case CMD_LV2_LOCAL_CLUSTER_UNKNOWN:
                    throw new UnsupportedOperationException(String.format("Unknown command - %s", cmd_lv2));
                default:
                    throw new UnsupportedOperationException(String.format("Unknown command - %s", cmd_lv2));
            }
        } else {
            StringBuilder sb = new StringBuilder();
            for(COMMAND_LV2_LOCAL_CLUSTER cmd : COMMAND_LV2_LOCAL_CLUSTER.values()) {
                if(cmd != COMMAND_LV2_LOCAL_CLUSTER.CMD_LV2_LOCAL_CLUSTER_UNKNOWN) {
                    if(sb.length() != 0) {
                        sb.append(" ");
                    }
                    sb.append(cmd.getValue());
                }
            }
            System.out.println(String.format("Available commands - %s", sb.toString()));
        }
    }
    
    private static void remote_clusters(CommandParser parser) {
        String[] positionalArgs = parser.getPositionalArgs();
        if(positionalArgs.length >= 2) {
            String cmd_lv2 = positionalArgs[1];
            COMMAND_LV2_REMOTE_CLUSTER cmd = COMMAND_LV2_REMOTE_CLUSTER.fromString(cmd_lv2);
            
            switch(cmd) {
                case CMD_LV2_REMOTE_CLUSTER_SHOW:
                    if(positionalArgs.length >= 3) {
                        process_remote_clusters_show(parser.getServiceURI(), positionalArgs[1]);
                    }
                    break;
                case CMD_LV2_REMOTE_CLUSTER_LIST:
                    process_remote_clusters_list(parser.getServiceURI());
                    break;
                case CMD_LV2_REMOTE_CLUSTER_ADD:
                    if(positionalArgs.length >= 3) {
                        process_remote_clusters_add(parser.getServiceURI(), positionalArgs[1]);
                    }
                    break;
                case CMD_LV2_REMOTE_CLUSTER_REMOVE:
                    if(positionalArgs.length >= 3) {
                        process_remote_clusters_remove(parser.getServiceURI(), positionalArgs[1]);
                    }
                    break;
                case CMD_LV2_REMOTE_CLUSTER_SYNC:
                    throw new UnsupportedOperationException("Not supported yet.");
                case CMD_LV2_REMOTE_CLUSTER_UNKNOWN:
                    throw new UnsupportedOperationException(String.format("Unknown command - %s", cmd_lv2));
                default:
                    throw new UnsupportedOperationException(String.format("Unknown command - %s", cmd_lv2));
            }
        } else {
            StringBuilder sb = new StringBuilder();
            for(COMMAND_LV2_REMOTE_CLUSTER cmd : COMMAND_LV2_REMOTE_CLUSTER.values()) {
                if(cmd != COMMAND_LV2_REMOTE_CLUSTER.CMD_LV2_REMOTE_CLUSTER_UNKNOWN) {
                    if(sb.length() != 0) {
                        sb.append(" ");
                    }
                    sb.append(cmd.getValue());
                }
            }
            System.out.println(String.format("Available commands - %s", sb.toString()));
        }
    }
    
    private static void process_local_cluster_show(URI serviceURI) {
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            Cluster cluster = client.getCluster();

            JsonSerializer serializer = new JsonSerializer();
            String json = serializer.formatPretty(cluster.toJson());
            System.out.println(json);
            String dateTimeString = DateTimeUtils.getDateTimeString(client.getLastActiveTime());
            System.out.println(String.format("<Request processed %s>", dateTimeString));
            client.disconnect();
            System.exit(0);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void process_remote_clusters_show(URI serviceURI, String name) {
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            Cluster cluster = client.getRemoteCluster(name);

            JsonSerializer serializer = new JsonSerializer();
            String json = serializer.formatPretty(cluster.toJson());
            System.out.println(json);
            String dateTimeString = DateTimeUtils.getDateTimeString(client.getLastActiveTime());
            System.out.println(String.format("<Request processed %s>", dateTimeString));
            client.disconnect();
            System.exit(0);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void process_remote_clusters_list(URI serviceURI) {
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            Collection<Cluster> remoteClusters = client.getRemoteClusters();
            if(remoteClusters == null || remoteClusters.isEmpty()) {
                System.out.println("<EMPTY!>");
            } else {
                JsonSerializer serializer = new JsonSerializer();
                for(Cluster cluster : remoteClusters) {
                    String json = serializer.formatPretty(cluster.toJson());
                    System.out.println(json);
                }
            }
            String dateTimeString = DateTimeUtils.getDateTimeString(client.getLastActiveTime());
            System.out.println(String.format("<Request processed %s>", dateTimeString));
            client.disconnect();
            System.exit(0);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void process_remote_clusters_add(URI serviceURI, String jsonPath) {
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            Cluster remoteCluster = Cluster.createInstance(new File(jsonPath));
            client.addRemoteCluster(remoteCluster);
            String dateTimeString = DateTimeUtils.getDateTimeString(client.getLastActiveTime());
            System.out.println(String.format("<Request processed %s>", dateTimeString));
            client.disconnect();
            System.exit(0);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    private static void process_remote_clusters_remove(URI serviceURI, String name) {
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            client.removeRemoteCluster(name);
            String dateTimeString = DateTimeUtils.getDateTimeString(client.getLastActiveTime());
            System.out.println(String.format("<Request processed %s>", dateTimeString));
            client.disconnect();
            System.exit(0);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
}
