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
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.internal.commandline.CommandHandler;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
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
        CMD_LV1_CLUSTER_SUMMARY("summary"),
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
        CMD_LV2_LOCAL_CLUSTER_LEADER("leader"),
        CMD_LV2_LOCAL_CLUSTER_ACTIVATE("activate"),
        CMD_LV2_LOCAL_CLUSTER_ACTIVE("active"),
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
                    case CMD_LV1_CLUSTER_SUMMARY:
                        process_cluster_summary(parser.getServiceURI());
                        break;
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
                case CMD_LV2_LOCAL_CLUSTER_LEADER:
                    process_local_cluster_leader(parser.getServiceURI());
                    break;
                case CMD_LV2_LOCAL_CLUSTER_ACTIVATE:
                    process_local_cluster_activate(parser.getServiceURI());
                    break;
                case CMD_LV2_LOCAL_CLUSTER_ACTIVE:
                    process_local_cluster_active(parser.getServiceURI());
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
                        process_remote_clusters_show(parser.getServiceURI(), positionalArgs[2]);
                    }
                    break;
                case CMD_LV2_REMOTE_CLUSTER_LIST:
                    process_remote_clusters_list(parser.getServiceURI());
                    break;
                case CMD_LV2_REMOTE_CLUSTER_ADD:
                    if(positionalArgs.length >= 3) {
                        process_remote_clusters_add(parser.getServiceURI(), positionalArgs[2]);
                    }
                    break;
                case CMD_LV2_REMOTE_CLUSTER_REMOVE:
                    if(positionalArgs.length >= 3) {
                        process_remote_clusters_remove(parser.getServiceURI(), positionalArgs[2]);
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
    
    private static String get_node_string(Node node) {
        Collection<String> hostnames = node.getHostnames();
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for(String hostname : hostnames) {
            if(first) {
                sb.append(hostname);
                first = false;
            } else {
                sb.append(", ");
                sb.append(hostname);
            }
        }
        sb.append("]");
        return String.format("%s %s %s", node.getName(), node.getUserInterfaceServiceInfo().getServiceURI().toASCIIString(), sb.toString());
    }
    
    private static void process_cluster_summary(URI serviceURI) {
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            Cluster localCluster = client.getLocalCluster();
            Collection<Cluster> remoteClusters = client.getRemoteClusters();
            
            Collection<Node> localNodes = localCluster.getNodes();
            System.out.println(String.format("Local Cluster - %s (%d nodes)", localCluster.getName(), localCluster.getNodeNum()));
            for(Node node : localNodes) {
                System.out.println(get_node_string(node));
            }
            System.out.println("");
            
            System.out.println("Remote Clusters");
            for(Cluster remoteCluster : remoteClusters) {
                System.out.println(String.format("%s (%d nodes)", remoteCluster.getName(), remoteCluster.getNodeNum()));
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
    
    private static void process_local_cluster_show(URI serviceURI) {
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            Cluster cluster = client.getLocalCluster();

            String json = JsonSerializer.formatPretty(cluster.toJson());
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
    
    private static void process_local_cluster_leader(URI serviceURI) {
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            
            Node node = client.getLeaderNode();

            String json = JsonSerializer.formatPretty(node.toJson());
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
    
    private static void process_local_cluster_activate(URI serviceURI) {
            CommandHandler hnd = new CommandHandler();
            String[] command = {"--activate"};
            hnd.execute(Arrays.asList(command));
            
            /*
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            System.out.println("Activating the cluster");
            client.activateCluster();
            
            String dateTimeString = DateTimeUtils.getDateTimeString(client.getLastActiveTime());
            System.out.println(String.format("<Request processed %s>", dateTimeString));
            client.disconnect();
            */
            System.exit(0);
    }
    
    private static void process_local_cluster_active(URI serviceURI) {
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            boolean active = client.isClusterActive();
            
            System.out.println(String.format("the cluster is %s", active ? "active" : "inactive"));
            
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

            String json = JsonSerializer.formatPretty(cluster.toJson());
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
                for(Cluster cluster : remoteClusters) {
                    String json = JsonSerializer.formatPretty(cluster.toJson());
                    System.out.println(json);
                }
                System.out.println(String.format("TOTAL %d remote clusters", remoteClusters.size()));
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
    
    private static void process_remote_clusters_add(URI serviceURI, String argument) {
        try {
            Cluster remoteCluster = null;
            if(argument.startsWith("http://") || argument.startsWith("https://")) {
                try {
                    URI remoteClusterServiceURI = new URI(argument);
                    HTTPUserInterfaceClient remoteClient = HTTPUIClient.getClient(remoteClusterServiceURI);
                    remoteClient.connect();
                    remoteCluster = remoteClient.getLocalCluster();
                    remoteClient.disconnect();
                } catch (URISyntaxException ex) {
                    ex.printStackTrace();
                    System.exit(1);
                }
            } else {
                File f = (new File(argument)).getAbsoluteFile();
                remoteCluster = Cluster.createInstance(f);
            }
            
            if(remoteCluster == null) {
                System.out.println("Cannot retrieve remote cluster");
                System.exit(1);
            }
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
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
