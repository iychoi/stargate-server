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
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.datasource.DataExportEntry;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.utils.PathUtils;
import stargate.drivers.userinterface.http.HTTPUserInterfaceClient;

/**
 *
 * @author iychoi
 */
public class DataExports {
    private static final Log LOG = LogFactory.getLog(DataExports.class);

    private enum COMMAND_LV1 {
        CMD_LV1_SHOW("show"),
        CMD_LV1_LIST("list"),
        CMD_LV1_ADD("add"),
        CMD_LV1_REMOVE("remove"),
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
    
    public static void main(String[] args) {
        try {
            CommandParser parser = new CommandParser();
            parser.parse(args);
            
            String[] positionalArgs = parser.getPositionalArgs();
            if(positionalArgs.length >= 1) {
                String cmd_lv1 = positionalArgs[0];
                COMMAND_LV1 cmd = COMMAND_LV1.fromString(cmd_lv1);

                switch(cmd) {
                    case CMD_LV1_SHOW:
                        if(positionalArgs.length >= 2) {
                            process_data_exports_get(parser.getServiceURI(), positionalArgs[1]);
                        }
                        break;
                    case CMD_LV1_LIST:
                        process_data_exports_list(parser.getServiceURI());
                        break;
                    case CMD_LV1_ADD:
                        if(positionalArgs.length >= 3) {
                            process_data_exports_add(parser.getServiceURI(), positionalArgs[1], positionalArgs[2]);
                        }
                        break;
                    case CMD_LV1_REMOVE:
                        if(positionalArgs.length >= 2) {
                            process_data_exports_remove(parser.getServiceURI(), positionalArgs[1]);
                        }
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
        } catch(UnsupportedOperationException ex) {
            System.err.println(ex.getMessage());
        }
    }
    
    private static void process_data_exports_get(URI serviceURI, String stargatePath) {
        try {
            String concatPath = PathUtils.makeAbsolutePath(stargatePath);
            
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            DataObjectURI uri = new DataObjectURI("", concatPath);
            DataExportEntry dataExportEntry = client.getDataExportEntry(uri);
            if(dataExportEntry == null || dataExportEntry.isEmpty()) {
                System.out.println("<ENTRY DOES NOT EXIST!>");
            } else {
                String json = JsonSerializer.formatPretty(dataExportEntry.toJson());
                System.out.println(json);
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
    
    private static void process_data_exports_list(URI serviceURI) {
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            Collection<DataExportEntry> dataExportEntries = client.getDataExportEntries();
            if(dataExportEntries == null || dataExportEntries.isEmpty()) {
                System.out.println("<EMPTY!>");
            } else {
                for(DataExportEntry entry : dataExportEntries) {
                    String json = JsonSerializer.formatPretty(entry.toJson());
                    System.out.println(json);
                }
                System.out.println(String.format("TOTAL %d entries", dataExportEntries.size()));
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
    
    private static void process_data_exports_add(URI serviceURI, String sourceURI, String stargatePath) {
        try {
            String concatPath = PathUtils.makeAbsolutePath(stargatePath);
            DataExportEntry entry = new DataExportEntry(sourceURI, concatPath);
            
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            client.addDataExportEntry(entry);
            String dateTimeString = DateTimeUtils.getDateTimeString(client.getLastActiveTime());
            System.out.println(String.format("<Request processed %s>", dateTimeString));
            client.disconnect();
            System.exit(0);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        } catch (URISyntaxException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void process_data_exports_remove(URI serviceURI, String stargatePath) {
        try {
            String concatPath = PathUtils.makeAbsolutePath(stargatePath);
            
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            DataObjectURI uri = new DataObjectURI("", concatPath);
            client.removeDataExportEntry(uri);
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
