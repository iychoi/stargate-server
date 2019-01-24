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

import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.internal.commandline.CommandHandler;

/**
 *
 * @author iychoi
 */
public class Activation {
    private static final Log LOG = LogFactory.getLog(Activation.class);

    private enum COMMAND_LV1 {
        CMD_LV1_ACTIVATE_CLUSTER("activate"),
        CMD_LV1_DEACTIVATE_CLUSTERS("deactivate"),
        CMD_LV1_STATE_CLUSTERS("state"),
        CMD_LV1_BASELINE_CLUSTERS("baseline"),
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
                    case CMD_LV1_ACTIVATE_CLUSTER:
                        activate_cluster(parser);
                        break;
                    case CMD_LV1_DEACTIVATE_CLUSTERS:
                        deactivate_clusters(parser);
                        break;
                    case CMD_LV1_STATE_CLUSTERS:
                        state_clusters(parser);
                        break;
                    case CMD_LV1_BASELINE_CLUSTERS:
                        baseline_clusters(parser);
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
    
    private static void activate_cluster(CommandParser parser) {
        //String[] positionalArgs = parser.getPositionalArgs();
        CommandHandler hnd = new CommandHandler();
        String[] command = {"--activate"};
        hnd.execute(Arrays.asList(command));
    }
    
    private static void deactivate_clusters(CommandParser parser) {
        //String[] positionalArgs = parser.getPositionalArgs();
        CommandHandler hnd = new CommandHandler();
        String[] command = {"--deactivate"};
        hnd.execute(Arrays.asList(command));
    }
    
    private static void state_clusters(CommandParser parser) {
        //String[] positionalArgs = parser.getPositionalArgs();
        CommandHandler hnd = new CommandHandler();
        String[] command = {"--state"};
        hnd.execute(Arrays.asList(command));
    }
    
    private static void baseline_clusters(CommandParser parser) {
        //String[] positionalArgs = parser.getPositionalArgs();
        CommandHandler hnd = new CommandHandler();
        String[] command = {"--baseline"};
        hnd.execute(Arrays.asList(command));
    }
}
