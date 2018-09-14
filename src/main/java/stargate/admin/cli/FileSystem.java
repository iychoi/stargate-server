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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.utils.PathUtils;
import stargate.drivers.userinterface.http.HTTPUserInterfaceClient;

/**
 *
 * @author iychoi
 */
public class FileSystem {
    private static final Log LOG = LogFactory.getLog(FileSystem.class);
    
    private enum COMMAND_LV1 {
        CMD_LV1_LIST("ls"),
        CMD_LV1_RECIPE("recipe"),
        CMD_LV1_GET("get"),
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
            if(positionalArgs.length != 0) {
                String cmd_lv1 = positionalArgs[0];
                COMMAND_LV1 cmd = COMMAND_LV1.fromString(cmd_lv1);

                switch(cmd) {
                    case CMD_LV1_LIST:
                        if(positionalArgs.length >= 2) {
                            process_fs_list(parser.getServiceURI(), positionalArgs[1]);
                        }
                        break;
                    case CMD_LV1_RECIPE:
                        if(positionalArgs.length >= 2) {
                            process_fs_recipe(parser.getServiceURI(), positionalArgs[1]);
                        }
                        break;
                    case CMD_LV1_GET:
                        if(positionalArgs.length >= 2) {
                            String targetPath = ".";
                            if(positionalArgs.length >= 3) {
                                targetPath = positionalArgs[2];
                            }
                            process_fs_get(parser.getServiceURI(), positionalArgs[1], targetPath);
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
    
    private static String formatDataObjectMetadata(DataObjectMetadata metadata) {
        DataObjectURI uri = metadata.getURI();
        long lastModifiedTime = metadata.getLastModifiedTime();
        long size = metadata.getSize();
        String path = uri.toString();
        return String.format("%s\t%d\t%s", path, size, DateTimeUtils.getDateTimeString(lastModifiedTime));
    }
    
    private static void process_fs_list(URI serviceURI, String stargatePath) {
        DataObjectURI uri = new DataObjectURI(stargatePath);
        
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            try {
                DataObjectMetadata metadata = client.getDataObjectMetadata(uri);
                if(metadata == null) {
                    System.out.println(String.format("<%s not exist!>", uri.toString()));
                } else if(!metadata.isDirectory()) {
                    System.out.println(formatDataObjectMetadata(metadata));
                } else {
                    Collection<DataObjectMetadata> listDataObjectMetadatas = client.listDataObjectMetadata(uri);
                    if(listDataObjectMetadatas == null || listDataObjectMetadatas.isEmpty()) {
                        System.out.println("<EMPTY!>");
                    } else {
                        for(DataObjectMetadata m : listDataObjectMetadatas) {
                            System.out.println(formatDataObjectMetadata(m));
                        }
                    }
                }
            } catch (FileNotFoundException ex) {
                System.out.println(String.format("<%s not exist!>", uri.toString()));
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

    private static void process_fs_recipe(URI serviceURI, String stargatePath) {
        DataObjectURI uri = new DataObjectURI(stargatePath);
        
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            try {
                DataObjectMetadata metadata = client.getDataObjectMetadata(uri);
                if(metadata == null) {
                    System.out.println(String.format("<%s not exist!>", uri.toString()));
                } else if(metadata.isDirectory()) {
                    System.out.println(String.format("<%s is a directory!>", uri.toString()));
                } else {
                    Recipe recipe = client.getRecipe(uri);
                    if(recipe == null) {
                        System.out.println("<ENTRY DOES NOT EXIST!>");
                    } else {
                        String json = JsonSerializer.formatPretty(recipe.toJson());
                        System.out.println(json);
                    }
                }
            } catch (FileNotFoundException ex) {
                System.out.println(String.format("<%s not exist!>", uri.toString()));
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

    private static void process_fs_get(URI serviceURI, String stargatePath, String targetPath) {
        DataObjectURI uri = new DataObjectURI(stargatePath);
        
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            try {
                DataObjectMetadata metadata = client.getDataObjectMetadata(uri);
                if(metadata == null) {
                    System.out.println(String.format("<%s not exist!>", uri.toString()));
                } else if(metadata.isDirectory()) {
                    System.out.println(String.format("<%s is a directory!>", uri.toString()));
                } else {
                    LOG.debug("Downloading a recipe");
                    Recipe recipe = client.getRecipe(uri);
                    if(recipe == null) {
                        System.out.println("<Recipe does not exist!>");
                    } else {
                        File f = (new File(targetPath)).getAbsoluteFile();
                        if(f.isDirectory()) {
                            f = new File(f, PathUtils.getFileName(stargatePath));
                        }

                        FileOutputStream fos = new FileOutputStream(f);
                        int bufferlen = 1024*4;
                        byte[] buffer = new byte[bufferlen];
                        Collection<RecipeChunk> chunks = recipe.getChunks();
                        for(RecipeChunk chunk : chunks) {
                            String hash = chunk.getHashString();
                            LOG.debug(String.format("Downloading a chunk for a hash %s", hash));
                            InputStream is = client.getDataChunk(uri.getClusterName(), hash);
                            int readLen = 0;
                            while((readLen = is.read(buffer, 0, bufferlen)) > 0) {
                                fos.write(buffer, 0, readLen);
                            }
                            is.close();
                        }
                        fos.close();
                    }
                }
            } catch (FileNotFoundException ex) {
                System.out.println(String.format("<%s not exist!>", uri.toString()));
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
}
