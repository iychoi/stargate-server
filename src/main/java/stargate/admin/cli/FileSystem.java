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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stargate.commons.cluster.Cluster;
import stargate.commons.cluster.Node;
import stargate.commons.dataobject.DataObjectMetadata;
import stargate.commons.dataobject.DataObjectURI;
import stargate.commons.recipe.Recipe;
import stargate.commons.recipe.RecipeChunk;
import stargate.commons.service.FSServiceInfo;
import stargate.commons.userinterface.DataChunkStatus;
import stargate.commons.userinterface.UserInterfaceInitialDataPack;
import stargate.commons.userinterface.UserInterfaceServiceInfo;
import stargate.commons.utils.DateTimeUtils;
import stargate.commons.utils.JsonSerializer;
import stargate.commons.utils.PathUtils;
import stargate.drivers.userinterface.http.HTTPChunkInputStream;
import stargate.drivers.userinterface.http.HTTPUserInterfaceClient;

/**
 *
 * @author iychoi
 */
public class FileSystem {
    private static final Log LOG = LogFactory.getLog(FileSystem.class);

    private static class ChunkDownloadTask {

        private Recipe recipe;
        private RecipeChunk recipeChunk;
        private String nodeName;
        
        public ChunkDownloadTask(Recipe recipe, RecipeChunk recipeChunk, String nodeName) {
            this.recipe = recipe;
            this.recipeChunk = recipeChunk;
            this.nodeName = nodeName;
        }

        private Recipe getRecipe() {
            return this.recipe;
        }
        
        private RecipeChunk getRecipeChunk() {
            return this.recipeChunk;
        }
        
        private String getNodeName() {
            return this.nodeName;
        }
    }
    
    private enum COMMAND_LV1 {
        CMD_LV1_SHOW_INFO("show_info"),
        CMD_LV1_LIST("ls"),
        CMD_LV1_RECIPE("recipe"),
        CMD_LV1_RECIPE_LOCALIZED("recipe_localized"),
        CMD_LV1_RECIPE_LOCAL("recipe_local"),
        CMD_LV1_GET("get"),
        CMD_LV1_READ("read"),
        CMD_LV1_GET_CHUNKS_PARALLEL("get_chunks_parallel"),
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
                    case CMD_LV1_SHOW_INFO:
                        process_fs_show_info(parser.getServiceURI());
                        break;
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
                    case CMD_LV1_RECIPE_LOCAL:
                    case CMD_LV1_RECIPE_LOCALIZED:
                        if(positionalArgs.length >= 2) {
                            process_fs_recipe_localized(parser.getServiceURI(), positionalArgs[1]);
                        }
                        break;
                    case CMD_LV1_GET:
                        if(positionalArgs.length >= 2) {
                            List<String> sourcePaths = new ArrayList<String>();
                            String targetPath = ".";
                            if(positionalArgs.length >= 3) {
                                targetPath = positionalArgs[positionalArgs.length - 1];
                                for(int i=1;i<positionalArgs.length - 1;i++) {
                                    sourcePaths.add(positionalArgs[i]);
                                }
                            } else {
                                sourcePaths.add(positionalArgs[1]);
                            }
                            process_fs_get(parser.getServiceURI(), sourcePaths, targetPath);
                        }
                        break;
                    case CMD_LV1_READ:
                        if(positionalArgs.length >= 2) {
                            String sourcePath = positionalArgs[1];
                            long offset = 0;
                            long length = Long.MAX_VALUE;
                            if(positionalArgs.length >= 3) {
                                offset = Long.parseLong(positionalArgs[2]);
                                
                                if(positionalArgs.length >= 4) {
                                    length = Long.parseLong(positionalArgs[3]);
                                }
                            }
                            process_fs_read(parser.getServiceURI(), sourcePath, offset, length);
                        }
                        break;
                    case CMD_LV1_GET_CHUNKS_PARALLEL:
                        if(positionalArgs.length >= 2) {
                            List<String> sourcePaths = new ArrayList<String>();
                            String targetPath = ".";
                            if(positionalArgs.length >= 3) {
                                targetPath = positionalArgs[positionalArgs.length - 1];
                                for(int i=1;i<positionalArgs.length - 1;i++) {
                                    sourcePaths.add(positionalArgs[i]);
                                }
                            } else {
                                sourcePaths.add(positionalArgs[1]);
                            }
                            process_fs_get_chunks_parallel(parser.getServiceURI(), sourcePaths, targetPath);
                        }
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
    
    private static void process_fs_show_info(URI serviceURI) {
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            UserInterfaceInitialDataPack initialDataPack = client.getInitialDataPack();
            
            System.out.println("= FSServiceInfo =");
            FSServiceInfo info = initialDataPack.getFSServiceInfo();
            if(info == null) {
                System.out.println("<EMPTY!>");
            } else {
                String json = JsonSerializer.formatPretty(info.toJson());
                System.out.println(json);
            }
            
            System.out.println("= LocalCluster =");
            Cluster localCluster = initialDataPack.getLocalCluster();
            if(localCluster == null) {
                System.out.println("<EMPTY!>");
            } else {
                String json = JsonSerializer.formatPretty(localCluster.toJson());
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
                        System.out.println(String.format("TOTAL %d entries", listDataObjectMetadatas.size()));
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
    
    private static void process_fs_recipe_localized(URI serviceURI, String stargatePath) {
        DataObjectURI uri = new DataObjectURI(stargatePath);
        
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            try {
                Cluster localCluster = client.getLocalCluster();
                
                DataObjectMetadata metadata = client.getDataObjectMetadata(uri);
                if(metadata == null) {
                    System.out.println(String.format("<%s not exist!>", uri.toString()));
                } else if(metadata.isDirectory()) {
                    System.out.println(String.format("<%s is a directory!>", uri.toString()));
                } else {
                    Recipe recipe = null;
                    if(uri.getClusterName().equals(localCluster.getName())) {
                        // local
                        recipe = client.getRecipe(uri);
                    } else {
                        // remote
                        recipe = client.getRemoteRecipeWithTransferSchedule(uri);
                    }
                    
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
    
    private static void process_fs_get(URI serviceURI, Collection<String> stargatePaths, String targetPath) {
        try {
            File fileDir = (new File(targetPath)).getAbsoluteFile();
            if(!fileDir.exists()) {
                fileDir.mkdirs();
            }
            
            if(fileDir.isDirectory()) {
                HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
                client.connect();
                Cluster cluster = client.getLocalCluster();

                Map<String, HTTPUserInterfaceClient> clients = new HashMap<String, HTTPUserInterfaceClient>();
                List<Recipe> recipes = new ArrayList<Recipe>();

                for(String stargatePath : stargatePaths) {
                    DataObjectURI uri = new DataObjectURI(stargatePath);

                    try {
                        DataObjectMetadata metadata = client.getDataObjectMetadata(uri);
                        if(metadata == null) {
                            System.out.println(String.format("<%s not exist!>", uri.toString()));
                        } else if(metadata.isDirectory()) {
                            System.out.println(String.format("<%s is a directory!>", uri.toString()));
                        } else {
                            LOG.debug(String.format("Downloading a recipe for a file %s", uri.toString()));
                            Recipe recipe = client.getRecipe(uri);
                            if(recipe == null) {
                                System.out.println("<Recipe does not exist!>");
                            } else {
                                recipes.add(recipe);

                                // create connections
                                Collection<String> nodeNames = recipe.getNodeNames();
                                for(String nodeName : nodeNames) {
                                    if(!clients.containsKey(nodeName)) {
                                        Node node = cluster.getNode(nodeName);
                                        UserInterfaceServiceInfo userInterfaceServiceInfo = node.getUserInterfaceServiceInfo();
                                        URI nodeServiceURI = userInterfaceServiceInfo.getServiceURI();
                                        if(!client.getServiceURI().equals(nodeServiceURI)) {
                                            HTTPUserInterfaceClient newClient = HTTPUIClient.getClient(nodeServiceURI);
                                            clients.put(nodeName, newClient);
                                        } else {
                                            clients.put(nodeName, client);
                                        }
                                    }
                                }
                            }
                        }
                    } catch (FileNotFoundException ex) {
                        System.out.println(String.format("<%s not exist!>", uri.toString()));
                    }
                }
            
                // download
                for(Recipe recipe : recipes) {
                    DataObjectURI uri = recipe.getMetadata().getURI();

                    LOG.debug(String.format("Downloading a file %s", uri.toString()));

                    File f = new File(fileDir, PathUtils.getFileName(uri.toString()));

                    long startTimeC = DateTimeUtils.getTimestamp();

                    FileOutputStream fos = new FileOutputStream(f);
                    fos.getChannel().truncate(0);
                    BufferedOutputStream bos = new BufferedOutputStream(fos);

                    int bufferlen = 1024*1024;
                    byte[] buffer = new byte[bufferlen];

                    HTTPChunkInputStream cis = new HTTPChunkInputStream(clients, recipe);
                    int readLen = 0;
                    while((readLen = cis.read(buffer, 0, bufferlen)) >= 0) {
                        fos.write(buffer, 0, readLen);
                    }
                    cis.close();
                    long endTimeC = DateTimeUtils.getTimestamp();
                    LOG.debug(String.format("Downloading took - %d ms", endTimeC - startTimeC));

                    bos.close();
                }
                
                // close
                for(HTTPUserInterfaceClient newClient : clients.values()) {
                    if(newClient != client) {
                        newClient.disconnect();
                    }
                }
                clients.clear();

                String dateTimeString = DateTimeUtils.getDateTimeString(client.getLastActiveTime());
                System.out.println(String.format("<Request processed %s>", dateTimeString));
                client.disconnect();
            } else {
                System.out.println(String.format("<Failed to create an output dir %s!>", fileDir.getPath()));
            }
            
            System.exit(0);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void process_fs_read(URI serviceURI, String stargatePath, long offset, long length) {
        try {
            HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
            client.connect();
            Cluster cluster = client.getLocalCluster();

            Map<String, HTTPUserInterfaceClient> clients = new HashMap<String, HTTPUserInterfaceClient>();
            DataObjectURI uri = new DataObjectURI(stargatePath);

            try {
                DataObjectMetadata metadata = client.getDataObjectMetadata(uri);
                if(metadata == null) {
                    System.out.println(String.format("<%s not exist!>", uri.toString()));
                } else if(metadata.isDirectory()) {
                    System.out.println(String.format("<%s is a directory!>", uri.toString()));
                } else {
                    LOG.debug(String.format("Downloading a recipe for a file %s", uri.toString()));
                    Recipe recipe = client.getRecipe(uri);
                    if(recipe == null) {
                        System.out.println("<Recipe does not exist!>");
                    } else {
                        // create connections
                        Collection<String> nodeNames = recipe.getNodeNames();
                        for(String nodeName : nodeNames) {
                            if(!clients.containsKey(nodeName)) {
                                Node node = cluster.getNode(nodeName);
                                UserInterfaceServiceInfo userInterfaceServiceInfo = node.getUserInterfaceServiceInfo();
                                URI nodeServiceURI = userInterfaceServiceInfo.getServiceURI();
                                if(!client.getServiceURI().equals(nodeServiceURI)) {
                                    HTTPUserInterfaceClient newClient = HTTPUIClient.getClient(nodeServiceURI);
                                    clients.put(nodeName, newClient);
                                } else {
                                    clients.put(nodeName, client);
                                }
                            }
                        }
                        
                        // download
                        LOG.debug(String.format("Reading a file %s, offset %d, length %d", uri.toString(), offset, Math.min(length, metadata.getSize() - offset)));

                        long startTimeC = DateTimeUtils.getTimestamp();

                        int bufferlen = 64*1024;
                        byte[] buffer = new byte[bufferlen];

                        HTTPChunkInputStream cis = new HTTPChunkInputStream(clients, recipe);
                        cis.seek(offset);
                        
                        int readLen = 0;
                        long remain = Math.min(length, metadata.getSize() - offset);
                        while(remain > 0) {
                            readLen = cis.read(buffer, 0, bufferlen);
                            if(readLen < 0) {
                                //EOF
                                break;
                            }
                            
                            System.out.write(buffer, 0, readLen);
                            remain -= readLen;
                        }
                        cis.close();
                        long endTimeC = DateTimeUtils.getTimestamp();
                        LOG.debug(String.format("Reading took - %d ms", endTimeC - startTimeC));
                    }
                }
            } catch (FileNotFoundException ex) {
                System.out.println(String.format("<%s not exist!>", uri.toString()));
            }

            
            

            // close
            for(HTTPUserInterfaceClient newClient : clients.values()) {
                if(newClient != client) {
                    newClient.disconnect();
                }
            }
            clients.clear();

            String dateTimeString = DateTimeUtils.getDateTimeString(client.getLastActiveTime());
            System.out.println(String.format("<Request processed %s>", dateTimeString));
            client.disconnect();
            
            System.exit(0);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void process_fs_get_chunks_parallel(URI serviceURI, Collection<String> stargatePaths, String targetPath) {
        try {
            File fileDir = (new File(targetPath)).getAbsoluteFile();
            if(!fileDir.exists()) {
                fileDir.mkdirs();
            }
            
            if(fileDir.isDirectory()) {
                HTTPUserInterfaceClient client = HTTPUIClient.getClient(serviceURI);
                client.connect();
                Cluster cluster = client.getLocalCluster();

                Map<String, HTTPUserInterfaceClient> clients = new HashMap<String, HTTPUserInterfaceClient>();
                List<Recipe> recipes = new ArrayList<Recipe>();

                for(String stargatePath : stargatePaths) {
                    DataObjectURI uri = new DataObjectURI(stargatePath);

                    try {
                        DataObjectMetadata metadata = client.getDataObjectMetadata(uri);
                        if(metadata == null) {
                            System.out.println(String.format("<%s not exist!>", uri.toString()));
                        } else if(metadata.isDirectory()) {
                            System.out.println(String.format("<%s is a directory!>", uri.toString()));
                        } else {
                            LOG.debug(String.format("Downloading a recipe for a file %s", uri.toString()));
                            Recipe recipe = client.getRecipe(uri);
                            if(recipe == null) {
                                System.out.println("<Recipe does not exist!>");
                            } else {
                                recipes.add(recipe);

                                // create connections
                                Collection<String> nodeNames = recipe.getNodeNames();
                                for(String nodeName : nodeNames) {
                                    if(!clients.containsKey(nodeName)) {
                                        Node node = cluster.getNode(nodeName);
                                        UserInterfaceServiceInfo userInterfaceServiceInfo = node.getUserInterfaceServiceInfo();
                                        URI nodeServiceURI = userInterfaceServiceInfo.getServiceURI();
                                        if(!client.getServiceURI().equals(nodeServiceURI)) {
                                            HTTPUserInterfaceClient newClient = HTTPUIClient.getClient(nodeServiceURI);
                                            clients.put(nodeName, newClient);
                                        } else {
                                            clients.put(nodeName, client);
                                        }
                                    }
                                }
                            }
                        }
                    } catch (FileNotFoundException ex) {
                        System.out.println(String.format("<%s not exist!>", uri.toString()));
                    }
                }

                // split
                Map<String, List<ChunkDownloadTask>> taskMap = new HashMap<String, List<ChunkDownloadTask>>();
                for(Recipe recipe : recipes) {
                    Collection<RecipeChunk> chunks = recipe.getChunks();

                    for(RecipeChunk chunk : chunks) {
                        Collection<Integer> nodeIDs = chunk.getNodeIDs();
                        String nodeName = recipe.getNodeName(nodeIDs.iterator().next());
                        List<ChunkDownloadTask> list = taskMap.get(nodeName);
                        if(list == null) {
                            list = new ArrayList<ChunkDownloadTask>();
                            taskMap.put(nodeName, list);
                        }
                        list.add(new ChunkDownloadTask(recipe, chunk, nodeName));
                    }
                }

                Queue<ChunkDownloadTask> taskQueue = new LinkedList<ChunkDownloadTask>();
                while(true) {
                    if(taskMap.size() > 0) {
                        for(String key : taskMap.keySet()) {
                            List<ChunkDownloadTask> list = taskMap.get(key);
                            if(list != null) {
                                if(list.size() > 0) {
                                    ChunkDownloadTask t = list.get(0);
                                    list.remove(0);
                                    taskQueue.add(t);
                                }

                                if(list.isEmpty()) {
                                    taskMap.remove(key);
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
                
                int threadNum = 16;
                ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum);
                long startTimeC = DateTimeUtils.getTimestamp();
                
                while(!taskQueue.isEmpty()) {
                    ChunkDownloadTask t = taskQueue.poll();
                    executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            Recipe recipe = t.getRecipe();
                            RecipeChunk recipeChunk = t.getRecipeChunk();
                            HTTPUserInterfaceClient chunkClient = clients.get(t.getNodeName());
                            
                            if(!chunkClient.isConnected()) {
                                try {
                                    chunkClient.connect();
                                } catch (IOException ex) {
                                    LOG.error(String.format("cannot connect to server to read a chunk %s", recipeChunk.getHash()));
                                    throw new RuntimeException(String.format("cannot connect to server to read a chunk %s", recipeChunk.getHash()));
                                }
                            }

                            DataObjectURI uri = recipe.getMetadata().getURI();

                            LOG.debug(String.format("Downloading a file %s, a chunk %s", uri.toString(), recipeChunk.getHash()));
                            FileOutputStream fos = null;
                            try {
                                File chunkFile = new File(fileDir, PathUtils.getFileName(recipeChunk.getHash()));
                                fos = new FileOutputStream(chunkFile);
                                fos.getChannel().truncate(0);
                                BufferedOutputStream bos = new BufferedOutputStream(fos);

                                int bufferlen = 1024*1024;
                                byte[] buffer = new byte[bufferlen];
                                
                                DataChunkStatus dataChunkStatus = chunkClient.requestDataChunk(uri, recipeChunk.getHash());
                                InputStream dataChunkIS = client.getDataChunk(uri, recipeChunk.getHash(), dataChunkStatus);
                                
                                int remaining = recipeChunk.getLength();
                                while(remaining > 0) {
                                    int readLen = dataChunkIS.read(buffer, 0, Math.min(remaining, bufferlen));
                                    if(readLen < 0) {
                                        //EOF
                                        break;
                                    }

                                    fos.write(buffer, 0, readLen);
                                    remaining -= readLen;
                                }
                                dataChunkIS.close();
                                bos.close();
                            } catch (FileNotFoundException ex) {
                                System.out.println(String.format("<%s not exist!>", fileDir.getPath()));
                            } catch (IOException ex) {
                                System.out.println(String.format("<Failed to download a chunk %s!>", recipeChunk.getHash()));
                            }
                        }
                    });
                }

                long endTimeC = DateTimeUtils.getTimestamp();
                LOG.debug(String.format("Downloading took - %d ms", endTimeC - startTimeC));

                // close
                for(HTTPUserInterfaceClient newClient : clients.values()) {
                    if(newClient != client) {
                        newClient.disconnect();
                    }
                }
                clients.clear();
                
                String dateTimeString = DateTimeUtils.getDateTimeString(client.getLastActiveTime());
                client.disconnect();
                System.out.println(String.format("<Request processed %s>", dateTimeString));
            } else {
                System.out.println(String.format("<Failed to create an output dir %s!>", fileDir.getPath()));
            }

            System.exit(0);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
}
