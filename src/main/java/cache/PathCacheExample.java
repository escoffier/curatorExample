package cache;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

public class PathCacheExample {

    private static final String PATH = "/robbie/cache";
    private static final String connectString = "192.168.21.241:2181";

    private static  final Logger logger = LoggerFactory.getLogger(PathCacheExample.class);
    public static void main(String[] args) throws Exception {
        CuratorFramework client = null;
        PathChildrenCache cache = null;

        try {
            client = CuratorFrameworkFactory.newClient(connectString, new ExponentialBackoffRetry(1000, 3));
            client.start();

            cache = new PathChildrenCache(client, PATH, true);
            cache.start();

            cache.getListenable().addListener(new PathChildrenCacheListener() {
                @Override
                public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                    switch (event.getType()) {
                        case CHILD_ADDED: {
                            System.out.println("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                            logger.info("Node added: {}", ZKPaths.getNodeFromPath(event.getData().getPath()));
                            break;
                        }

                        case CHILD_UPDATED:
                        {
                            System.out.println("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                            break;
                        }

                        case CHILD_REMOVED:
                        {
                            System.out.println("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                            break;
                        }
                    }
                }
            });

            processCommands(client, cache);
        } finally {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);

        }
    }


    private static void processCommands(CuratorFramework client, PathChildrenCache cache) throws Exception {
        printHelp();

        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            boolean         done = false;

            while (!done){
                System.out.print("> ");
                String      line = in.readLine();
                if ( line == null )
                {
                    break;
                }

                String      command = line.trim();
                String[]    parts = command.split("\\s");
                if ( parts.length == 0 )
                {
                    continue;
                }
                String      operation = parts[0];
                String      args[] = Arrays.copyOfRange(parts, 1, parts.length);

                if ( operation.equalsIgnoreCase("help") || operation.equalsIgnoreCase("?") )
                {
                    printHelp();
                }
                else if ( operation.equalsIgnoreCase("q") || operation.equalsIgnoreCase("quit") )
                {
                    done = true;
                }
                else if ( operation.equals("set") )
                {
                    setValue(client, command, args);
                }
                else if ( operation.equals("remove") )
                {
                    remove(client, command, args);
                }
                else if ( operation.equals("list") )
                {
                    list(cache);
                }

                Thread.sleep(1000); // just to allow the console output to catch up

            }

        } finally {

        }
    }


    private static void setValue(CuratorFramework client, String command, String[] args) throws Exception {
        if ( args.length != 2 )
        {
            System.err.println("syntax error (expected set <path> <value>): " + command);
            return;
        }

        String      name = args[0];
        if ( name.contains("/") )
        {
            System.err.println("Invalid node name" + name);
            return;
        }

        String path = ZKPaths.makePath(PATH, name);

        byte[] data = args[1].getBytes();

        try {
            client.setData().forPath(path, data);

        } catch (KeeperException.NoNodeException e) {
            client.create().creatingParentContainersIfNeeded().forPath(path, data);

        }
    }

    private static void list(PathChildrenCache cache) {
        if (cache.getCurrentData().size() == 0) {
            System.out.println("----Empty----");
        } else {
            for (ChildData data : cache.getCurrentData()) {
                System.out.println(data.getPath() + " = " + new String(data.getData()));
            }
        }


    }

    private static void remove(CuratorFramework client, String command, String[] args) throws Exception {

    }

    private static void printHelp()
    {
        System.out.println("An example of using PathChildrenCache. This example is driven by entering commands at the prompt:\n");
        System.out.println("set <name> <value>: Adds or updates a node with the given name");
        System.out.println("remove <name>: Deletes the node with the given name");
        System.out.println("list: List the nodes/values in the cache");
        System.out.println("quit: Quit the example");
        System.out.println();
    }

}
