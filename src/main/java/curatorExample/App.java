package curatorExample;

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Hello world!
 *
 */
public class App 
{
    private static final  String ZK_PATH = "/robbie";
    public static void main( String[] args ) throws Exception
    {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,3);
        CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.21.241:2181", retryPolicy);
        client.start();

       // String data = "test data";
        //client.create().forPath(ZK_PATH, data.getBytes());

        System.out.println("get path: "  + client.getChildren().forPath("/"));
        System.out.println("data: "+ new String(client.getData().forPath(ZK_PATH)));
        System.out.println( "Hello World!" );
    }
}
