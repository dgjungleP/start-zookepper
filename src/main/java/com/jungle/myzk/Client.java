package com.jungle.myzk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.apache.zookeeper.KeeperException.ConnectionLossException;
import static org.apache.zookeeper.KeeperException.NodeExistsException;

public class Client extends DefaultWatcher {
    public Client(String hostPort) {
        super(hostPort);
    }

    public void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public String queueCommand(String command) throws Exception {
        String name = "";
        while (true) {
            try {
                name = zk.create("/tasks/task-", command.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (NodeExistsException e) {
                throw new Exception(name + " already appears to be running");
            } catch (ConnectionLossException e) {
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Client client = new Client(args[0]);
        client.startZk();
        String command = client.queueCommand(args[1]);
        System.out.println("command = " + command);
    }
}
