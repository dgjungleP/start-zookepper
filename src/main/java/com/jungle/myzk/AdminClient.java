package com.jungle.myzk;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;

public class AdminClient extends DefaultWatcher {

    public AdminClient(String hostPort) {
        super(hostPort);
    }


    public void startZk() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void listStat() throws InterruptedException, KeeperException {
        try {
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master", false, stat);
            Date startTime = new Date(stat.getCtime());
            System.out.println("Master: " + new String(masterData) + " since " + startTime);
        } catch (NoNodeException e) {
            System.out.println("No master");
        }

        System.out.println("Workers:");
        for (String child : zk.getChildren("/workers", false)) {
            byte[] data = zk.getData("/workers/" + child, false, null);
            String stat = new String(data);
            System.out.println("\t" + child + ": " + stat);
        }
        System.out.println("Tasks:");
        for (String task : zk.getChildren("/tasks", false)) {
            System.out.println("\t" + task);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        AdminClient adminClient = new AdminClient(args[0]);
        adminClient.startZk();
        adminClient.listStat();
    }
}
