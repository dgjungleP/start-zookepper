package com.jungle.myzk;

import javafx.concurrent.Worker;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.apache.zookeeper.AsyncCallback.DataCallback;
import static org.apache.zookeeper.AsyncCallback.StringCallback;
import static org.apache.zookeeper.KeeperException.Code;

public class Master extends DefaultWatcher {
    public static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    String serverId = Integer.toHexString(new Random().nextInt());

    boolean isLeader;
    StringCallback masterCreateCallback = (int rc, String path, Object ctx, String name) -> {
        switch (Code.get(rc)) {
            case OK:
                isLeader = true;
                bootStrap();
                break;
            case CONNECTIONLOSS:
                checkMaster();
                return;
            default:
                isLeader = false;
        }
        System.out.println("I`m " + (isLeader ? "" : "not") + " the leader");
    };
    DataCallback masterCheckCallBack = (int rc, String path, Object ctx, byte[] data, Stat stat) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMaster();
                return;
            case NONODE:
                runForMaster();
                return;
        }
    };
    StringCallback creatParentCallback = (int rc, String path, Object ctx, String name) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                createParent(path, (byte[]) ctx);
                break;
            case OK:
                LOG.info("Parent Created");
                break;
            case NODEEXISTS:
                LOG.warn("Parent already registered: " + path);
                break;
            default:
                LOG.error("Something went wrong: ", KeeperException.create(Code.get(rc), path));
        }
    };

    public Master(String hostPort) {
        super(hostPort);
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void stopZk() throws InterruptedException {
        zk.close();
    }

    public void runForMaster() {
        zk.create("/master", serverId.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    public void checkMaster() {
        zk.getData("/master", false, masterCheckCallBack, null);
    }


    public void bootStrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    private void createParent(String path, byte[] data) {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                creatParentCallback, data);
    }

    public static void main(String[] args) throws Exception {
        Master master = new Master(args[0]);
        master.startZK();
        master.runForMaster();
        Thread.sleep(60000);
        master.stopZk();
    }
}