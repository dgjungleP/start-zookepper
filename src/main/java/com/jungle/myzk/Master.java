package com.jungle.myzk;

import javafx.concurrent.Worker;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Random;

import static org.apache.zookeeper.AsyncCallback.*;
import static org.apache.zookeeper.KeeperException.*;

public class Master extends DefaultWatcher {
    public static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    public static final String PATH = "/master";
    enum MasterStates {RUNNING, ELECTED, NOTELECTED;}

    String serverId = Integer.toHexString(new Random().nextInt());
    volatile MasterStates state = MasterStates.RUNNING;
    private final Watcher masterExistsWatcher = event -> {
        if (Objects.equals(event.getType(), EventType.NodeDeleted)) {
            assert PATH.equals(event.getPath());
            runForMaster();
        }
    };
    private StatCallback masterExistsCallback = (int rc, String path, Object ctx, Stat stat) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                masterExists();
                break;
            case OK:
                if (stat == null) {
                    this.state = MasterStates.RUNNING;
                    runForMaster();
                }
                break;
            default:
                checkMaster();
                break;
        }
    };
    StringCallback masterCreateCallback = (int rc, String path, Object ctx, String name) -> {
        switch (Code.get(rc)) {
            case OK:
                state = MasterStates.ELECTED;
                takeLeadership();
                break;
            case CONNECTIONLOSS:
                checkMaster();
                break;
            case NODEEXISTS:
                state = MasterStates.NOTELECTED;
                masterExists();
                break;
            default:
                state = MasterStates.NOTELECTED;
                LOG.error("Something went wrong when running for master.", KeeperException.create(Code.get(rc), path));

        }
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


    private void takeLeadership() {
        System.out.println("I`m the master.");
    }

    private void masterExists() {
        zk.exists(PATH, masterExistsWatcher, masterExistsCallback, null);
    }
    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void stopZk() throws InterruptedException {
        zk.close();
    }

    public MasterStates getState() {
        return state;
    }

    public void runForMaster() {
        zk.create(PATH, serverId.getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    public void checkMaster() {
        zk.getData(PATH, false, masterCheckCallBack, null);
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
