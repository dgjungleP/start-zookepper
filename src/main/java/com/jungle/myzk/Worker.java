package com.jungle.myzk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.AsyncCallback.*;
import static org.apache.zookeeper.AsyncCallback.DataCallback;
import static org.apache.zookeeper.AsyncCallback.StringCallback;
import static org.apache.zookeeper.KeeperException.Code;

public class Worker extends DefaultWatcher {
    public static final Logger LOG = LoggerFactory.getLogger(javafx.concurrent.Worker.class);
    String serverId = Integer.toHexString(new Random().nextInt());
    String status;
    String name;
    StringCallback workerCreateCallback = (int rc, String path, Object ctx, String name) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                register();
                break;
            case OK:
                LOG.info("Registered successfully: " + serverId);
                break;
            case NODEEXISTS:
                LOG.warn("Already registered:" + serverId);
                break;
            default:
                LOG.error("Something went wrong: ", KeeperException.create(Code.get(rc), path));
        }
    };
    StatCallback statusUpdateCallback = (int rc, String path, Object ctx, Stat stat) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                updateStatus((String) ctx);
                return;
        }

    };


    synchronized private void updateStatus(String status) {
        if (Objects.equals(status, this.status)) {
            zk.setData("/workers/" + name, status.getBytes(StandardCharsets.UTF_8), -1, statusUpdateCallback, status);
        }
    }

    public void register() {
        this.name = "worker-" + serverId;
        zk.create("/workers/" + name, "Idle".getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, workerCreateCallback, null);
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    public Worker(String hostPort) {
        super(hostPort);
    }

    public void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }


    public static void main(String[] args) throws Exception {
        Worker worker = new Worker(args[0]);
        worker.startZK();
        worker.register();
        Thread.sleep(30000);
    }
}
