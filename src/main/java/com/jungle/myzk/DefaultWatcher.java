package com.jungle.myzk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public abstract class DefaultWatcher implements Watcher {
    ZooKeeper zk;
    String hostPort;

    public DefaultWatcher(String hostPort) {
        this.hostPort = hostPort;
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("event = " + event);
    }
}
