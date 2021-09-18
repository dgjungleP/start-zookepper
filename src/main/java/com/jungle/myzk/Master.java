package com.jungle.myzk;

import javafx.concurrent.Worker;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.zookeeper.AsyncCallback.*;
import static org.apache.zookeeper.KeeperException.Code;
import static org.apache.zookeeper.ZooDefs.Ids;

public class Master extends DefaultWatcher {


    enum MasterStates {RUNNING, ELECTED, NOTELECTED;}


    private final Random RANDOM = new Random();
    public static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    public static final String PATH = "/master";
    String serverId = Integer.toHexString(RANDOM.nextInt());
    volatile MasterStates state = MasterStates.RUNNING;
    ChildrenCache workersCache;


    private StringCallback assignTaskCallback = (rc, path, ctx, name) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                createAssignment(path, (byte[]) ctx);
                break;
            case OK:
                LOG.info("Task assigned correctly: " + name);
                deleteTask(name.substring(name.lastIndexOf("/") + 1));
                break;
            case NODEEXISTS:
                LOG.warn("Task already assigned");
                break;
            default:
                LOG.error("Error when trying to assign task.", KeeperException.create(Code.get(rc), path));
        }
    };
    private DataCallback taskDataCallback = (rc, path, ctx, data, stat) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                getTaskData((String) ctx);
                break;
            case OK:
                List<String> workersCacheList = workersCache.getList();
                int worker = RANDOM.nextInt(workersCacheList.size());
                String designatedWorker = workersCacheList.get(worker);

                String assignmentPath = "/assign/" + designatedWorker + "/" + ctx;
                createAssignment(assignmentPath, data);
                break;
            default:
                LOG.error("Error when trying to get task data.", KeeperException.create(Code.get(rc), path));
        }
    };
    private final ChildrenCallback workersGetChildrenCallback = (rc, path, ctx, children) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                getWorkers();
                break;
            case OK:
                LOG.info("Successfully got a list of workers: " + children.size() + " workers");
                reassignAndSet(children);
                break;
            default:
                LOG.error("getChildren failed", KeeperException.create(Code.get(rc), path));
        }
    };
    Watcher workersChangeWatcher = event -> {
        if (event.getType().equals(EventType.NodeChildrenChanged)) {
            assert "/workers".equals(event.getPath());
            getWorkers();
        }
    };
    private final ChildrenCallback tasksGetChildrenCallback = (rc, path, ctx, children) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                assignTasks(children);
                break;
            default:
                LOG.error("getChildren failed", KeeperException.create(Code.get(rc), path));
        }
    };
    Watcher tasksChangeWatcher = event -> {
        if (event.getType().equals(EventType.NodeChildrenChanged)) {
            assert "/tasks".equals(event.getPath());
            getTasks();
        }
    };
    private final Watcher masterExistsWatcher = event -> {
        if (Objects.equals(event.getType(), EventType.NodeDeleted)) {
            assert PATH.equals(event.getPath());
            runForMaster();
        }
    };
    private final StatCallback masterExistsCallback = (int rc, String path, Object ctx, Stat stat) -> {
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


    private void deleteTask(String path) {

    }

    private void createAssignment(String path, byte[] data) {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignTaskCallback, data);
    }

    private void assignTasks(List<String> children) {
        if (children == null) {
            return;
        }
        for (String task : children) {
            getTaskData(task);
        }
    }

    private void getTaskData(String task) {
        zk.getData("/tasks/" + task, false, taskDataCallback, task);
    }


    private void getTasks() {
        zk.getChildren("/tasks", tasksChangeWatcher, tasksGetChildrenCallback, null);
    }

    private void getWorkers() {
        zk.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null);
    }

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

    private void reassignAndSet(List<String> children) {
        List<String> toProcess;
        if (workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            LOG.info("Removing and setting");
            toProcess = workersCache.removedAndSet(children);
        }
        if (toProcess != null) {
            for (String worker : toProcess) {
                getAbsentWorkerTasks(worker);
            }
        }
    }

    private void getAbsentWorkerTasks(String worker) {
    }


    public MasterStates getState() {
        return state;
    }

    public void runForMaster() {
        zk.create(PATH, serverId.getBytes(StandardCharsets.UTF_8),
                Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
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
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                creatParentCallback, data);
    }

    public static void main(String[] args) throws Exception {
        Master master = new Master(args[0]);
        master.startZK();
        master.runForMaster();
        TimeUnit.SECONDS.sleep(10000);
        master.stopZk();
    }
}
