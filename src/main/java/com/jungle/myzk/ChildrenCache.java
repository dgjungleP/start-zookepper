package com.jungle.myzk;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ChildrenCache {
    protected List<String> children;

    ChildrenCache() {
        this.children = null;
    }

    ChildrenCache(List<String> children) {
        this.children = children;
    }

    List<String> getList() {
        return children;
    }

    List<String> addedAndSet(List<String> newChildren) {
        List<String> diff = new ArrayList<>(newChildren);
        if (children != null) {
            diff = newChildren.stream()
                    .filter(data -> !children.contains(data))
                    .collect(Collectors.toList());
        }
        this.children = newChildren;
        return diff;
    }

    List<String> removedAndSet(List<String> newChildren) {
        List<String> diff = null;
        if (children != null) {
            diff = newChildren.stream()
                    .filter(data -> !children.contains(data))
                    .collect(Collectors.toList());
        }
        this.children = newChildren;
        return diff;
    }
}
