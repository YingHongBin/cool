package com.nus.cool.core.OLAP;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * @author yhb
 */
public class Node {

    private Node parent;

    private List<Node> children = new ArrayList<>();

    private String value;

    private String level;

    private boolean isLeaf;

    private boolean isRoot;

    public Node(String value, String level) {
        this.value = value;
        this.level = level;
    }

    public Node getParent() {
        return parent;
    }

    public void setParent(Node parent) {
        this.parent = parent;
    }

    public List<Node> getChildren() {
        return children;
    }

    public void setChildren(List<Node> children) {
        this.children = children;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public boolean isRoot() {
        return isRoot;
    }

    public void setRoot(boolean root) {
        isRoot = root;
    }

    private void build() {
        if (this.children.size() == 0) {
            this.isLeaf = true;
        } else {
            for (Node child : this.children) {
                child.setParent(this);
                child.build();
            }
        }
        if (this.parent == null) this.isRoot = true;
    }

    public static Map readFrom(String filePath) throws IOException {
        Node root = new Node("root", "root");
        BufferedReader reader;
        reader = new BufferedReader(new FileReader(filePath));
        String[] fields = reader.readLine().split(",");
        Map<String, Map<String, Node>> map = new HashMap<>();
        for(String field : fields) {
            Map<String, Node> nodeMap = new HashMap<>();
            map.put(field, nodeMap);
        }
        String line;
        while((line = reader.readLine()) != null) {
            String[] values = line.split(",");
            for (int i = 0; i < values.length; i++) {
                Node node;
                if((node = map.get(fields[i]).get(values[i])) == null) {
                    node = new Node(values[i], fields[i]);
                    map.get(fields[i]).put(values[i], node);
                }
                if( i > 0 ) {
                    Node child = map.get(fields[i - 1]).get(values[i - 1]);
                    if(!node.getChildren().contains(child)) {
                        node.getChildren().add(child);
                    }
                }
            }
        }
        for (Map.Entry<String, Node> entry : map.get(fields[fields.length - 1]).entrySet()) {
            root.getChildren().add(entry.getValue());
        }
        root.build();
        return root.getNodes();
    }

    public List<Node> getLeaves() {
        List<Node> leaves = new ArrayList<>();
        if(this.isLeaf) {
            leaves.add(this);
        } else {
            for (Node child : this.children) {
                leaves.addAll(child.getLeaves());
            }
        }
        return leaves;
    }

    public Node getRoot() {
        if (this.isRoot) {
            return this;
        } else {
            return this.parent.getRoot();
        }
    }

    public String getChildLevel() {
        if (this.isLeaf) {
            return this.level;
        } else {
            return this.getChildren().get(0).getLevel();
        }
    }

    private Map getNodes() {
        Map<List<String>, Node> map = new HashMap<>();
        map.put(Arrays.asList(this.getLevel(), this.getValue()), this);
        if (!this.isLeaf) {
            for (Node child : this.getChildren()) {
                map.putAll(child.getNodes());
            }
        }
        return map;
    }
}
