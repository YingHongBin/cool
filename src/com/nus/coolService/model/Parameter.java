package com.nus.coolService.model;

public class Parameter implements Comparable<Parameter> {

    private Integer priority;

    private String content;

    public Parameter(int priority, String content) {
        this.content = content;
        this.priority = priority;
    }

    public Integer getPriority() {
        return this.priority;
    }

    public String getContent() {
        return this.content;
    }

    @Override
    public int compareTo(Parameter p) {
        return this.priority.compareTo(p.getPriority());
    }
}
