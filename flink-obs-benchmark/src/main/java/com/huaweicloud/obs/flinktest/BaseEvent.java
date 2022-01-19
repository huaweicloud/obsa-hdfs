package com.huaweicloud.obs.flinktest;

import java.util.Random;

/**
 * 功能描述
 *
 * @since 2021-08-11
 */
public class BaseEvent {
    long id;
    String name;
    String age;
    boolean sex;
    String parent;
    String project;
    String project_id;
    String fee;
    String cost_time;

    public BaseEvent(long id, String name, String age, boolean sex, String parent, String project, String fee, String cost_time) {
        this.project_id = String.valueOf(new Random().nextInt(10) + 1);
        this.id = id;
        this.name = name;
        this.age = age;
        this.sex = sex;
        this.parent = parent;
        this.project = project;
        this.fee = fee;
        this.cost_time = cost_time;
    }

    public String getProject_id() {
        return project_id;
    }
}
