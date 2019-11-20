package com.atguigu.bean;

/**
 * 用户后台活跃
 */
public class AppActive_background {

    //1=upgrade,2=download(下载),3=plugin_upgrade
    private String active_source;


    public String getActive_source() {
        return active_source;
    }

    public void setActive_source(String active_source) {
        this.active_source = active_source;
    }
}
