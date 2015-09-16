package com.adm.sparkserver.models;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Query {
    private String lat;
    private String lon;
    private String radiuses;
    private String applist;
    private int timeframe;
    private String schedule;


    public Query(){}

    public Query(String lat, String lon, String radiuses, String applist, int timeframe, String schedule) {
        this.lat = lat;
        this.lon = lon;
        this.radiuses = radiuses;
        this.applist = applist;
        this.timeframe = timeframe;
        this.schedule = schedule;
    }


    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getLon() {
        return lon;
    }

    public void setLon(String lon) {
        this.lon = lon;
    }

    public String getRadiuses() {
        return radiuses;
    }

    public void setRadiuses(String radiuses) {
        this.radiuses = radiuses;
    }

    public String getApplist() {
        return applist;
    }

    public void setApplist(String applist) {
        this.applist = applist;
    }

    public int getTimeframe() {
        return timeframe;
    }

    public void setTimeframe(int timeframe) {
        this.timeframe = timeframe;
    }

    public String getSchedule() {
        return schedule;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }
}
