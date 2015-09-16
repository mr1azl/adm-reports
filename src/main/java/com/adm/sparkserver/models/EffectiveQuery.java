package com.adm.sparkserver.models;

import com.adm.sparkserver.utils.TimeFrameMapper;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class EffectiveQuery {

    private Double lat;
    private Double lon;
    private List<Double> radiuses;
    private List<String> applist;
    private List<String> timeFrameDates;
    private List<String> schedules;

    public EffectiveQuery() {}

    public EffectiveQuery(Double lat, Double lon, List<Double> radiuses, List<String> applist, List<String> timeFrameDates, List<String> schedules) {
        this.lat = lat;
        this.lon = lon;
        this.radiuses = radiuses;
        this.applist = applist;
        this.timeFrameDates = timeFrameDates;
        this.schedules = schedules;
    }

    public EffectiveQuery(Query q){

        this.lat = Double.valueOf(q.getLat());
        this.lon = Double.valueOf(q.getLon());

        this.applist = Arrays.asList(q.getApplist().split(","));

        this.radiuses = Arrays.asList(q.getRadiuses().split(","))
                              .stream()
                              .map(Double::valueOf)
                              .collect(Collectors.toList());

        //Collections.sort(radiuses, Collections.reverseOrder());

        //TODO Validate the Int
        this.timeFrameDates = new TimeFrameMapper().mapper(q.getTimeframe());

        //TODO look into this.
        this.schedules = Arrays.asList(q.getSchedule().split(","));

    }


    public String timeFrameDateString() {
        return "('"+String.join("','", timeFrameDates)+"')";
    }

    public String applistString(){
        return "('"+String.join("','", applist) +"')";
    }

    @Override
    public String toString() {
        return "EffectiveQuery{" +
                "lat=" + lat +
                ", lon=" + lon +
                ", radiuses=" + String.join(",", radiuses.stream().map(String::valueOf).collect(Collectors.toList())) +
                ", applist=" + String.join("," ,applist) +
                ", timeFrameDates=" + String.join(",",timeFrameDates) +
                ", schedules=" + String.join(",", schedules) +
                '}';
    }

    public Double getLat() {
        return lat;
    }

    public void setLat(Double lat) {
        this.lat = lat;
    }

    public Double getLon() {
        return lon;
    }

    public void setLon(Double lon) {
        this.lon = lon;
    }

    public List<Double> getRadiuses() {
        return radiuses;
    }

    public void setRadiuses(List<Double> radiuses) {
        this.radiuses = radiuses;
    }

    public List<String> getApplist() {
        return applist;
    }

    public void setApplist(List<String> applist) {
        this.applist = applist;
    }

    public List<String> getTimeFrameDates() {
        return timeFrameDates;
    }

    public void setTimeFrameDates(List<String> timeFrameDates) {
        this.timeFrameDates = timeFrameDates;
    }

    public List<String> getSchedules() {
        return schedules;
    }

    public void setSchedules(List<String> schedules) {
        this.schedules = schedules;
    }


    public static void main(String[] args) {
        /**
        http://91.216.58.166/ws/geo-analytics.php?zone_latitude=48.858&zone_longitude=2.38115
         &radius=200000,3000,4000
         &applist=ZQ331E24,UX196C48,TZ355R94,BJ951G63,QB631X18
         &timeframe=2
         &schedule=11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111
         **/

        Query q = new Query ("48.858","2.38115", "200000,3000,4000", "ZQ331E24,UX196C48,TZ355R94,BJ951G63,QB631X18",3,"11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111");

        EffectiveQuery fq = new EffectiveQuery(q);



        System.out.println(fq);

        System.out.println(fq.timeFrameDateString());

        System.out.println(fq.applistString());


    }

}
