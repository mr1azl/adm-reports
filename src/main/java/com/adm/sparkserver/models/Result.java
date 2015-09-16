package com.adm.sparkserver.models;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Result {

    Map<Double, List<Row>> mapDay ;
    Map<Double, List<Row>> mapPub ;

    public Result(Map<Double, List<Row>> mapDay, Map<Double, List<Row>> mapPub) {
        this.mapDay = mapDay;
        this.mapPub = mapPub;
    }

    public String toJson(){

        ObjectMapper mapper = new ObjectMapper();

        Map<Double, List<StatsByDay>>  mDay = new HashMap<>();
        Map<Double, List<StatsByPubuids>>  mPub = new HashMap<>();

        mapDay.keySet().forEach(ks ->
              mDay.put(ks, new StatsByDay().fromRows(mapDay.get(ks)))
          );

        mapPub.keySet().forEach(ks ->
              mPub.put(ks,new StatsByPubuids().fromRows(mapPub.get(ks)))
        );

        String res =null;

        try {

            res = " { \"statsByDay\" :  " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mDay) +" ,\n" +
                     "\"statsByPubuid\" : " + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mPub) +" }";

        } catch (JsonProcessingException e) {
            System.err.println("Error " + e.getMessage());

         }

        return res;
    }



    // ########################################## Ulitity classes for JSON Generation ############## //


     class StatsByDay {
        private String log_day;
        private Long count_impression;
        private Long users;
        private Long impflags;

        public StatsByDay() {}

        public StatsByDay(String log_day, Long count_impression, Long users, Long impflags) {
            this.log_day = log_day;
            this.count_impression = count_impression;
            this.users = users;
            this.impflags = impflags;
        }

        public  StatsByDay(Row row){
            this.log_day = row.getString(0);
            this.count_impression = row.getLong(1);
            this.users = row.getLong(2);
            this.impflags = row.getLong(3);

        }

        public List<StatsByDay> fromRows(List<Row> rows) {
            List<StatsByDay> stats = new ArrayList<>();
            rows.forEach(row -> stats.add(new StatsByDay(row)));
            return stats;
        }

        public String getLog_day() {
            return log_day;
        }

        public void setLog_day(String log_day) {
            this.log_day = log_day;
        }

        public Long getCount_impression() {
            return count_impression;
        }

        public void setCount_impression(Long count_impression) {
            this.count_impression = count_impression;
        }

        public Long getUsers() {
            return users;
        }

        public void setUsers(Long users) {
            this.users = users;
        }

        public Long getImpflags() {
            return impflags;
        }

        public void setImpflags(Long impflags) {
            this.impflags = impflags;
        }

    }

    class StatsByPubuids {
        private String log_day;
        private String pubuid;
        private Long count_impression;
        private Long users;
        private Long impflags;

        public StatsByPubuids(String log_day, String pubuid, Long count_impression, Long users, Long impflags) {
            this.log_day = log_day;
            this.pubuid = pubuid;
            this.count_impression = count_impression;
            this.users = users;
            this.impflags = impflags;
        }

        public StatsByPubuids() {}

        public StatsByPubuids(Row row){
            this.log_day = row.getString(0);
            this.pubuid = row.getString(1);
            this.count_impression = row.getLong(2);
            this.users = row.getLong(3);
            this.impflags = row.getLong(4);
        }


        public  List<StatsByPubuids> fromRows(List<Row> rows) {
            List<StatsByPubuids> stats = new ArrayList<>();
            rows.forEach(row -> stats.add(new StatsByPubuids(row)));
            return stats;
        }


        public String getPubuid() {
            return pubuid;
        }

        public void setPubuid(String pubuid) {
            this.pubuid = pubuid;
        }

        public String getLog_day() {
            return log_day;
        }

        public void setLog_day(String log_day) {
            this.log_day = log_day;
        }

        public Long getCount_impression() {
            return count_impression;
        }

        public void setCount_impression(Long count_impression) {
            this.count_impression = count_impression;
        }

        public Long getUsers() {
            return users;
        }

        public void setUsers(Long users) {
            this.users = users;
        }

        public Long getImpflags() {
            return impflags;
        }

        public void setImpflags(Long impflags) {
            this.impflags = impflags;
        }

    }
}
