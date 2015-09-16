package com.adm.sparkserver.utils;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class TimeFrameMapper {
    private LocalDate today;
    private DateTimeFormatter jf = DateTimeFormat.forPattern("yyyy-MM-dd");

    public TimeFrameMapper(){
        today = new LocalDate();
    }

    /**
     * @param timeframe : { 0 = Today, 1 = Yesterday, 2 = Last Week(Mon => Sun), 3 = Last 7 days, 4 = CurrentMonth, 5 = Last 30 days }
     * @return List of corresponding date strings
     */
    public List<String> mapper(int timeframe){

        switch (timeframe) {
            case 0 : return Arrays.asList(jf.print(DateTime.now())); //Today
            case 1 : return Arrays.asList(jf.print(DateTime.now().minusDays(1))); //Yesterday
            case 2 : return this.lastWeek(); //Last Week
            case 3 : return this.lastSevenDays(); //Last 7 days
            case 4 : return this.currentMonth() ; //CurrentMonth until today
            case 5 : return this.currentMonth() ; // Last 30 days. Today not comprised
            default : return new ArrayList<String>();
        }
    }

    /**
     * @return the last seven days including today.
     */
    public List<String> lastSevenDays(){
        List<String> lastSevenD= new ArrayList<>();

        for (LocalDate last = today.minusDays(6); last.isBefore(today) || last.isEqual(today); last=last.plusDays(1)){
            lastSevenD.add(jf.print(last));
        }
        return lastSevenD;
    }


    /**
     * @return return the list of last weeks days string representations
     */
    public List<String> lastWeek(){
        List<String> lastWk = new ArrayList<>();

        LocalDate lastWeek = today.minusWeeks(1);
        LocalDate end = lastWeek.dayOfWeek().withMaximumValue();

        for (LocalDate start= lastWeek.dayOfWeek().withMinimumValue();
             start.isBefore(end) || start.isEqual(end);
             start = start.plusDays(1) )
        {
            lastWk.add(jf.print(start));
        }

        return lastWk;
    }


    /**
     *
     * @return a list of the days from the 1st of this month until today, included.
     */
    public List<String> currentMonth(){
        List<String> thisMonth = new ArrayList<String>();

        for ( LocalDate start= today.dayOfMonth().withMinimumValue();
              start.isBefore(today) || start.isEqual(today);
              start = start.plusDays(1) )
        {
            thisMonth.add(jf.print(start));
        }
        return thisMonth;
    }

    /**
     *
     * @return A list containing the last thirty days except today
     */
    public List<String> lastThirtyDays(){
        List<String> lastMonth = new ArrayList<String>();
        int idx = 0;

        for ( LocalDate start= today.minusDays(30);
              idx < 30;
              start = start.plusDays(1) )
        {
            lastMonth.add(jf.print(start));
            idx ++;
        }
        return lastMonth;
    }


}
