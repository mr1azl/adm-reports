package com.adm.sparkserver.utils;

import java.util.concurrent.TimeUnit;


public class TimeCounter {

     private final long start;

     public TimeCounter() {
            start = System.nanoTime();
     }

     /**
      * Returns the elapsed CPU time (in seconds) since initialization
     **/
     public long elapsedTime() {
        long now = System.nanoTime();
        return  TimeUnit.NANOSECONDS.toSeconds(now - start);
     }

}
