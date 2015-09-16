package com.adm.sparkserver;

import com.adm.sparkserver.models.EffectiveQuery;
import com.adm.sparkserver.models.Query;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;


public class TimeFrameMapperTest {

    private String query =
            "?zone_latitude=48.858&zone_longitude=2.38115&radius=200000,3000,4000&applist=ZQ331E24,UX196C48,TZ355R94,BJ951G63,QB631X18&timeframe=2&schedule=11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111,11111111111111111111111";


    private EffectiveQuery fq;

    @Before
    public void before() {
        // String Parse
        fq = new EffectiveQuery(
                new Query ("48.858","2.38115", "4000,213333,2000", "ZQ331E24,UX196C48,TZ355R94,BJ951G63,QB631X18",
                             0,"11111111111111111111111,11111111111111111111111,11111111111111111111111," +
                           "11111111111111111111111,11111111111111111111111,11111111111111111111111," +
                           "11111111111111111111111")
        );
    }


    @Test
    public void testValidElements() {
        DateTimeFormatter jf = DateTimeFormat.forPattern("yyyy-MM-dd");

        assertEquals(fq.getApplist(), Arrays.asList("ZQ331E24","UX196C48","TZ355R94","BJ951G63","QB631X18"));
        assertEquals(fq.getRadiuses(),Arrays.asList(4000.0,213333.0,2000.0) );
        assertEquals(fq.getTimeFrameDates(), Arrays.asList(jf.print(new LocalDate())) );


    }

}
