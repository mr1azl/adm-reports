package com.adm.sparkserver;


import com.adm.sparkserver.spatial.SpatialFilter;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.impl.PointImpl;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class SpatialFilterTest {
    SpatialContext spatialContext = SpatialContext.GEO;
    Point opera = new PointImpl(48.871949, 2.331601, spatialContext);
    Point montparnasse = new PointImpl(48.8436, 2.3238, spatialContext);;
    Point sgermain = new PointImpl(48.8539, 2.3344, spatialContext);;


    @Test
    public  void  testWithinCircle(){

        assertTrue(SpatialFilter.within(opera.getX(), opera.getY(), 3D, sgermain.getX(), sgermain.getY()));
        assertFalse(SpatialFilter.within(opera.getX(), opera.getY(), 2D, montparnasse.getX(), montparnasse.getY()));
        assertTrue(SpatialFilter.within(opera.getX(), opera.getY(), 3.5, montparnasse.getX(), montparnasse.getY()));
        assertFalse(SpatialFilter.within(sgermain.getX(), sgermain.getY(), 1.4, montparnasse.getX(), montparnasse.getY()));

    }


    public static void testWithinPolygon(){

    }

}
