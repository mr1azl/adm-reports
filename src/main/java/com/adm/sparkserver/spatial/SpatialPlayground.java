package com.adm.sparkserver.spatial;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceCalculator;
import com.spatial4j.core.shape.Circle;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.impl.PointImpl;

import static com.spatial4j.core.distance.DistanceUtils.DEG_TO_KM;
import static com.spatial4j.core.distance.DistanceUtils.KM_TO_DEG;

/**
 * Created by samklr on 13/08/15.
 */
public class SpatialPlayground {


    public static void testSpatial(){

        /**
         * Example taken from spatial4j tests :
         * https://github.com/locationtech/spatial4j/blob/master/src/test/java/com/spatial4j/core/shape/TestShapesGeo.java
         * https://github.com/locationtech/spatial4j/tree/master/src/test/java/com/spatial4j/core/shape
         *
         */
        SpatialContext spatialContext =SpatialContext.GEO;

        PointImpl london = new PointImpl(51.5120, 0.1228, spatialContext);
        PointImpl lhr = new PointImpl(51.477500 , -0.461388, spatialContext);
        Shape heathrow =  spatialContext.makePoint(51.477500 , -0.461388);
        Point LAX = new PointImpl(33.942495, -118.408067, spatialContext);
        //Shape lax = spatialContext.makePoint(33.942495, -118.408067);

        Circle circle = spatialContext.makeCircle(london, 50);

        System.out.println("Circle relates to Point : " + circle.relate(spatialContext.makePoint(51.477500 , -0.461388)));
        System.out.println("Point relates to circle : " + heathrow.relate(circle));

        System.out.println(SpatialRelation.WITHIN == heathrow.relate(circle));
        System.out.println("LHR falls within a 50km radius of downtown london? :  " +
                (SpatialRelation.INTERSECTS == heathrow.relate(spatialContext.makeCircle(london, 50))));

        System.out.println("Is LHR in downtown london? :  " +
                ( heathrow.relate(spatialContext.makeCircle(london, 25))));

        //Distance between LAX and LHR

        DistanceCalculator dc = spatialContext.getDistCalc();
        double dist = dc.distance(london, lhr); // Gives distances in DEGREE --> We NEED to convert it to KM
        double distKM= dist * DEG_TO_KM;

        Point paris_opera = new PointImpl(48.871949, 2.331601, spatialContext);
        Point montparnasse = new PointImpl(48.8436, 2.3238, spatialContext);
        Point saint_germain = new PointImpl(48.8539, 2.3344, spatialContext);

        double dist2 = dc.distance(paris_opera, montparnasse)*DEG_TO_KM;

        System.out.println("Opera - Montparnasse : "+ dist2);

        //check if Saint-Germain-des-pres is in 2km radius of Montparnasse

        boolean sgm_around_montparnasse = SpatialRelation.WITHIN ==
                saint_germain.relate(spatialContext.makeCircle(montparnasse, 2*KM_TO_DEG));

        System.out.println("IS Saint-Germain-des-pres in 2km radius of Montparnasse ? " + sgm_around_montparnasse);

        System.out.println("IS Saint-Germain-des-pres in 1km radius of Montparnasse ? " +
                ( SpatialRelation.WITHIN == saint_germain.relate(spatialContext.makeCircle(montparnasse, 1*KM_TO_DEG))));


        //check if Montparnasse is in 4km radius of Opera

        System.out.println("Is Opera is in 3.5km radius of Montparnasse ? " +
                ( SpatialRelation.WITHIN == paris_opera.relate(spatialContext.makeCircle(montparnasse, 3.5*KM_TO_DEG))));

        //check if Montparnasse is in 3km of Eiffel Tower
        Point eiffel = new PointImpl(48.8582 , 2.2945, spatialContext);
        //Distance between
        double distance_montparnasse_opera = dc.distance(eiffel, montparnasse)*DEG_TO_KM; //3.63 km

        double distance_montparnasse_sgm = dc.distance(saint_germain, montparnasse)*DEG_TO_KM; //3.63 km

        System.out.println("Distance between Montparnasse and Saint Germain: " + distance_montparnasse_sgm);


        System.out.println("Distance between Montparnasse and Eiffel Tower: " + distance_montparnasse_opera);
        System.out.println("Is montparnasse is in 1km radius of Eiffel tower ? " +
                ( SpatialRelation.WITHIN == montparnasse.relate(spatialContext.makeCircle(eiffel, 1*KM_TO_DEG))));

        System.out.println("Is montparnasse is in 3.7km radius of Eiffel tower ? " +
                ( SpatialRelation.WITHIN == montparnasse.relate(spatialContext.makeCircle(eiffel, 3.7*KM_TO_DEG))));
    }


    public static void main(String[] args){
        testSpatial();
    }

}
