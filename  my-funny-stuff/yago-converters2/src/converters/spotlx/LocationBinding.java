package converters.spotlx;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * BerkeleyDB binding for Location class.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class LocationBinding extends TupleBinding<GeoLocation> {

    @Override
    public GeoLocation entryToObject(TupleInput ti) {
        double latitude = ti.readDouble();
        double longitude = ti.readDouble();
        return new GeoLocation(latitude, longitude);
    }

    @Override
    public void objectToEntry(GeoLocation e, TupleOutput to) {
        to.writeDouble(e.latitude);
        to.writeDouble(e.longitude);
    }

}
