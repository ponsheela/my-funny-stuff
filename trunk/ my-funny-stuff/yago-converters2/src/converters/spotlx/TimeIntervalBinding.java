package converters.spotlx;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * BerkeleyDB binding for TimeInterval class.
 *
 * @author kberberi
 */
public class TimeIntervalBinding extends TupleBinding<TimeInterval> {

    @Override
    public TimeInterval entryToObject(TupleInput ti) {
        long begin = ti.readLong();
        long end = ti.readLong();
        return new TimeInterval(begin, end);
    }

    @Override
    public void objectToEntry(TimeInterval e, TupleOutput to) {
        to.writeLong(e.begin);
        to.writeLong(e.end);
    }

}
