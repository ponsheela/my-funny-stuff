package converters.spotlx;



/**
 * Represents a time interval using UNIX time format to encode its boundaries.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class TimeInterval {

    //public static final long MIN_TIMESTAMP = -210866889600000L;   // In Postgres
	
	  public static final long MIN_TIMESTAMP = 0L;   // In MySQL

    //public static final long MAX_TIMESTAMP = 884541340800000L; // In Postgres
 
	  public static final long MAX_TIMESTAMP = 884541340800000L; // In Postgres

    public long begin;

    public long end;

    public TimeInterval() {
        this.begin = MIN_TIMESTAMP;
        this.end = MAX_TIMESTAMP;
    }

    public TimeInterval(long begin, long end) {
        this.begin = begin;
        this.end = end;
    }

    public long getBegin() {
        return begin;
    }

    public long getEnd() {
        return end;
    }

    public void setBegin(long begin) {
        this.begin = begin;
    }

    public void setEnd(long end) {
        this.end = end;
    }
}
