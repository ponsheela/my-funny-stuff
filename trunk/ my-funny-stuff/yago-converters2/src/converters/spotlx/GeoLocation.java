package converters.spotlx;


/**
 * Represents a geographic location as a latitude/longitude pair.
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class GeoLocation {

    public double latitude;

    public double longitude;

    public GeoLocation(double latitude, double longitude) {
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
}
