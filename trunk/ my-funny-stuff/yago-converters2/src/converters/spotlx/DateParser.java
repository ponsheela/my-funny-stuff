
package converters.spotlx;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * Parses and rounds YYYY-MM-DD date format allowing for trailing wild cards (e.g., 1925-12-##)
 *
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class DateParser {

    private static final Calendar cal = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));

    private static final Pattern pattern = Pattern.compile("[-]?[0-9]+[0-9#]{0,3}\\-[0-1#][0-9#]-[0-3#][0-9#]");

    public static long floorDate(String date) {
        long ms = TimeInterval.MIN_TIMESTAMP;
        if (pattern.matcher(date).matches()) {
            // does this date refer to a negative year
            boolean negativeYear = date.startsWith("-");
            date = (negativeYear ? date.substring(1) : date);

            // split date into constituents
            String[] ymd = date.split("-");

            // replace wild cards
            int year = Integer.parseInt(ymd[0].replaceAll("#", "0"));
            int month = (!ymd[1].equals("##") ? Integer.parseInt(ymd[1]) : 1);
            int day = (!ymd[2].equals("##") ? Integer.parseInt(ymd[2]) : 1);

            synchronized (cal) {
                cal.clear();
                cal.set(Calendar.ERA, (negativeYear ? GregorianCalendar.BC : GregorianCalendar.AD));
                cal.set(Calendar.YEAR, year);
                cal.set(Calendar.MONTH, month - 1);
                cal.set(Calendar.DAY_OF_MONTH, day);
                
                if (isJulianLeapDay(cal)) {
                  cal.add(Calendar.DAY_OF_MONTH, -1);
                }
                
                ms = cal.getTimeInMillis();
            }
        }
        return ms;
    }

    public static long ceilDate(String date) {
        long ms = TimeInterval.MAX_TIMESTAMP;
        if (pattern.matcher(date).matches()) {
            // does this date refer to a negative year
            boolean negativeYear = date.startsWith("-");
            date = (negativeYear ? date.substring(1) : date);

            // split date into constituents
            String[] ymd = date.split("-");

            // replace wild cards
            int year = Integer.parseInt(ymd[0].replaceAll("#", "0"));
            int month = (!ymd[1].equals("##") ? Integer.parseInt(ymd[1]) : 1);
            int day = (!ymd[2].equals("##") ? Integer.parseInt(ymd[2]) : 1);

            synchronized (cal) {
                cal.clear();
                cal.set(Calendar.ERA, (negativeYear ? GregorianCalendar.BC : GregorianCalendar.AD));
                cal.set(Calendar.YEAR, year);
                cal.set(Calendar.MONTH, month - 1);
                cal.set(Calendar.DAY_OF_MONTH, day);
                
                // ceil taking into account the most significant wild card
                if (ymd[0].contains("#")) {
                    int pad = ymd[0].length() - ymd[0].indexOf('#');
                    cal.add(Calendar.YEAR, (int) (Math.pow(10, pad)));
                } else if (ymd[1].equals("##")) {
                    cal.add(Calendar.YEAR, 1);
                } else if (ymd[2].equals("##")) {
                    cal.add(Calendar.MONTH, 1);
                } else {
                    cal.add(Calendar.DAY_OF_MONTH, 1);
                }
                                
                cal.add(Calendar.MILLISECOND, -1);
                
                if (isJulianLeapDay(cal)) {
                  cal.add(Calendar.DAY_OF_MONTH, -1);
                }
                
                ms = cal.getTimeInMillis();
            }
        }
        return ms;
    }

    public static boolean isJulianLeapDay(Calendar c) {
      int year = c.get(Calendar.YEAR);
      if (year < 1582) {
        // java uses Julian here, but SQL Gregorian.
        // i.e. years divisible by 100 ARE leap years in Java, but not in SQL
      }
      if ((year % 100 == 0)) {
        int month = c.get(Calendar.MONTH);
        int day = c.get(Calendar.DAY_OF_MONTH);
        if (month == Calendar.FEBRUARY && day == 29) {
          return true;
        }
      }
      
      return false;
    }
}
