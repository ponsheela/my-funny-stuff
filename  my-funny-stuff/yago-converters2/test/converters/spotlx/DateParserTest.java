package converters.spotlx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.junit.Test;

public class DateParserTest {
  
  @Test
  public void floorTest() {
    long milis = DateParser.floorDate("1900-##-##");
    assertEquals(-2208988800000l, milis);
  
    milis = DateParser.floorDate("1900-01-##");
    assertEquals(-2208988800000l, milis);
    
    milis = DateParser.floorDate("9##-##-##");
    assertEquals(-33765552000000l, milis);
  }
  
  @Test
  public void ceilTest() {
    long milis = DateParser.ceilDate("1900-##-##");
    assertEquals(-2177452800001l, milis);
    
    milis = DateParser.ceilDate("1900-01-##");
    assertEquals(-2206310400001l, milis);
    
    milis = DateParser.ceilDate("1900-01-01");
    assertEquals(-2208902400001l, milis);
    
    milis = DateParser.ceilDate("9##-##-##");
    assertEquals(-30609792000001l, milis);
  } 
  
  @Test
  public void julianLeapDayTest() {
    long milis = DateParser.ceilDate("1400-02-##");
    Calendar c = GregorianCalendar.getInstance(TimeZone.getTimeZone("UTC"));
    c.setTimeInMillis(milis);
//    c.add(Calendar.DAY_OF_MONTH, -1);
    assertFalse(DateParser.isJulianLeapDay(c));    
    
    milis = DateParser.ceilDate("1400-02-28");
    c.clear();
    c.setTimeInMillis(milis);
    assertFalse(DateParser.isJulianLeapDay(c));
    
    milis = DateParser.ceilDate("1400-02-29");
    c.clear();
    c.setTimeInMillis(milis);
    assertFalse(DateParser.isJulianLeapDay(c));
  }
}
