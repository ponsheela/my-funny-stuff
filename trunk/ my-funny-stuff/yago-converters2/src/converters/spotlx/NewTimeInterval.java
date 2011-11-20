/**
 * 
 */
package converters.spotlx;

/**
 * @author susr
 *
 */
public class NewTimeInterval {

	private String timeBegin;
	
	private String timeEnd;

	public NewTimeInterval(String timeBegin, String timeEnd) {
		super();
		this.timeBegin = timeBegin;
		this.timeEnd = timeEnd;
	}

	public NewTimeInterval() {
		super();
	}

	public String getBegin() {
		return timeBegin;
	}

	public void setBegin(String timeBegin) {
		this.timeBegin = timeBegin;
	}

	public String getEnd() {
		return timeEnd;
	}

	public void setEnd(String timeEnd) {
		this.timeEnd = timeEnd;
	}
	
	
}
