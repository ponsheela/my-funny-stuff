/**
 * 
 */
package converters.spotlx;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * @author susr
 *
 */
public class NewTimeIntervalBinding extends TupleBinding<NewTimeInterval> {

	@Override
	public NewTimeInterval entryToObject(TupleInput arg0) {
		String begin = arg0.readString();
		String end = arg0.readString();
		return new NewTimeInterval(begin, end);
	}

	@Override
	public void objectToEntry(NewTimeInterval arg0, TupleOutput arg1) {
		arg1.writeString(arg0.getBegin());
		arg1.writeString(arg0.getEnd());
	}

}
