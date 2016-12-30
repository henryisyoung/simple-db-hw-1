package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based
 * field.
 */
public class IntHistogram {

	private final int min;
	private final int max;
	private final int buckets;
	private final int widthAverage;
	private final int[] buckets_Counts;
	private final int[] buckets_Mins;
	private final int[] buckets_Maxs;
	private int totalvalue;
	// private final int numvalues;

	/**
	 * Create a new IntHistogram.
	 * 
	 * This IntHistogram should maintain a histogram of integer values that it
	 * receives. It should split the histogram into "buckets" buckets.
	 * 
	 * The values that are being histogrammed will be provided one-at-a-time
	 * through the "addValue()" function.
	 * 
	 * Your implementation should use space and have execution time that are
	 * both constant with respect to the number of values being histogrammed.
	 * For example, you shouldn't simply store every value that you see in a
	 * sorted list.
	 * 
	 * @param buckets
	 *            The number of buckets to split the input value into.
	 * @param min
	 *            The minimum integer value that will ever be passed to this
	 *            class for histogramming
	 * @param max
	 *            The maximum integer value that will ever be passed to this
	 *            class for histogramming
	 */
	public IntHistogram(int buckets, int min, int max) {
		// some code goes here
		this.buckets = buckets;
		this.min = min;
		this.max = max;
		// this.widthAverage = (this.max - this.min) / buckets;
		this.totalvalue = 0;
		this.widthAverage = (this.max - this.min) / buckets + 1;
		this.buckets_Maxs = new int[buckets];
		this.buckets_Mins = new int[buckets];
		this.buckets_Counts = new int[buckets];
		for (int i = 0; i < buckets; i++) {
			buckets_Mins[i] = min + i * widthAverage;
			buckets_Maxs[i] = buckets_Mins[i] + widthAverage - 1;
		}
		buckets_Maxs[buckets - 1] = max;

	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 * 
	 * @param v
	 *            Value to add to the histogram
	 */
	public void addValue(int v) {
		// some code goes here
		if (v >= min && v <= max) {
			int pos = (v - min) / widthAverage;
			buckets_Counts[pos]++;
			this.totalvalue++;
		} else
			return;
	}

	/**
	 * Estimate the selectivity of a particular predicate and operand on this
	 * table.
	 * 
	 * For example, if "op" is "GREATER_THAN" and "v" is 5, return your estimate
	 * of the fraction of elements that are greater than 5.
	 * 
	 * @param op
	 *            Operator
	 * @param v
	 *            Value
	 * @return Predicted selectivity of this particular operator and value
	 */
	public double estimateSelectivity(Predicate.Op op, int v) {
		// out of bound
		if (v < min) {
			switch (op) {
			case GREATER_THAN:
			case GREATER_THAN_OR_EQ:
			case NOT_EQUALS:
				return 1.0;

			default:
				return 0.0;
			}
		}
		if (v > max) {
			switch (op) {
			case LESS_THAN:
			case LESS_THAN_OR_EQ:
			case NOT_EQUALS:
				// System.out.println("retufrn");
				return 1.0;
			default:
				return 0.0;
			}
		}
		// within the min and max
		int pos = (v - min) / widthAverage;
		double returnval = 0.0;
		// The width of the index position
		int widthPos = 0;
		widthPos =(buckets_Maxs[pos]) - (buckets_Mins[pos]) + 1;

		// set the comparison value
		switch (op) {
		case LESS_THAN_OR_EQ:
		case EQUALS:
		case GREATER_THAN_OR_EQ:
			returnval = (double)this.buckets_Counts[pos] / widthPos;

			break;
		// 1-(position of equality to the value)
		case NOT_EQUALS:
			return 1.0 -(double) this.buckets_Counts[pos] / widthPos / this.totalvalue;

		default:
			break;
		}
		// set the final return value
		switch (op) {
		case LESS_THAN:
		case LESS_THAN_OR_EQ:
			for (int i = 0; i < pos; i++) {
				returnval = returnval + this.buckets_Counts[i];
			}
			returnval += (double) (v - this.buckets_Mins[pos]) / widthPos * this.buckets_Counts[pos];
			break;
		case GREATER_THAN_OR_EQ:
		case GREATER_THAN:
			returnval += (double) (this.buckets_Maxs[pos] - v) / widthPos * this.buckets_Counts[pos];
			for (int i = pos + 1; i < this.buckets; i++) {
				returnval += this.buckets_Counts[i];
			}
			break;
		default:
			break;
		}

		// some code goes here
//		System.out.println(returnval / totalvalue);
		return returnval / totalvalue;
	}

	/**
	 * @return the average selectivity of this histogram.
	 * 
	 *         This is not an indispensable method to implement the basic join
	 *         optimization. It may be needed if you want to implement a more
	 *         efficient optimization
	 */
	public double avgSelectivity() {
		// some code goes here

		return 1.0;
	}

	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {
		// some code goes here
		return null;
	}
}
