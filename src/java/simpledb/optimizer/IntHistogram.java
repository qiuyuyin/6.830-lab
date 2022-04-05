package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int min;

    private int max;

    private int buckets;

    private double width;

    private int[] bucket;

    private int ntup;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.min = min;
        this.max = max;
        if (buckets > (max - min)) {
            buckets = (max - min + 1);
        }
        this.buckets = buckets;
        this.width = (double) (max - min + 1) / buckets ;
        this.bucket = new int[buckets];
        this.ntup = 0;
    }

    private int getIndex(int v) {
        return (int) ((v - min) / width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int index = getIndex(v);
        if (index >= 0 && index < this.buckets) {
            this.bucket[index]++;
            this.ntup++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch (op) {
            case EQUALS -> {
                int index = getIndex(v);

                if (index < 0 || index >= this.buckets) {
                    return 0.0;
                }
                return (bucket[index] / width) / ntup;
            }
            case LESS_THAN -> {
                int index = getIndex(v);
                if (index < 0) {
                    return 0.0;
                }
                if (index >= this.buckets) {
                    return 1.0;
                }
                double sum = 0.0;
                for (int i = 0; i < index; i++) {
                    sum += bucket[i] / width / ntup;
                }
                sum += (v - index * width - min) * bucket[index] / width / ntup;
                return sum;
            }
            case LESS_THAN_OR_EQ -> {
                return this.estimateSelectivity(Predicate.Op.LESS_THAN,v + 1);
            }
            case GREATER_THAN_OR_EQ -> {
                return 1 - this.estimateSelectivity(Predicate.Op.LESS_THAN,v);
            }
            case GREATER_THAN -> {
                return this.estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ,v+1);
            }
            case NOT_EQUALS -> {
                return 1 - this.estimateSelectivity(Predicate.Op.EQUALS,v);
            }
        }
    	// some code goes here
        return 0.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        double avg = 0.0;
        for (int i = 0; i < buckets; i++) {
            avg += (this.bucket[i] + 0.0) / ntup;
        }
        return avg;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bucket.length; i++) {
            double b_l = i * width;
            double b_r = (i+1) * width;
            sb.append(String.format("[%f, %f]:%d\n", b_l, b_r, bucket[i]));
        }
        return sb.toString();
    }
}
