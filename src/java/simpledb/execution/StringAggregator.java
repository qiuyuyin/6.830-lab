package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final Op op;

    private final int gbfield;

    private int afield;

    private final Type type;

    private Map<Field, Integer> groupMap = new HashMap<>();

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("op is not count!");
        }
        this.gbfield = gbfield;
        this.afield = afield;
        this.op = what;
        this.type = gbfieldtype;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField gbfield = (IntField) tup.getField(this.gbfield);
        Integer orDefault = groupMap.get(gbfield);
        if (orDefault == null) {
            groupMap.put(gbfield,1);
        } else {
            groupMap.put(gbfield,orDefault + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        TupleDesc tupleDesc = new TupleDesc(new Type[]{this.type, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Field field : groupMap.keySet()) {
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0,field);
            tuple.setField(1,new IntField(groupMap.get(field)));
            tuples.add(tuple);
        }
        return new TupleIterator(tupleDesc,tuples);
    }

}
