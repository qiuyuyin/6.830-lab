package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final Op op;

    private int gbfield;

    private int afield;

    private Type type;

    private Map<Field, List<Integer>> listMap = new HashMap<>();

    private Map<Field, Integer> groupMap = new HashMap<>();

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.afield = afield;
        this.op = what;
        this.type = gbfieldtype;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbfield = tup.getField(this.gbfield);
        IntField afield = (IntField) tup.getField(this.afield);
        if (this.op != Op.AVG) {
            Integer mapGet = groupMap.get(gbfield);
            if (mapGet != null) {
                switch (this.op) {
                    case MIN -> groupMap.put(gbfield, Math.min(afield.getValue(), mapGet));
                    case MAX -> groupMap.put(gbfield, Math.max(afield.getValue(), mapGet));
                    case SUM -> groupMap.put(gbfield,  mapGet + afield.getValue());
                    case COUNT -> groupMap.put(gbfield,  mapGet + 1);
                }
            } else {
                if (this.op == Op.COUNT) {
                    groupMap.put(gbfield,1);
                } else {
                    groupMap.put(gbfield,afield.getValue());
                }
            }
        } else {
            List<Integer> list = listMap.get(gbfield);
            if(list != null) {
                list.add(afield.getValue());
                int sum = 0;
                for (Integer integer : list) {
                    sum += integer;
                }
                groupMap.put(gbfield,sum/list.size());
                listMap.put(gbfield,list);
            } else {
                groupMap.put(gbfield,afield.getValue());
                ArrayList<Integer> newList = new ArrayList<>(afield.getValue());
                newList.add(afield.getValue());
                listMap.put(gbfield,newList);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        TupleDesc tupleDesc = new TupleDesc(new Type[]{this.type, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        List<Tuple> tuples = new ArrayList<>();
        for (Field field : groupMap.keySet()) {
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0,field);
            tuple.setField(1,new IntField(groupMap.get(field)));
            tuples.add(tuple);
        }
        return new TupleIterator(tupleDesc,tuples);
    }

}
