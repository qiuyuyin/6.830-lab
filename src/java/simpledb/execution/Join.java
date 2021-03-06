package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private final JoinPredicate joinPredicate;

    private OpIterator child1;

    private OpIterator child2;

    private final TupleDesc tupleDesc;

    private Iterator<Tuple> it;

    private final List<Tuple> childTups = new ArrayList<>();

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;

        TupleDesc tupleDesc1 = child1.getTupleDesc();
        TupleDesc tupleDesc2 = child2.getTupleDesc();
        // get the TupleDesc
        this.tupleDesc = TupleDesc.merge(tupleDesc1,tupleDesc2);


    }

    public JoinPredicate getJoinPredicate() {
        return this.joinPredicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return child1.getTupleDesc().toString();
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().toString();
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        int size1 = child1.getTupleDesc().numFields();
        int size2 = child2.getTupleDesc().numFields();
        this.child1.open();
        this.child2.open();
        while (this.child1.hasNext()) {
            Tuple child1 = this.child1.next();
            this.child2.rewind();
            while (this.child2.hasNext()) {
                Tuple child2 = this.child2.next();
                if(this.joinPredicate.filter(child1,child2)) {
                    Tuple tuple = new Tuple(this.tupleDesc);
                    for (int i = 0; i < size1; i++) {
                        tuple.setField(i,child1.getField(i));
                    }
                    for (int i = size1; i < size1 + size2; i++) {
                        tuple.setField(i,child2.getField(i-size1));
                    }
                    this.childTups.add(tuple);
                }
            }
        }
        it = this.childTups.iterator();
        super.open();

    }

    public void close() {
        super.close();
        it = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        it = childTups.iterator();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (it != null && it.hasNext()) {
            return it.next();
        } else
            return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child1,this.child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
