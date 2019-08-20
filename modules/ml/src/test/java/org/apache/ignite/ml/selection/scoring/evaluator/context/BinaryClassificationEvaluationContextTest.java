package org.apache.ignite.ml.selection.scoring.evaluator.context;

import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link BinaryClassificationEvaluationContext} class.
 */
public class BinaryClassificationEvaluationContextTest {
    /** */
    @Test
    public void testAggregate() {
        BinaryClassificationEvaluationContext ctx = new BinaryClassificationEvaluationContext();
        ctx.aggregate(VectorUtils.of().labeled(1.0));
        assertEquals(ctx.getFirstClassLbl(), 1., 0.);
        assertEquals(ctx.getSecondClassLbl(), Double.NaN, 0.);

        ctx.aggregate(VectorUtils.of().labeled(0.0));
        assertEquals(ctx.getFirstClassLbl(), 0., 0.);
        assertEquals(ctx.getSecondClassLbl(), 1., 0.);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testAggregateWithThreeLabels() {
        BinaryClassificationEvaluationContext ctx = new BinaryClassificationEvaluationContext();
        ctx.aggregate(VectorUtils.of().labeled(-1.0));
        ctx.aggregate(VectorUtils.of().labeled(1.0));

        assertEquals(ctx.getFirstClassLbl(), -1., 0.);
        assertEquals(ctx.getSecondClassLbl(), 1., 0.);

        ctx.aggregate(VectorUtils.of().labeled(0.0));
    }

    /** */
    @Test
    public void testMerge1() {
        BinaryClassificationEvaluationContext left = new BinaryClassificationEvaluationContext();
        BinaryClassificationEvaluationContext right = new BinaryClassificationEvaluationContext();

        BinaryClassificationEvaluationContext res = left.mergeWith(right);
        assertEquals(res.getFirstClassLbl(), Double.NaN, 0.);
        assertEquals(res.getSecondClassLbl(), Double.NaN, 0.);
    }

    /** */
    @Test
    public void testMerge2() {
        BinaryClassificationEvaluationContext left = new BinaryClassificationEvaluationContext(0., Double.NaN);
        BinaryClassificationEvaluationContext right = new BinaryClassificationEvaluationContext();

        BinaryClassificationEvaluationContext res = left.mergeWith(right);
        assertEquals(res.getFirstClassLbl(), 0., 0.);
        assertEquals(res.getSecondClassLbl(), Double.NaN, 0.);

        res = right.mergeWith(left);
        assertEquals(res.getFirstClassLbl(), 0., 0.);
        assertEquals(res.getSecondClassLbl(), Double.NaN, 0.);
    }

    /** */
    @Test
    public void testMerge3() {
        BinaryClassificationEvaluationContext left = new BinaryClassificationEvaluationContext(Double.NaN, 0.);
        BinaryClassificationEvaluationContext right = new BinaryClassificationEvaluationContext();

        BinaryClassificationEvaluationContext res = left.mergeWith(right);
        assertEquals(res.getFirstClassLbl(), 0., 0.);
        assertEquals(res.getSecondClassLbl(), Double.NaN, 0.);

        res = right.mergeWith(left);
        assertEquals(res.getFirstClassLbl(), 0., 0.);
        assertEquals(res.getSecondClassLbl(), Double.NaN, 0.);
    }

    /** */
    @Test
    public void testMerge4() {
        BinaryClassificationEvaluationContext left = new BinaryClassificationEvaluationContext(1., 0.);
        BinaryClassificationEvaluationContext right = new BinaryClassificationEvaluationContext();

        BinaryClassificationEvaluationContext res = left.mergeWith(right);
        assertEquals(res.getFirstClassLbl(), 0., 0.);
        assertEquals(res.getSecondClassLbl(), 1., 0.);

        res = right.mergeWith(left);
        assertEquals(res.getFirstClassLbl(), 0., 0.);
        assertEquals(res.getSecondClassLbl(), 1., 0.);
    }

    /** */
    @Test
    public void testMerge5() {
        BinaryClassificationEvaluationContext left = new BinaryClassificationEvaluationContext(1., 0.);
        BinaryClassificationEvaluationContext right = new BinaryClassificationEvaluationContext(0., 1.);

        BinaryClassificationEvaluationContext res = left.mergeWith(right);
        assertEquals(res.getFirstClassLbl(), 0., 0.);
        assertEquals(res.getSecondClassLbl(), 1., 0.);

        res = right.mergeWith(left);
        assertEquals(res.getFirstClassLbl(), 0., 0.);
        assertEquals(res.getSecondClassLbl(), 1., 0.);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testMerge6() {
        BinaryClassificationEvaluationContext left = new BinaryClassificationEvaluationContext(1., 0.);
        BinaryClassificationEvaluationContext right = new BinaryClassificationEvaluationContext(2., 1.);
        BinaryClassificationEvaluationContext res = left.mergeWith(right);
    }
}
