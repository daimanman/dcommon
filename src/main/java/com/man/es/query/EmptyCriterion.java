package com.man.es.query;

/**
 * 空查询条件
 */
public class EmptyCriterion extends Criterion {
    private static final long serialVersionUID = -7460163233463413438L;

    @Override
    public Criterion and(Criterion criterion) {
        return criterion;
    }

    @Override
    public Criterion or(Criterion criterion) {
        return criterion;
    }
}
