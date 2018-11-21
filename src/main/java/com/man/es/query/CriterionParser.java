package com.man.es.query;

/**
 * 查询条件解析器接口
 */
public abstract class CriterionParser<T> {

    /**
     * 解析为特定领域查询构造类
     *
     * @return 查询构造类
     */
    final public T toQueryBuilder(Criterion criterion) {
        if (criterion == null) {
            return null;
        }

        if (criterion instanceof EmptyCriterion) {
            return null;
        } else if (criterion instanceof SimpleCriterion) {
            return this.toQueryBuilder((SimpleCriterion) criterion);
        } else if (criterion instanceof LogicalCriterion) {
            return this.toQueryBuilder((LogicalCriterion) criterion);
        }

        throw new RuntimeException(String.format("不支持解析该查询条件：%s", criterion.getClass().getName()));
    }

    /**
     * 解析为特定领域查询构造类
     *
     * @param criterion 简单查询条件
     * @return 查询构造类
     */
    protected abstract T toQueryBuilder(SimpleCriterion criterion);

    /**
     * 解析为特定领域查询构造类
     *
     * @param criterion 逻辑查询条件
     * @return 查询构造类
     */
    protected abstract T toQueryBuilder(LogicalCriterion criterion);
}
