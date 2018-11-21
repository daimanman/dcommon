package com.man.es.query;

import java.util.Collection;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.PrefixQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;

/**
 * 查询条件elasticsearch解析器
 */
public class CriterionElasticsearchParser extends CriterionParser<QueryBuilder> {

    /**
     * 解析为特定领域查询构造类
     *
     * @param criterion 简单查询条件
     * @return 查询构造类
     */
    @Override
    protected QueryBuilder toQueryBuilder(SimpleCriterion criterion) {
        String name = criterion.getName();
        Object value = criterion.getValue();
        Compare compare = criterion.getCompare();

        switch (compare) {
            case CON:
                return new TermQueryBuilder(name, value);
            case NCON:
                return new BoolQueryBuilder().mustNot(new TermQueryBuilder(name, value));
            case MCON:
                if (value == null || !(value instanceof Collection) || ((Collection) value).isEmpty()) {
                    throw new RuntimeException("属性值不满足multi contain查询格式要求");
                }
                return new TermsQueryBuilder(name, (Collection<?>) value);
            case NMCON:
                if (value == null || !(value instanceof Collection) || ((Collection) value).isEmpty()) {
                    throw new RuntimeException("属性值不满足not multi contain查询格式要求");
                }
                return new BoolQueryBuilder().mustNot(new TermsQueryBuilder(name, (Collection<?>) value));
            case GT:
                return new RangeQueryBuilder(name).gt(value);
            case LT:
                return new RangeQueryBuilder(name).lt(value);
            case GE:
                return new RangeQueryBuilder(name).gte(value);
            case LE:
                return new RangeQueryBuilder(name).lte(value);
            case BETWEEN:
                if (value == null || !(value instanceof Object[]) || ((Object[]) value).length != 2) {
                    throw new RuntimeException("属性值不满足between查询格式要求");
                }
                Object[] betweenValues = (Object[]) value;
                return new RangeQueryBuilder(name).from(betweenValues[0]).to(betweenValues[1]);
            case EX:
                return new ExistsQueryBuilder(name);
            case NEX:
                return new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(name));
            case PRE:
                return new PrefixQueryBuilder(name, (String) value);

            case MATCH:
                return new MatchQueryBuilder(name, value);
            default:
                throw new RuntimeException(String.format("比较操作符格式不正确：%s", compare.name()));
        }
    }

    /**
     * 解析为特定领域查询构造类
     *
     * @param criterion 逻辑查询条件
     * @return 查询构造类
     */
    @Override
    protected QueryBuilder toQueryBuilder(LogicalCriterion criterion) {
        Criterion left = criterion.getLeft();
        Criterion right = criterion.getRight();
        Logic logic = criterion.getLogic();

        QueryBuilder leftQueryBuilder = this.toQueryBuilder(left);
        QueryBuilder rightQueryBuilder = this.toQueryBuilder(right);

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        switch (logic) {
            case AND:
                if (leftQueryBuilder != null) {
                    boolQueryBuilder.must(leftQueryBuilder);
                }
                if (rightQueryBuilder != null) {
                    boolQueryBuilder.must(rightQueryBuilder);
                }
                break;
            case OR:
                if (leftQueryBuilder != null) {
                    boolQueryBuilder.should(leftQueryBuilder);
                }
                if (rightQueryBuilder != null) {
                    boolQueryBuilder.should(rightQueryBuilder);
                }
                break;
            default:
                throw new RuntimeException(String.format("逻辑运算符格式不正确：%s", logic.name()));
        }
       
        return boolQueryBuilder;
    }
}
