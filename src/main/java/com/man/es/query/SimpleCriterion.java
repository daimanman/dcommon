package com.man.es.query;

/**
 * 简单查询条件
 */
public class SimpleCriterion extends Criterion {
    private static final long serialVersionUID = 6548020423596359929L;
    private String name; // 属性名
    private Object value; // 属性值
    private Compare compare; // 比较操作符

    public SimpleCriterion() {
    }

    public SimpleCriterion(String name, Object value, Compare compare) {
        this.name = name;
        this.value = value;
        this.compare = compare;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Compare getCompare() {
        return compare;
    }

    public void setCompare(Compare compare) {
        this.compare = compare;
    }
}
