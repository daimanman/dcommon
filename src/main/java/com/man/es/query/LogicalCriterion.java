package com.man.es.query;

/**
 * 逻辑查询条件
 */
public class LogicalCriterion extends Criterion {
    private static final long serialVersionUID = 6544969994193923844L;
    private Criterion left; // 左查询条件
    private Criterion right; // 右查询条件
    private Logic logic; // 逻辑运算符

    public LogicalCriterion() {
    }

    public LogicalCriterion(Criterion left, Criterion right, Logic logic) {
        this.left = left;
        this.right = right;
        this.logic = logic;
    }

    public Criterion getLeft() {
        return left;
    }

    public void setLeft(Criterion left) {
        this.left = left;
    }

    public Criterion getRight() {
        return right;
    }

    public void setRight(Criterion right) {
        this.right = right;
    }

    public Logic getLogic() {
        return logic;
    }

    public void setLogic(Logic logic) {
        this.logic = logic;
    }
}
