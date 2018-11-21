package com.man.es.query;

/**
 * 排序规则
 */
public class OrderRule implements java.io.Serializable {
    private static final long serialVersionUID = -6783823185853911077L;
    private String property; //属性
    private Direction direction; //排序方向

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public Direction getDirection() {
        return direction;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public enum Direction {
        ASC, DESC
    }
}
