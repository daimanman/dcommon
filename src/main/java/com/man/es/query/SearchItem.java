package com.man.es.query;

/**
 * 查询项
 */
public class SearchItem {
	private String logic = Logic.AND.name(); // 逻辑操作符
	private boolean left = false; // 是否有左括号
	private String field; // 字段名
	private String compare = Compare.CON.name(); // 比较操作符
	private Object data; // 字段值
	private boolean right = false; // 是否有右括号
	private String fieldtype = FieldType.STRING.name(); // 字段类型

	public SearchItem() {

	}

	public SearchItem(String field, String compare, String data) {
		this.field = field;
		this.compare = compare;
		this.data = data;
	}

	public SearchItem(String logic, String field, String compare, String data, String fieldtype) {
		this.logic = logic;
		this.field = field;
		this.compare = compare;
		this.data = data;
		this.fieldtype = fieldtype;
	}

	public SearchItem(String logic, boolean left, String field, String compare, String data, boolean right,
			String fieldtype) {
		this.logic = logic;
		this.left = left;
		this.field = field;
		this.compare = compare;
		this.data = data;
		this.right = right;
		this.fieldtype = fieldtype;
	}

	public String getLogic() {
		return logic;
	}

	public void setLogic(String logic) {
		this.logic = logic;
	}

	public boolean isLeft() {
		return left;
	}

	public void setLeft(boolean left) {
		this.left = left;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getCompare() {
		return compare;
	}

	public void setCompare(String compare) {
		this.compare = compare;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public boolean isRight() {
		return right;
	}

	public void setRight(boolean right) {
		this.right = right;
	}

	public String getFieldtype() {
		return fieldtype;
	}

	public void setFieldtype(String fieldtype) {
		this.fieldtype = fieldtype;
	}
}
