package com.man.pageinfo;

public class SortParams implements java.io.Serializable {

	private static final long serialVersionUID = 7668546334210962611L;

	/**
	 * 排序字段
	 */
	private String field;

	/**
	 * 排序顺序
	 */
	private String sort;

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getSort() {
		return sort;
	}

	public void setSort(String sort) {
		this.sort = sort;
	}

	public SortParams(String field, String sort) {
		this.field = field;
		this.sort = sort;
	}
	public SortParams() {
		
	}
	

}
