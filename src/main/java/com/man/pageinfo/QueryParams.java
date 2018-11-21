package com.man.pageinfo;

import java.util.List;

import com.man.basequery.QueryItem;

/**s
 * 查询条件
 * 
 * @author daixm
 *
 */
public class QueryParams implements java.io.Serializable {

	private static final long serialVersionUID = 9060187408054731245L;

	public static int DEFAULT_PAGE_SIZE = 20;

	/**
	 * 当前页
	 */
	private int page;

	/**
	 * 分页大小
	 */
	private int pageSize;

	/**
	 * 排序参数条件
	 */
	private List<SortParams> sorts;

	/**
	 * 查询条件
	 */
	private List<QueryItem> queryItems;
	
	

	public List<QueryItem> getQueryItems() {
		return queryItems;
	}

	public void setQueryItems(List<QueryItem> queryItems) {
		this.queryItems = queryItems;
	}

	public List<SortParams> getSorts() {
		return sorts;
	}

	public void setSorts(List<SortParams> sorts) {
		this.sorts = sorts;
	}

	public int getPage() {
		return page;
	}

	public void setPage(int page) {
		this.page = page;
	}

	public int getPageSize() {
		return pageSize > 0 ? pageSize : DEFAULT_PAGE_SIZE;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getOffset() {
		return ((getPage() >= 1 ? getPage() : 1) - 1) * (getPageSize() > 0 ? getPageSize() : DEFAULT_PAGE_SIZE);
	}

}
