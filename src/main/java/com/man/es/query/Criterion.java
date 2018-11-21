package com.man.es.query;

import java.util.List;
import java.util.Map;

import org.elasticsearch.common.util.CollectionUtils;

public abstract class Criterion implements java.io.Serializable {
	private static final long serialVersionUID = -2885079651365545073L;

	public Criterion and(Criterion criterion) {
		return criterion == null ? this : new LogicalCriterion(this, criterion, Logic.AND);
	}

	public Criterion or(Criterion criterion) {
		return criterion == null ? this : new LogicalCriterion(this, criterion, Logic.OR);
	}

	/**
	 * 从参数map创建查询条件
	 *
	 * @param paramMap
	 *            参数map
	 * @return 查询条件
	 */
	public static Criterion createFrom(Map<String, Object> paramMap) {
		if (null == paramMap || paramMap.keySet().size() == 0 ) {
			return new EmptyCriterion();
		}
		return paramMap.entrySet().stream()
				.map(param -> (Criterion) new SimpleCriterion(param.getKey(), param.getValue(), Compare.CON))
				.reduce(Criterion::and).orElse(new EmptyCriterion());
	}

	/**
	 * 从简单查询条件集合创建查询条件
	 *
	 * @param simpleCriteria
	 *            简单查询条件集合
	 * @return 查询条件
	 */
	public static Criterion createFrom(List<SimpleCriterion> simpleCriteria) {
		if (simpleCriteria == null || simpleCriteria.size() == 0) {
			return new EmptyCriterion();
		}
		return simpleCriteria.stream().map(item -> (Criterion) item).reduce(Criterion::and)
				.orElse(new EmptyCriterion());
	}

	/**
	 * 从查询项集合集合创建查询条件
	 *
	 * @param searchItems
	 *            查询项集合
	 * @return 查询条件
	 */
	public static Criterion createFromSearchItems(List<SearchItem> searchItems) {
		if (searchItems == null || searchItems.size() == 0) {
			return new EmptyCriterion();
		}

		Criterion criterion = createFromSearchItem(searchItems.get(0));

		for (int i = 1; i < searchItems.size(); i++) {
			SearchItem searchItem = searchItems.get(i);
			if (searchItem.isLeft() && !searchItem.isRight()) {
				int rightBracketIndex = getRightBracket(searchItems.subList(i + 1, searchItems.size() - 1));
				List<SearchItem> subSearchItems = searchItems.subList(i + 1, rightBracketIndex + 1);
				String logicStr = searchItem.getLogic().toUpperCase();
				criterion = logicStr.equals(Logic.AND.name()) ? criterion.and(createFromSearchItems(subSearchItems))
						: criterion.or(createFromSearchItems(subSearchItems));
			} else {
				SimpleCriterion current = createFromSearchItem(searchItem);
				String logicStr = searchItem.getLogic().toUpperCase();
				criterion = logicStr.equals(Logic.AND.name()) ? criterion.and(current) : criterion.or(current);
			}
		}
		return criterion;
	}

	/**
	 * 根据searchItem构造简单查询条件
	 *
	 * @param searchItem
	 *            查询项
	 * @return 简单查询条件
	 */
	private static SimpleCriterion createFromSearchItem(SearchItem searchItem) {
		return new SimpleCriterion(searchItem.getField(), searchItem.getData(),
				Compare.valueOf(searchItem.getCompare().toUpperCase()));
	}

	/**
	 * 计算右括号索引
	 *
	 * @param searchItems
	 *            查询项子集
	 * @return 右括号索引
	 */
	private static int getRightBracket(List<SearchItem> searchItems) {
		int leftBrackets = 0;
		for (int i = 0; i < searchItems.size(); i++) {
			SearchItem searchItem = searchItems.get(i);
			if (searchItem.isLeft() && searchItem.isRight()) {
				continue;
			}

			if (searchItem.isLeft()) {
				leftBrackets++;
			} else if (searchItem.isRight()) {
				leftBrackets--;
				if (leftBrackets == -1) {
					return i;
				}
			}
		}

		throw new RuntimeException("查询项括号不匹配");
	}
}
