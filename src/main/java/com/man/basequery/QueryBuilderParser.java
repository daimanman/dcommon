package com.man.basequery;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;

import com.man.constants.IdxConstant;

/**
 * 解析
 * 
 * @author daixm
 *
 */
public class QueryBuilderParser implements java.io.Serializable {

	private static final long serialVersionUID = 3508279471042756280L;

	/**
	 * 单个字段的简单解析查询
	 * @param queryItem
	 * @return
	 */
	public QueryBuilder parseQueryBuilder(QueryItem queryItem) {
		if (queryItem == null) {
			return null;
		}

		String field = queryItem.field;
		Object value = queryItem.value;
		String matchType = queryItem.matchType;

		if (!queryItem.isNeedSearch()) {
			return null;
		}

		/**
		 * ES IN -> Terms 查询
		 */
		if (QueryTypeEnum.IN.isValid(matchType)) {

			if (value instanceof Collection<?>) {
				value = (Collection<?>) value;
				if (((Collection<?>) value).isEmpty()) {
					throw new RuntimeException("IN Collection is empty");
				}
				return new TermsQueryBuilder(field, (Collection<?>) value);
			}
			return new TermQueryBuilder(field, value);
		}
		// LIKE -> match查询
		if (QueryTypeEnum.LIKE.isValid(matchType)) {
			return new MatchQueryBuilder(field, value);
		}

		// GT
		if (QueryTypeEnum.GT.isValid(matchType)) {
			return new RangeQueryBuilder(field).gt(value);
		}
		// GT
		if (QueryTypeEnum.GTE.isValid(matchType)) {
			return new RangeQueryBuilder(field).gte(value);
		}
		// LT
		if (QueryTypeEnum.LT.isValid(matchType)) {
			return new RangeQueryBuilder(field).lt(value);
		}
		// LTE
		if (QueryTypeEnum.LTE.isValid(matchType)) {
			return new RangeQueryBuilder(field).lte(value);
		}

		// NOTIN
		if (QueryTypeEnum.NOTIN.isValid(matchType)) {
			if (value instanceof Collection<?>) {
				value = (Collection<?>) value;
				if (((Collection<?>) value).isEmpty()) {
					throw new RuntimeException("IN Collection is empty");
				}
				return new BoolQueryBuilder().mustNot(new TermsQueryBuilder(field, (Collection<?>) value));
			}
			return new BoolQueryBuilder().mustNot(new TermQueryBuilder(field, value));
		}

		// BETWEEN
		if (QueryTypeEnum.BETWEEN.isValid(matchType)) {
				RangeQueryBuilder builder = null;
				if(value instanceof Object[] && ((Object[])value).length >= 1){
					Object[] objs = (Object[])value; 
					builder = new RangeQueryBuilder(field).from(objs[0],true);
					if(objs.length >= 2){
						builder.to(objs[1],true);
					}
					return builder;
				}
				
				if(value instanceof Collection<?> && ((Collection<?>)value).size() >=1){
					Collection<?> cols = (Collection<?>)value;
					Iterator<?> iter = cols.iterator();
					builder = new RangeQueryBuilder(field).from(iter.next(),true);
					if(cols.size() >= 2){
						builder.to(iter.next(),true);
					}
					return builder;
				}
				
				return builder;
		}
		
		// 属性存在
		if(QueryTypeEnum.HASP.isValid(matchType)){
			return new ExistsQueryBuilder(field);
		}
		
		// 属性不存在
		if(QueryTypeEnum.NHASP.isValid(matchType)){
			return new BoolQueryBuilder().mustNot(new ExistsQueryBuilder(field));
		}
		return null;
	}
	
	private QueryBuilder parseMultiField(QueryItem item,String[] fields){
		if(null != item && fields != null && fields.length > 0){
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
			for(String field:fields){
				item.field = field.trim();
				if(item.andOr == IdxConstant.OR){
					boolQuery.should(parseQueryBuilder(item));
				}else{
					boolQuery.must(parseQueryBuilder(item));
				}
			}
			return boolQuery;
		}
		return null;
	}
	/**
	 * @param items
	 * @return
	 */
	public BoolQueryBuilder parseQueryItems(List<QueryItem> items){
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		if(null != items && items.size() > 0){
			for(QueryItem item:items){
				String fields = item.field;
				if(null == fields || "".equals(fields.trim())){
					continue;
				}
				//拆分
				String[] fieldsArr = fields.split("\\W");
				if(fieldsArr != null && fieldsArr.length > 1){
					//符合匹配
					boolQueryBuilder.filter(parseMultiField(item,fieldsArr));
				}else{
					//简单匹配单个字段
					boolQueryBuilder.filter(parseQueryBuilder(item));
				}
				
			}
		}
		return boolQueryBuilder;
	}
}
