package com.man.es.query;

public enum Compare {
	/*---------- 结构化搜索 ----------*/
	CON, // 包含
	NCON, // 不包含
	MCON, // 包含多个之一
	NMCON, // 其中一个也不包含
	GT, // 大于
	LT, // 小于
	GE, // 大于等于
	LE, // 小于等于
	BETWEEN, // 区间
	EX, // 存在属性
	NEX, // 不存在属性
	PRE, // 前缀

	/*---------- 全文搜索 ----------*/
	MATCH, // 匹配查询
}