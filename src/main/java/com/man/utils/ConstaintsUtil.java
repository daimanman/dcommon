package com.man.utils;

public class ConstaintsUtil {

	public static  String FLAG_MQ = "1";
	/**
	 * 标记字段所属接口累心 1 基本信息 0 其他信息
	 */
	public static int INTERFACE_TYPE_BASE = 1;
	public static int INTERFACE_TYPE_OTHER = 2;
	
	
	/**
	 *标记产品是否参与更新 1 参与更新 0 不参与更新
	 */
	public static int INTERFACE_STATUS_OK = 1;
	public static int INTERFACE_STATUS_NO = 0;
	
	public static String TASK_STATUS_START="1";
	public static String TASK_STATUS_OK="2";
	
	public static String RET_OK = "1";
	public static String RET_ERR = "0";
	
	/**
	 * 字典key 查询产品的接口 url
	 */
	public static String QUNAR_QUERY_PRODUCT = "QUNAR_QUERY_PRODUCT";
	/**
	 * 获取Cookie
	 */
	public static String QUNAR_QUERY_COOKIE = "QUNAR_QUERY_COOKIE";
	
	/**
	 * 更新基本信息
	 */
	public static String QUNAR_UPDATE_BASE = "QUNAR_UPDATE_BASE";
	
	/**
	 * 获取基本信息HTML
	 */
	public static String QUNAR_QUERY_BASE = "QUNAR_QUERY_BASE";
	
	public static String QUNAR_USER_AGENT = "QUNAR_USER_AGENT";
	
	public static String QUNAR_UPDATE_OTHER = "QUNAR_UPDATE_OTHER";
	
	public static String QUNAR_QUERY_OTHER = "QUNAR_QUERY_OTHER";
	
	public static String QUNAR_QUERY_FEATURE = "QUNAR_QUERY_FEATURE";
	
	public static String QUNAR_QUERY_ROUTE = "QUNAR_QUERY_ROUTE";
	
	public static String QUNAR_QUERY_TRAFFIC = "QUNAR_QUERY_TRAFFIC";
	
	public static String QUNAR_UPDATE_ROUTE = "QUNAR_UPDATE_ROUTE";
	
	public static String QUNAR_UPDATE_TRAFFIC = "QUNAR_UPDATE_TRAFFIC";
	
	public static String QUNAR_COPY_PRODUCT = "QUNAR_COPY_PRODUCT";
	
	public static String QUNAR_UPDATE_PRICE = "QUNAR_UPDATE_PRICE";
	
	/**
	 * 超时重试次数
	 */
	public static int TIMEOUT_RETRY = 10;
	
	//算价规则 每天算价
	public static String PRICE_RULE_1 = "1";
	//算价规则 单号算价
	public static String PRICE_RULE_2 = "2";
	//算价规则 双号算价 
	public static String PRICE_RULE_3 = "3";
	//算价规则 周1 3 5 算价
	public static String PRICE_RULE_4 = "4";
	//算价规则 周 2 4 6算价
	public static String PRICE_RULE_5 = "5";
	//算价规则 周2 4 6 7 算价
	public static String PRICE_RULE_6 = "6";
	
	public static String COUNT_TYPE_SYS = "sys";
	public static String COUNT_TYPE_SINGLE = "single";
	
	public static String OPT_TYPE_ALL = "2";
	public static String OPT_TYPE_SELECT = "1";
	
}
