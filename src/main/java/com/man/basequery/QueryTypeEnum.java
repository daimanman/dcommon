package com.man.basequery;

public enum QueryTypeEnum {

	LIKE("LIKE"),
	IN("IN"),
	NOTIN("NOTIN"),
	BETWEEN("BETWEEN"),
	GT("GT"),
	LT("LT"),
	GTE("GTE"),
	LTE("LTE"),
	HASP("HASP"),
	NHASP("NHASP");
	private String type;
	
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	private QueryTypeEnum(String type){
		this.type = type;
	}
	
	/**
	 * 校验是否合法
	 * @param type
	 * @return
	 */
	public boolean isValid(String type){
		return type != null ?  this.getType().equals(type.trim().toUpperCase()) : false;
	}
	
	
	
	
	
	
}
