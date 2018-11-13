package com.man.utils;

import java.util.HashMap;

public class ReqParam extends HashMap<String,Object>{

	private static final long serialVersionUID = -4078948510474302368L;

	public String getStr(String key) {
		return ObjectUtil.toString(this.get(key),"");
	}
	
	public int getInt(String key) {
		return ObjectUtil.parseInt(this.get(key));
	}
	
	public long getLong(String key) {
		return ObjectUtil.parseLong(this.get(key));
	}
}
