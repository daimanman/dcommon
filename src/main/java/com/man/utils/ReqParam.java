package com.man.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	
	public List getList(String key) {
		return ObjectUtil.castListObj(this.get(key));
	}
	
	public Map getMap(String key) {
		return ObjectUtil.castMapObj(this.get(key));
	}
	
}
