package com.man.utils;

public class ResultJson<T> {

	public int code;
	
	public String msg;
	
	public T data;
	
	private final static int CODE_SUCCESS=0;
	private final static int CODE_FAIL=1;

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public String getMsg() {
		return msg;
	}

	public void setMsg(String msg) {
		this.msg = msg;
	}

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}
	
	public ResultJson<T> success(String msg,T data){
		this.data = data;
		this.code = CODE_SUCCESS;
		this.msg = msg;
		return this;
	}
	
	public ResultJson<T> fail(String msg){
		this.code = CODE_FAIL;
		this.msg = msg;
		return this;
	}
	
	
	
	
	
}
