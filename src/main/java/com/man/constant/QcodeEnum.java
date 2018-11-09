package com.man.constant;

public enum QcodeEnum {
	QOFEN_P(-99997, -86999, "对不起，您的操作太频繁，请稍后再试。"),
	QOFEN_P1(1003,1003,"操作过于频繁咯！休息会再来操作吧"),
	QNOAUTH_P(-4009, -4009, "无权访问"),
	QLOGIN_P(-3000, -4001, "请先登录"),
	QOK_P(0,0,"正常访问");
	public int code;
	public int subcode;
	public String message;

	private QcodeEnum(int code, int subcode, String message) {
		this.code = code;
		this.subcode = subcode;
		this.message = message;
	}

	public int getCode() {
		return code;
	}

	public int getSubcode() {
		return subcode;
	}

	public String getMessage() {
		return message;
	}
}
