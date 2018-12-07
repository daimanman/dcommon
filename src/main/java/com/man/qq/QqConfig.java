package com.man.qq;

public class QqConfig {

	// shuoshuo
	public static String QQ_EMOTINFO_URL = "https://h5.qzone.qq.com/proxy/domain/taotao.qq.com/cgi-bin/emotion_cgi_msglist_v6";

	// 个人信息
	public static String QQ_BASEINFO_URL = "https://h5.qzone.qq.com/proxy/domain/base.qzone.qq.com/cgi-bin/user/cgi_userinfo_get_all";

	// msg
	public static String QQ_MSGINFO_URL = "https://h5.qzone.qq.com/proxy/domain/m.qzone.qq.com/cgi-bin/new/get_msgb";

	// photo
	public static String QQ_PHOTOINFO_URL = "https://h5.qzone.qq.com/proxy/domain/alist.photo.qq.com/fcgi-bin/fcg_list_album_v3";

	// image
	public static String QQ_IMGINFO_URL = "https://h5.qzone.qq.com/proxy/domain/plist.photo.qzone.qq.com/fcgi-bin/cgi_list_photo";

	// visit
	public static String QQ_VISITINFO_URL = "https://h5.qzone.qq.com/proxy/domain/g.qzone.qq.com/cgi-bin/friendshow/cgi_get_visitor_simple";

	//publish emot 
	public static String QQ_SENT_EMOT_URL = "https://user.qzone.qq.com/proxy/domain/taotao.qzone.qq.com/cgi-bin/emotion_cgi_publish_v6";
	
	//img video
	public static String QQ_IMG_VEDIO_URL = "https://h5.qzone.qq.com/proxy/domain/photo.qzone.qq.com/fcgi-bin/cgi_floatview_photo_list_v2";
	
	// emot num
	public static int DEFAULT_EMOT_NUM = 30;
	
	//emot num real return 
	public static int DEFAULT_EMOT_REAL_NUM = DEFAULT_EMOT_NUM;

	// msg num
	public static int DEFAULT_MSG_NUM = 20;

	// img num

	public static int DEFAULT_IMG_NUM = 500;

	public static int CODE_VERSION = 1;

	public static int REPLYNUM = 100;

	public static String ALLOW_ACCESS = "1";
}
