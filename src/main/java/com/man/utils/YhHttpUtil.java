package com.man.utils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import com.alibaba.fastjson.JSON;

public class YhHttpUtil {

	private static RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(15000).setConnectTimeout(15000)
			.setConnectionRequestTimeout(15000).build();

	/**
	 * 发送post请求
	 * 
	 * @author daixiaoman
	 * @date 2016年12月15日 上午9:27:15
	 */
	public static String sendHttpPost(String url, Map<String, Object> params, Map<String, String> headersMap)
			throws IOException {
		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		HttpEntity entity = null;
		String responseContent = null;

		httpClient = HttpClients.createDefault();
		HttpPost post = new HttpPost(url);
		post.setConfig(requestConfig);
		// 设置参数
		setPostFormParams(post, params);
		// 设置头信息
		setHeaders(post, headersMap);
		try {
			response = httpClient.execute(post);
			entity = response.getEntity();
			responseContent = EntityUtils.toString(entity, Consts.UTF_8);
			return responseContent;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} finally {
			if (null != response) {
				try {
					response.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (null != httpClient) {
				try {
					httpClient.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return null;
	}

	public static List<NameValuePair> parseParams(Map<String, Object> params) {
		List<NameValuePair> pairList = new ArrayList<NameValuePair>();
		if (null != params && params.size() > 0) {
			Set<String> keys = params.keySet();
			for (String key : keys) {
				Object obj = params.get(key);
				// 一个key对应多个参数
				if (null != obj && (obj instanceof Collection)) {
					Collection colParams = (Collection) obj;
					Iterator iterator = colParams.iterator();
					while (iterator.hasNext()) {
						Object paramObj = iterator.next();
						String val = ObjectUtil.toString(paramObj);
						if (null != paramObj && (paramObj instanceof String) && "null".equals(val)) {
							val = "";
						}
						pairList.add(new BasicNameValuePair(key, val));
					}
				} else {
					String objStr = ObjectUtil.toString(obj);
					if (null != obj && (obj instanceof String) && "null".equals(objStr)) {
						objStr = "";
					}
					pairList.add(new BasicNameValuePair(key, objStr));
				}
			}
		}
		return pairList;
	}

	public static String sendHttpGet(String url, Map<String, Object> params, Map<String, String> headersMap)
			throws IOException {
		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		HttpEntity entity = null;
		String responseContent = null;

		httpClient = HttpClients.createDefault();
		HttpGet get = new HttpGet(url);
		get.setConfig(requestConfig);
		// 设置请求头
		setHeaders(get, headersMap);
		String strParams;
		try {
			strParams = EntityUtils.toString(new UrlEncodedFormEntity(parseParams(params), Consts.UTF_8));
			String joinChar = "?";
			if (get.getURI().toString().indexOf("?") != -1) {
				joinChar = "&";
			}
			get.setURI(new URI(get.getURI().toString() + joinChar + strParams));

		} catch (ParseException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		try {
			response = httpClient.execute(get);
			entity = response.getEntity();
			responseContent = EntityUtils.toString(entity, Consts.UTF_8);
			return responseContent;
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} finally {
			if (null != response) {
				try {
					response.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (null != httpClient) {
				try {
					httpClient.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return null;
	}

	/**
	 * 设置请求头信息
	 * 
	 * @author daixiaoman
	 * @date 2016年12月15日 上午9:32:51
	 */
	public static void setHeaders(HttpRequest request, Map<String, String> headersMap) {
		if (null != request && null != headersMap) {
			Set<String> keys = headersMap.keySet();
			for (String key : keys) {
				request.addHeader(key, headersMap.get(key));
			}
		}
	}

	public static void setPostFormParams(HttpPost post, Map<String, Object> params) {
		post.setEntity(new UrlEncodedFormEntity(parseParams(params), Consts.UTF_8));
	}

	/**
	 * get请求超时后重试
	 * 
	 * @param url
	 * @param params
	 * @param headersMap
	 * @return
	 */
	public static String sendHttpGetWithRetry(String url, Map<String, Object> params, Map<String, String> headersMap) {
		String content = "";
		Map<String, String> retMap = new HashMap<String, String>();
		retMap.put("ret", "0");
		retMap.put("message", "请求去哪儿网超时");
		String timeOutContent = JSON.toJSONString(retMap);

		Map<String, String> successMap = new HashMap<String, String>();
		successMap.put("ret", "1");
		successMap.put("meassage", "操作成功");

		int i = 1;
		int ei = 1;
		int flag = 0;
		while ((i < ConstaintsUtil.TIMEOUT_RETRY) && (flag == 0) && (ei < ConstaintsUtil.TIMEOUT_RETRY)) {
			try {
				content = sendHttpGet(url, params, headersMap);
				flag = 1;
			} catch (IOException e) {
				e.printStackTrace();
				if (e instanceof java.net.SocketTimeoutException) {
					i++;
					content = timeOutContent;
					System.out.println("#######"+url+"#####");
					try {
						Thread.sleep(1000*5);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				} else {
					ei++;
				}
			}
		}
		return content;
		//return ObjectUtil.toString(content, JSON.toJSONString(successMap));
	}

	/**
	 * Post请求超时后重试
	 * 
	 * @param url
	 * @param params
	 * @param headersMap
	 * @return
	 */
	public static String sendHttpPostWithRetry(String url, Map<String, Object> params, Map<String, String> headersMap) {
		String content = "";
		Map<String, String> retMap = new HashMap<String, String>();
		retMap.put("ret", "0");
		retMap.put("message", "请求去哪儿网超时");
		String timeOutContent = JSON.toJSONString(retMap);

		Map<String, String> successMap = new HashMap<String, String>();
		successMap.put("ret", "1");
		successMap.put("meassage", "操作成功");

		int i = 1;
		int ei = 1;
		int flag = 0;
		while ((i < ConstaintsUtil.TIMEOUT_RETRY) && (flag == 0) && (ei < ConstaintsUtil.TIMEOUT_RETRY)) {
			try {
				content = sendHttpPost(url, params, headersMap);
				flag = 1;
			} catch (IOException e) {
				e.printStackTrace();
				if (e instanceof java.net.SocketTimeoutException) {
					i++;
					content = timeOutContent;
				} else {
					ei++;
				}
			}
		}
		return content;
	}

	/**
	 * 复制产品返回新产品id
	 * 
	 * @author daixiaoman
	 * @date 2016年12月29日 下午9:04:25
	 */
	public static String doCopyProduct(String url, Map<String, Object> params, Map<String, String> headersMap) {
		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		HttpEntity entity = null;
		String responseContent = null;

		httpClient = HttpClients.createDefault();
		HttpGet get = new HttpGet(url);
		// 设置超时时间
		RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(1000 * 10).setConnectTimeout(1000 * 10)
				.build();// 设置请求和传输超时时间
		get.setConfig(requestConfig);

		// 设置请求头
		setHeaders(get, headersMap);
		String strParams;
		try {
			strParams = EntityUtils.toString(new UrlEncodedFormEntity(parseParams(params), Consts.UTF_8));
			String joinChar = "?";
			if (get.getURI().toString().indexOf("?") != -1) {
				joinChar = "&";
			}
			get.setURI(new URI(get.getURI().toString() + joinChar + strParams));

		} catch (ParseException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		try {
			HttpContext context = new BasicHttpContext();
			response = httpClient.execute(get, context);
			System.out.println(EntityUtils.toString(response.getEntity()));
			int statusCode = response.getStatusLine().getStatusCode();
			HttpHost targetHost = (HttpHost) context.getAttribute(ExecutionContext.HTTP_TARGET_HOST);
			HttpUriRequest realRequest = (HttpUriRequest) context.getAttribute(ExecutionContext.HTTP_REQUEST);
			String location = realRequest.getURI().toString();
			// System.out.println(EntityUtils.toString(response.getEntity(),Consts.UTF_8));
			if (!ObjectUtil.isNull(location)) {
				Pattern patter = Pattern.compile("id=(\\d+)");
				Matcher m = patter.matcher(location);
				while (m.find()) {
					return m.group(1);
				}
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		} finally {
			if (null != response) {
				try {
					response.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (null != httpClient) {
				try {
					httpClient.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	public static String doCopyProductWithRetry(String url, Map<String, Object> params,
			Map<String, String> headersMap) {
		String copyId = "";
		int times = 0;
		copyId = doCopyProduct(url, params, headersMap);
		while (ObjectUtil.isNull(copyId) && times < ConstaintsUtil.TIMEOUT_RETRY * 3) {
			times++;
			copyId = doCopyProduct(url, params, headersMap);
			try {
				Thread.sleep(1000 * 4);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return copyId;
	}

	public static String uploadFile(String url, Map<String, String> joinparams, Map<String, String> headersMap,
			String name, File file) {
		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		HttpEntity entity = null;
		String responseContent = null;
		httpClient = HttpClients.createDefault();
		String newUrl = joinUrl(url, joinparams);
		HttpPost post = new HttpPost(newUrl);
		System.out.println(newUrl);
		setHeaders(post, headersMap);
		StringBody stringBody1 = new StringBody(name, ContentType.MULTIPART_FORM_DATA);
		FileBody fileBody = new FileBody(file);
		MultipartEntityBuilder builder = MultipartEntityBuilder.create();
		builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
		builder.addPart("uploadFile", fileBody);
		entity = builder.build();
		post.setEntity(entity);
		try {
			response = httpClient.execute(post);
			responseContent = EntityUtils.toString(response.getEntity(), Consts.UTF_8);
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return responseContent;
	}

	private static String joinUrl(String url, Map<String, String> params) {
		StringBuffer sb = new StringBuffer(url);
		if (null != params) {
			if (url.indexOf("?") < 0) {
				sb.append("?");
			}
			Set<String> keys = params.keySet();
			for (String key : keys) {
				sb.append("&").append(key).append("=").append(params.get(key).toString());
			}
		}
		return sb.toString();
	}

	public static String sendHttpPostJoin(String url, Map<String, String> joinParams, Map<String, Object> params,
			Map<String, String> headersMap) throws IOException {
		String newUrl = joinUrl(url, joinParams);
		return sendHttpPost(newUrl, params, headersMap);
	}

	

}
