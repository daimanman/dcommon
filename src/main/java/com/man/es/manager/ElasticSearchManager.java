package com.man.es.manager;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.man.basequery.QueryBuilderParser;
import com.man.basequery.QueryItem;
import com.man.dto.CountSingleDto;
import com.man.es.query.Criterion;
import com.man.pageinfo.PageResult;
import com.man.pageinfo.QueryParams;
import com.man.pageinfo.SortParams;
import com.man.utils.ObjectUtil;

public class ElasticSearchManager  {

	private Logger logger = LoggerFactory.getLogger(ElasticSearchManager.class);

	private TransportClient client;
	
	private String clusterName;

	private String hosts;

	private int port;

	private static int DEFAULT_PORT = 9300;
	
	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getHosts() {
		return hosts;
	}

	public void setHosts(String hosts) {
		this.hosts = hosts;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	

	private String[] parseHosts() {
		String[] hostsList = new String[] {};
		if (hosts != null) {
			hostsList = hosts.split(",");
		}
		return hostsList;
	}

	private String parseHost(String url) {
		return url.split(":")[0];
	}

	private int parsePort(String url) {
		String[] s = url.split(":");
		if (s.length == 2) {
			return Integer.parseInt(s[1]);
		} else {
			return DEFAULT_PORT;
		}
	}
	
	public TransportClient initClient() {
		TransportClient client = null;
		Settings settings = Settings.builder().put("cluster.name", this.clusterName).put("client.transport.sniff", true)
				.build();
		client = new PreBuiltTransportClient(settings);
		String[] hs = parseHosts();
		if (hs != null && hs.length > 0) {
			for (String h : hs) {
				if (h != null && !"".equals(h.trim())) {
					String ph = parseHost(h);
					int port = parsePort(h);
					try {
						client.addTransportAddress(new TransportAddress(InetAddress.getByName(ph), port));
					} catch (UnknownHostException e) {
						logger.error("#########init es  error hosts={} ###########",hosts,e);
						e.printStackTrace();
					}
				}
			}
		}
		this.client = client;
		return client;
	}

	public TransportClient getClient() {
		return client;
	}

	public void setClient(TransportClient client) {
		this.client = client;
	}

	/**
	 * 索引文档
	 *
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param doc
	 *            数据
	 * @param refresh
	 *            是否刷新
	 */
	public Map<String, Object> index(String index, String type, Map<String, Object> doc, boolean refresh) {
		//String id = (String) (doc.get(ID_NAME) != null ? doc.get(ID_NAME) : UUID.randomUUID().toString());
		//doc.put("uid", id);
		IndexRequestBuilder indexRequest = client.prepareIndex(index, type,null).setSource(doc);
		if (refresh) {
			indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
		}
		indexRequest.get();
		return doc;
	}

	/**
	 * 更新文档
	 *
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param id
	 *            主键
	 * @param doc
	 *            文档
	 * @param refresh
	 *            是否刷新
	 */
	public Map<String, Object> update(String index, String type, String id, Map<String, Object> doc, boolean refresh) {
		UpdateRequestBuilder updateRequest = client.prepareUpdate(index, type, id).setDoc(doc);
		if (refresh) {
			updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
		}
		updateRequest.get();
		return doc;
	}

	/**
	 * 获取文档
	 *
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param id
	 *            主键
	 * @return 文档
	 */
	public Map<String, Object> get(String index, String type, String id) {
		GetRequestBuilder getRequest = client.prepareGet(index, type, id);
		return getRequest.get().getSource();
	}

	/**
	 * 删除文档
	 *
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param id
	 *            主键
	 * @param refresh
	 *            是否刷新
	 */
	public void delete(String index, String type, String id, boolean refresh) {
		DeleteRequestBuilder deleteRequest = client.prepareDelete(index, type, id);
		if (refresh) {
			deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
		}
	}

	/**
	 * 索引文档
	 *
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param docList
	 *            文档列表
	 * @param refresh
	 *            是否刷新
	 */
	public List<Map<String, Object>> multiIndex(String index, String type, List<Map<String, Object>> docList,
			boolean refresh) {

		if (null != docList && docList.size() > 0) {
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (Map<String, Object> doc : docList) {
				String innerId = ObjectUtil.toString(doc.get("id"));
				bulkRequest.add(client.prepareIndex(index, type, innerId).setSource(doc));
			}
			if (refresh) {
				bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
			}
			BulkResponse bulkResponse = bulkRequest.get();
			if (bulkResponse.hasFailures()) {
				logger.error(bulkResponse.buildFailureMessage());
			}
			bulkRequest.get();
		}

		return docList;
	}

	public List<Map<String, Object>> multiIndex(String index, String type, List<Map<String, Object>> docList,
			boolean refresh, String idProp) {

		if (null != docList && docList.size() > 0) {
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			for (Map<String, Object> doc : docList) {
				//String innerId = IDGenerator.uuid();
				//doc.put(ID_NAME, innerId);
				bulkRequest.add(client.prepareIndex(index, type,null).setSource(doc));
			}
			if (refresh) {
				bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
			}
			bulkRequest.get();
		}

		return docList;
	}

	/**
	 * 批量更新文档
	 *
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param docMap
	 *            文档map
	 * @param refresh
	 *            是否刷新
	 */
	public Map<String, Map<String, Object>> multiUpdate(String index, String type,
			Map<String, Map<String, Object>> docMap, boolean refresh) {
		// BulkRequestBuilder bulkRequest = client.prepareBulk();
		// docMap.forEach((key, value) -> {
		// value.put(ID_NAME, key);
		// bulkRequest.add(client.prepareUpdate(index, type,
		// key).setDoc(value));
		// });
		// if (refresh) {
		// bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
		// }
		// LOGGER.info(bulkRequest.toString());
		// bulkRequest.get();
		return docMap;
	}

	/**
	 * 批量获取文档
	 *
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param ids
	 *            主键数组
	 * @return 文档
	 */
	public Map<String, Map<String, Object>> multiGet(String index, String type, String[] ids) {
		MultiGetRequestBuilder multiGetRequest = client.prepareMultiGet().add(index, type, ids);
		Map<String, Map<String, Object>> docMap = new HashMap<String, Map<String, Object>>();
		for (MultiGetItemResponse itemResponse : multiGetRequest.get()) {
			GetResponse response = itemResponse.getResponse();
			if (response.isExists()) {
				docMap.put(response.getId(), response.getSource());
			}
		}
		return docMap;
	}

	/**
	 * 批量删除文档
	 *
	 * @param index
	 *            索引
	 * @param type
	 *            类型
	 * @param ids
	 *            主键数组
	 * @param refresh
	 *            是否刷新
	 */
	public void multiDelete(String index, String type, String[] ids, boolean refresh) {
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		// Arrays.stream(ids).forEach(id ->
		// bulkRequest.add(client.prepareDelete(index, type, id)));
		// if (refresh) {
		// bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
		// }
		// LOGGER.info(bulkRequest.toString());
		bulkRequest.get();
	}

	/**
	 * 设置元数据
	 *
	 * @param index
	 *            索引名
	 * @param type
	 *            类型名
	 * @param metadataStr
	 *            元数据字符串
	 * @return 是否成功标识
	 */
	public boolean setMetadata(String index, String type, String metadataStr) {
		// Map<String, String> properties = new HashMap<>();
		// JSONArray jsonArray =
		// JSONObject.parseObject(JSONObject.parseObject(metadataStr).getString("data")).
		// getJSONArray("children").getJSONObject(1).getJSONArray("children");
		// for (Object object : jsonArray) {
		// JSONObject jsonObject = (JSONObject) object;
		// String key = jsonObject.getString("uikey");
		// String label = jsonObject.getString("uititle");
		// properties.put(key, label);
		// }
		// JSONObject metadata = new JSONObject();
		// metadata.put("properties", properties);
		// metadata.put("source", metadataStr);
		//
		// JSONObject mapping = new JSONObject();
		// mapping.put("_meta", metadata);
		//
		// IndicesAdminClient adminClient = client.admin().indices();
		// if (!adminClient.prepareExists(index).get().isExists()) {
		// adminClient.prepareCreate(index).execute();
		// }
		// PutMappingRequestBuilder requestBuilder =
		// adminClient.preparePutMapping(new String[]{index}).setType(type);
		// requestBuilder.setSource(mapping.toJSONString(), XContentType.JSON);
		// return requestBuilder.execute().actionGet().isAcknowledged();
		return false;
	}

	public Map<String, Object> filterForObject(String index, String type, Criterion criterion) {

		return null;
	}

	public long count(String[] indexes, String[] types, Criterion criterion) {
		return 0;
	}

	/**
	 * 获取文档内容
	 *
	 * @param response
	 *            {@link SearchResponse}
	 * @return 文档列表
	 */
	private List<Map<String, Object>> getDocContent(SearchResponse response) {
		List<Map<String, Object>> docList = new ArrayList<>();
		for (SearchHit hit : response.getHits()) {
			Map<String, Object> doc = hit.getSourceAsMap();
			doc.put("_index", hit.getIndex());
			doc.put("_type", hit.getType());
			docList.add(doc);
		}
		return docList;
	}

	private void setSort(SearchRequestBuilder searchRequest, List<SortParams> sorts) {

		// TODO 需要判断字段是否支持排序,全文检索的字段不能参与排序处理

		// 是否需要排序
		if (null != sorts && sorts.size() > 0) {
			// 设置排序字段
			for (SortParams sort : sorts) {
				if (sort.getField() != null && !sort.getField().trim().equals("") && sort.getSort() != null
						&& (sort.getSort().toLowerCase().equals("asc")
								|| sort.getSort().toLowerCase().equals("desc"))) {
					searchRequest.addSort(new FieldSortBuilder(sort.getField())
							.order(SortOrder.valueOf(sort.getSort().toUpperCase())));
				}
			}
		}
	}

	/**
	 * 查询文档列表
	 * 
	 * @param index
	 * @param type
	 * @param size
	 * @param queryParams
	 * @return
	 */
	public List<Map<String, Object>> filterList(String index, String type, int size, List<QueryItem> queryParams,
			List<SortParams> sorts) {
		SearchRequestBuilder searchRequest = client.prepareSearch(index).setSize(size).setTypes(type)
				.setQuery(new QueryBuilderParser().parseQueryItems(queryParams));
				//.setPostFilter(new QueryBuilderParser().parseQueryItems(queryParams));
		setSort(searchRequest, sorts);
		return getDocContent(searchRequest.get());
	}
	
	public List<Map<String,Object>> queryList(String index,String type,QueryParams queryParams){
		return filterList(index,type,queryParams.getPageSize(),queryParams.getQueryItems(),queryParams.getSorts());
	}

	/**
	 * 分页获取数据
	 */
	public PageResult<Map<String, Object>> filterPage(String index, String type, QueryParams queryParams) {
		PageResult<Map<String, Object>> pageResult = new PageResult<Map<String, Object>>();
		SearchRequestBuilder searchRequest = client.prepareSearch(index).setTypes(type)
				.setQuery(new QueryBuilderParser().parseQueryItems(queryParams.getQueryItems()));
				//.setPostFilter(new QueryBuilderParser().parseQueryItems(queryParams.getQueryItems()));
		searchRequest.setFrom(queryParams.getOffset()).setSize(queryParams.getPageSize());
		setSort(searchRequest, queryParams.getSorts());
		SearchResponse searchResponse = searchRequest.get();
		pageResult.setTotal(searchResponse.getHits().getTotalHits());
		pageResult.setPage(queryParams.getPage());
		pageResult.setPageSize(queryParams.getPageSize());
		pageResult.setDatas(getDocContent(searchResponse));
		return pageResult;
	}

	public Map<String, Object> filterOneObj(String index, String type, QueryItem queryItem) {
		List<QueryItem> items = new ArrayList<QueryItem>();
		items.add(queryItem);
		List<Map<String, Object>> datas = filterList(index, type, 1, items, null);
		if (datas != null && datas.size() > 0) {
			return datas.get(0);
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public Map<String,Object> getIndexFields(String index,String type){
		ImmutableOpenMap<String, MappingMetaData> mappings = client.admin().cluster().prepareState().execute()
                .actionGet().getState().getMetaData().getIndices().get(index).getMappings();
        String maps  = mappings.get(type).source().toString();
        Map<String,Object> topMap = JSON.parseObject(maps,Map.class);
        Map<String,Object> typeMap = ObjectUtil.castMapObj(topMap.get(type));
        Map<String,Object> colsMap = ObjectUtil.castMapObj(typeMap.get("properties"));
        return colsMap;
	}
	
	
	//get max id
	public long getMaxId(String index,String type) {
		MaxAggregationBuilder  aggsBuilder = AggregationBuilders.max("maxid").field("id");
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type) 
                .addAggregation(aggsBuilder)    
                .setSize(0); 
		 SearchResponse sr = searchRequestBuilder.execute().actionGet();
		 Max max = sr.getAggregations().get("maxid");
		 
		 double  maxVal = max.getValue();
		 return (long)maxVal;
	}
	
	public List<CountSingleDto> countGroupOneField(String index,String type,String field,int size,QueryParams queryParams){
		List<CountSingleDto> datas = new ArrayList<>();
		SearchRequestBuilder searchRequest = client.prepareSearch(index).setTypes(type)
				.setQuery(new QueryBuilderParser().parseQueryItems(queryParams.getQueryItems()));
		TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("count_group").field(field);
		termsAggregationBuilder.size(size);
		searchRequest.addAggregation(termsAggregationBuilder);
		searchRequest.setSize(0);
		SearchResponse sr = searchRequest.execute().actionGet();
		Terms aggregation = sr.getAggregations().get("count_group");
		 for (Terms.Bucket bucket : aggregation.getBuckets()) {
			 CountSingleDto data = new CountSingleDto();
			 data.key = bucket.getKeyAsString();
			 data.value = bucket.getDocCount();
			 datas.add(data);
		 }     
		return datas;
	}

}
