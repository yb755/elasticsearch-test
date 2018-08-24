package com.by.elasticsearch;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElasticSearchTest {

	public static void main(String[] args) throws Exception {
		Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
		PreBuiltTransportClient transportClient=new PreBuiltTransportClient(settings);
		TransportClient client = transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.70.136"), 9300));
		addEmploy(client);

		//undateEmployee(client);
		// delEmployee(client);
		//getEmployee(client);
		querySearch(client,"company","employee","imei","*32541*");
		transportClient.close();
		client.close();
	}

	public static void addEmploy(TransportClient client) throws Exception {
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject()//
				.field("name", "zhangsan")//
				.field("age", 27)//
				.field("imei", "8633254125412")//
				.field("position", "technique english")//
				.field("country", "America")//
				.field("join_date", "2017-01-01")//
				.field("salary", "10000").endObject();//
		IndexResponse response = client.prepareIndex("company", "employee", "7").setSource(builder).get();
		System.out.println(response.getResult());
	}

	public static void delEmployee(TransportClient client) {
		DeleteResponse response = client.prepareDelete("company", "employee", "6").get();
		System.out.println(response.getResult());
	}

	public static void undateEmployee(TransportClient client) throws Exception {
		XContentBuilder builder = XContentFactory.jsonBuilder().startObject()//
				.field("name", "lisi").endObject();//
		UpdateResponse response = client.prepareUpdate("company", "employee", "6").setDoc(builder).get();
		System.out.println(response.getResult());
	}

	public static void getEmployee(TransportClient client) {
		GetResponse response = client.prepareGet("company", "employee", "6").get();
		System.out.println(response.getSourceAsString());
	}

	public static void querySearch(TransportClient client,String index, String type, String term, String queryString) {
		SearchResponse response = client.prepareSearch(index) .setTypes(type)
		// 设置查询类型
		// 1.SearchType.DFS_QUERY_THEN_FETCH = 精确查询
		// 2.SearchType.SCAN = 扫描查询,无序
		// 3.SearchType.COUNT = 不设置的话,这个为默认值,还有的自己去试试吧
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
				// 设置查询关键词
				.setQuery(QueryBuilders.wildcardQuery(term, queryString))
				.setQuery(QueryBuilders.matchQuery("country", "china"))
				// 设置查询数据的位置,分页用
				.setFrom(0)
				// 设置查询结果集的最大条数
				.setSize(60)
				// 设置是否按查询匹配度排序
				.setExplain(true)
				// 最后就是返回搜索响应信息
				.execute().actionGet();
		SearchHits searchHits = response.getHits();
		System.out.println("-----------------在[" + term + "]中搜索关键字[" + queryString + "]---------------------");
		System.out.println("共匹配到:" + searchHits.getTotalHits() + "条记录!");
		SearchHit[] hits = searchHits.getHits();
		for (SearchHit searchHit : hits) {
			Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
			Set<String> keySet = sourceAsMap.keySet();
			for (String string : keySet) {
				// key value 值对应关系
				System.out.println(string + ":" + sourceAsMap.get(string));
			}
			System.out.println();
		}
	}
}
