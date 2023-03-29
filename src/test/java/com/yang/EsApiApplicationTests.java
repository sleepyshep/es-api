package com.yang;

import com.alibaba.fastjson.JSON;
import com.yang.pojo.User;
import com.yang.utils.ESconst;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

// ES API 测试
@SpringBootTest
class YangEsApiApplicationTests {

	@Autowired
	@Qualifier("restHighLevelClient")
	private RestHighLevelClient client;

	// 创建索引
	@Test
	void testCreateIndex() throws IOException {
		// 1、创建索引请求
		CreateIndexRequest request = new CreateIndexRequest("zhengce");
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		{
			builder.startObject("properties");
			{
				builder.startObject("POLICY_ID");
				{
					builder.field("type", "long");
				}
				builder.endObject();

				builder.startObject("POLICY_TITLE");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("POLICY_GRADE");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("PUB_AGENCY_ID");
				{
					builder.field("type", "long");
				}
				builder.endObject();

				builder.startObject("PUB_AGENCY");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("PUB_AGENCY_FULLNAME");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("PUB_NUMBER");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("PUB_TIME");
				{
					builder.field("type", "date");
					builder.field("format", "yyyy/MM/dd");
				}
				builder.endObject();

				builder.startObject("POLICY_TYPE");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("POLICY_BODY");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("PROVINCE");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("CITY");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("POLICY_SOURCE");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("UPDATE_DATE");
				{
					builder.field("type", "date");
					builder.field("format", "yyyy/MM/dd");
				}
				builder.endObject();
			}
			builder.endObject();
		}
		builder.endObject();
		request.mapping(builder);
		// 2、客户端执行请求，请求获得响应
		CreateIndexResponse createIndexResponse =
				client.indices().create(request, RequestOptions.DEFAULT);

		System.out.println(createIndexResponse);
	}

	// 修改mapping
	@Test
	void testUpdateMap() throws IOException {
		// 1、创建索引请求
		PutMappingRequest request = new PutMappingRequest("jiansuo_index");
		XContentBuilder builder = XContentFactory.jsonBuilder();
		builder.startObject();
		{
			builder.startObject("properties");
			{
				builder.startObject("POLICY_ID");
				{
					builder.field("type", "long");
				}
				builder.endObject();

				builder.startObject("POLICY_TITLE");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("POLICY_GRADE");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("PUB_AGENCY_ID");
				{
					builder.field("type", "long");
				}
				builder.endObject();

				builder.startObject("PUB_AGENCY");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("PUB_AGENCY_FULLNAME");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("PUB_NUMBER");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("PUB_TIME");
				{
					builder.field("type", "date");
					builder.field("format", "yyyy/MM/dd");
				}
				builder.endObject();

				builder.startObject("POLICY_TYPE");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("POLICY_BODY");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("PROVINCE");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("CITY");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("POLICY_SOURCE");
				{
					builder.field("type", "text");
					builder.field("analyzer", "ik_max_word");
				}
				builder.endObject();

				builder.startObject("UPDATE_DATE");
				{
					builder.field("type", "date");
					builder.field("format", "yyyy/MM/dd");
				}
				builder.endObject();
			}
			builder.endObject();
		}
		builder.endObject();
		request.source(builder);
		// 2、客户端执行请求，请求获得响应
		AcknowledgedResponse putMappingResponse = client.indices().putMapping(request, RequestOptions.DEFAULT);

		System.out.println(putMappingResponse.isAcknowledged());
	}

	// 获取索引，判断是否存在
	@Test
	void testExistIndex() throws IOException{
		GetIndexRequest request = new GetIndexRequest("yang_index");
		boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
		System.out.println(exists);
	}

	// 删除索引
	@Test
	void testDeleteIndex() throws IOException{
		DeleteIndexRequest request = new DeleteIndexRequest("yang_index");
		AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
		System.out.println(delete.isAcknowledged());
	}

	// 添加文档：将数据封装为对象，对象转JSON
	@Test
	void testAddDocument() throws IOException{
		// 创建对象
		User user = new User("羊叙铮", 3);
		// 创建请求
		IndexRequest request = new IndexRequest("yang_index");

		// 规则 put /yang_index/_doc/1
		request.id("1");
		request.timeout(TimeValue.timeValueSeconds(1));
		request.timeout("1s");

		// 将JSON数据放入请求
		request.source(JSON.toJSONString(user), XContentType.JSON);

		//客户端发送请求，获取响应结果
		IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

		System.out.println(indexResponse.toString());
		System.out.println(indexResponse.status());
	}

	// 获取文档,判断是否存在	get /index/_doc/1
	@Test
	void testIsExists() throws IOException {
		GetRequest getRequest = new GetRequest("yang_index", "1");
		// 不获取返回的 _source 的上下文
		getRequest.fetchSourceContext(new FetchSourceContext(false));
		getRequest.storedFields("_none_");

		boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
		System.out.println(exists);
	}

	// 获取文档信息
	@Test
	void testGetDocument() throws IOException {
		GetRequest getRequest = new GetRequest("jiansuo_index", "1");
		GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
		// 打印文档内容
		System.out.println(getResponse.getSourceAsString());
		// 返回的全部内容和命令是一样的
		System.out.println(getResponse);
	}

	// 更新文档信息
	@Test
	void testUpdateDocument() throws IOException {
		UpdateRequest updateRequest = new UpdateRequest("yang_index", "1");
		updateRequest.timeout("1s");

		User user = new User("羊叙铮ES", 18);
		updateRequest.doc(JSON.toJSONString(user), XContentType.JSON);

		UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
		System.out.println(updateResponse.status());
	}

	// 删除文档记录
	@Test
	void testDeleteDocument() throws IOException {
		DeleteRequest request = new DeleteRequest("zhengce", "DCkCKIcBR6VvlVZsQmPP");
		request.timeout("1s");

		DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);
		System.out.println(deleteResponse.status());
	}

	// 真实项目：批量插入、批量查询
	@Test
	void testBulkRequest() throws IOException {
		BulkRequest bulkRequest = new BulkRequest();
		bulkRequest.timeout("600s");

		ArrayList<Object> userList = new ArrayList<>();
		userList.add(new User("yang", 3));
		userList.add(new User("xu", 3));
		userList.add(new User("zheng", 3));
		userList.add(new User("123", 3));

		// 批处理请求
		for (int i = 0; i < userList.size(); i++) {
			bulkRequest.add(
					new IndexRequest("yang_index")
					.id(""+(i+1))
					.source(JSON.toJSONString(userList.get(i)), XContentType.JSON));
		}

		BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
		// 是否失败，返回false表示成功
		System.out.println(bulkResponse.hasFailures());
	}

	// 查询
	// SearchRequest 搜索请求
	// SearchSourceBuilder 条件构造
	// HighlightBuilder 构建高亮
	// TermQueryBuilder 精确查询
	// MatchAllQueryBuilder
	// xxxQueryBuilder 对应所有查询命令
	@Test
	void testSearch() throws IOException {
		SearchRequest searchRequest = new SearchRequest(ESconst.ES_INDEX);
		// 构建搜索条件
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

		// 查询条件，可以使用 QueryBuilders 工具来实现
		// QueryBuilders.termQuery 精确匹配
		// QueryBuilders.matchAllQuery 匹配所有
		TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("age", 3);
//		MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
		sourceBuilder.query(termQueryBuilder);
//		sourceBuilder.from();
//		sourceBuilder.size();
		sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

		searchRequest.source(sourceBuilder);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		System.out.println(JSON.toJSONString(searchResponse.getHits()));
		System.out.println("--------------------------");
		for (SearchHit documentFields : searchResponse.getHits().getHits()) {
			System.out.println(documentFields.getSourceAsMap());
		}
	}
}














