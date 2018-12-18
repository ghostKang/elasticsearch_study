package com.yk.elasticsearchstudy;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@SpringBootApplication
@RestController
public class ElasticsearchStudyApplication {

	@Autowired
	private TransportClient client;

	@GetMapping("/")
	public String index(){
		return "index";
	}

	@GetMapping("get/people/man")
	@ResponseBody
	public ResponseEntity get(@RequestParam(name="id",defaultValue = "")String id){
		if(id.isEmpty()){
			return new ResponseEntity(HttpStatus.NOT_FOUND);

		}

		// 通过配置操作
		GetResponse response = this.client.prepareGet("people","man",id)
				.get();

		if(!response.isExists()){
			return new ResponseEntity(HttpStatus.NOT_FOUND);
		}

		return new ResponseEntity(response.getSource(),HttpStatus.OK);
	}


	@PostMapping("/add/people/man")
	@ResponseBody
	public ResponseEntity add(
			@RequestParam(name = "name") String name,
			@RequestParam(name = "country") String country,
			@RequestParam(name = "age") int age,
			@RequestParam(name = "data")
				@DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
				Date data){

		try {
			XContentBuilder content =  XContentFactory.jsonBuilder()
					.startObject()// 开始构建文档
					.field("name",name)
					.field("country",country)
					.field("age",age)
					.field("data",data.getTime())
					.endObject();// 结束构建文档
			IndexResponse response = this.client.prepareIndex("people","man")
					.setSource(content)
					.get();
			return new ResponseEntity(response.getId(),HttpStatus.OK);// 返回文档id

		}catch (IOException e){
			e.printStackTrace();
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);// 内部错误
		}

	}


	@DeleteMapping("delete/people/man")
	@ResponseBody
	public ResponseEntity delete(@RequestParam(name = "id")String id){

		DeleteResponse response = this.client.prepareDelete("people","man",id)
				.get();
		return new ResponseEntity(response.getResult().toString(),HttpStatus.OK);
	}

	@PutMapping("update/people/man")
	@ResponseBody
	public ResponseEntity update(
			@RequestParam(name = "id") String id,
			@RequestParam(name = "country",required = false) String country,
			@RequestParam(name = "age",required = false) String age
			){

		UpdateRequest update = new UpdateRequest("people","man",id);

		try {
			// 构建文档
			XContentBuilder content =  XContentFactory.jsonBuilder()
					.startObject();
			if(country != null){
				content.field("country",country);
			}
			if(age != null){
				content.field("age",age);
			}
			content.endObject();

			update.doc(content);

		}catch (IOException e){
			e.printStackTrace();
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);// 内部错误
		}

		try {
			UpdateResponse response = this.client.update(update).get();
			return new ResponseEntity(response.getResult().toString(),HttpStatus.OK);
		}catch (Exception e){
			e.printStackTrace();
			return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);// 内部错误
		}

	}


	@PostMapping("query/people/man")
	@ResponseBody
	public ResponseEntity query(
			@RequestParam(name = "name",required = false) String name,
			@RequestParam(name = "country",required = false) String country,
			@RequestParam(name = "gt_age",defaultValue = "0") int gt_age,
			@RequestParam(name = "lt_age",required = false) Integer lt_age){

		// boolean条件
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
		if(name != null)
			boolQuery.must(QueryBuilders.matchQuery("name",name));
		if(country != null)
			boolQuery.must(QueryBuilders.matchQuery("country",country));

		// 范围条件
		RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age")
				.from(gt_age);
		if(lt_age != null && lt_age > 0)
			rangeQuery.to(lt_age);

		// boolean条件结合范围条件
		boolQuery.filter(rangeQuery);

		// 构建builder
		SearchRequestBuilder builder = this.client.prepareSearch("people")// 节点
				.setTypes("man")// 类型
				.setSearchType(SearchType.DEFAULT)
				.setQuery(boolQuery)// 设置查询条件
				.setFrom(0)// 开始下标
				.setSize(10);// 查询大小

		// 发送请求
		SearchResponse response = builder.get();

		// 接收返回
		List<Map<String,Object>> result = new ArrayList<>();
		for (SearchHit hit : response.getHits())
			result.add(hit.getSource());

		return new ResponseEntity(result,HttpStatus.OK);
	}








	public static void main(String[] args) {
		SpringApplication.run(ElasticsearchStudyApplication.class, args);
	}

}

