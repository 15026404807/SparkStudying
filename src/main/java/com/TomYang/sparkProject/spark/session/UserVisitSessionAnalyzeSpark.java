package com.TomYang.sparkProject.spark.session;


import groovy.ui.SystemOutputInterceptor;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import com.TomYang.sparkProject.conf.ConfigurationManager;
import com.TomYang.sparkProject.constant.Constants;
import com.TomYang.sparkProject.dao.ISessionAggrStatDAO;
import com.TomYang.sparkProject.dao.ISessionDetailDAO;
import com.TomYang.sparkProject.dao.ISessionRandomExtractDAO;
import com.TomYang.sparkProject.dao.ITaskDAO;
import com.TomYang.sparkProject.dao.ITop10SessionDAO;
import com.TomYang.sparkProject.dao.factory.DAOFactory;
import com.TomYang.sparkProject.domain.SessionAggrStat;
import com.TomYang.sparkProject.domain.SessionDetail;
import com.TomYang.sparkProject.domain.SessionRandomExtract;
import com.TomYang.sparkProject.domain.Task;
import com.TomYang.sparkProject.domain.Top10Session;
import com.TomYang.sparkProject.test.MockData;
import com.TomYang.sparkProject.util.DateUtils;
import com.TomYang.sparkProject.util.NumberUtils;
import com.TomYang.sparkProject.util.ParamUtils;
import com.TomYang.sparkProject.util.StringUtils;
import com.TomYang.sparkProject.util.ValidUtils;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;

@SuppressWarnings("unused")
public class UserVisitSessionAnalyzeSpark {
	
	public static void main(String[] args){
		
		args = new String[]{"2"};
		SparkConf conf = new SparkConf()
				.setAppName(Constants.SPARK_APP_NAME_SESSION)
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = getSQLContext(sc.sc());
		mockData(sc, sqlContext);
		
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		long taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION);
		Task task = taskDAO.findById(taskId);
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		/*System.out.println(taskParam);*/
		
		/* Constants.PARAM_START_DATE);
		 Constants.PARAM_END_DATE);*/
		
		JavaRDD<Row> actionRDD = getActionRDDByDateRange(sqlContext, taskParam);
		
		JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
		
		JavaPairRDD<String,String> sessionId2AggrInfoRDD = aggregateBySession(sqlContext,actionRDD);
		
		for(Tuple2<String,String> t: sessionId2AggrInfoRDD.take(10)){
			System.out.println(t._2);
		}
		System.out.println("sessionId2AggrInfoRDD数量："+sessionId2AggrInfoRDD.count());
		
		//步长统计
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("",new SessionAggrStatAccumulator());
		
		//过滤session 
		// 相当于我们自己编写的算子，是要访问外面的任务参数对象的
		// 匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的 
		JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD = filterSession(sessionId2AggrInfoRDD, taskParam,sessionAggrStatAccumulator);
		System.out.println("filteredSessionid2AggrInfoRDD："+filteredSessionid2AggrInfoRDD.count());
		
		for(Tuple2<String,String> t: filteredSessionid2AggrInfoRDD.take(10)){
			System.out.println(t._2);
		}
		System.out.println(sessionAggrStatAccumulator.value());
		
		randomExtractSession(taskId,filteredSessionid2AggrInfoRDD,sessionid2actionRDD);
		
		getTop10Category(filteredSessionid2AggrInfoRDD,sessionid2actionRDD);
		
		//calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(),taskId);
		
		
		sc.close();					
	
	}
	
	/**
	 * 获取sessionid2到访问行为数据的映射的RDD
	 * @param actionRDD 
	 * @return
	 */
	public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
		return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				return new Tuple2<String, Row>(row.getString(2), row);  
			}
			
		});
	}
	
	private static JavaPairRDD<String,String> aggregateBySession(SQLContext sqlContext, JavaRDD<Row> actionRDD){
		
		//user_visit_action 
		JavaPairRDD<String,Row> sessionId2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				
				return new Tuple2<String,Row>(row.getString(2),row);
			}
		});
		
		JavaPairRDD<String,Iterable<Row>> session2ActionsRDD = sessionId2ActionRDD.groupByKey();
		
		JavaPairRDD<Long, String> userId2PartAggrInfoRDD = session2ActionsRDD.mapToPair(
				new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<Long, String> call(
							Tuple2<String, Iterable<Row>> tuple) throws Exception {
						String sessionId = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();
						StringBuffer searchKeywordsBuffer = new StringBuffer("");
						StringBuffer clickCateGoryIdsBuffer = new StringBuffer("");

						Long userId = null;
						Date startTime = null;
						Date endTime = null;
						int stepLength = 0;
						
						
						while(iterator.hasNext()){
							Row row = iterator.next();
							if(userId == null) {
								userId = row.getLong(1);
							}
							String searchKeyWord = row.getString(5);
							Long clickCategoryId = row.getLong(6);
							
							if(StringUtils.isNotEmpty(searchKeyWord)){
								if(!searchKeywordsBuffer.toString().contains(searchKeyWord)){
									searchKeywordsBuffer.append(searchKeyWord + ",");
								}
							}
							
							if(clickCategoryId != null){
								if(!clickCateGoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
									clickCateGoryIdsBuffer.append(clickCategoryId + ",");
								}
							}
							
							Date actionTime = DateUtils.parseTime(row.getString(4));
							
							if(startTime == null) {
								startTime = actionTime;
							}
							
							if(endTime == null){
								endTime = actionTime;
							}
							
							if(actionTime.before(startTime)){
								startTime = actionTime;
							}
							
							if(actionTime.after(endTime)){
								endTime = actionTime;
							}
							
							//session访问步长
							stepLength++;
						}
						
						String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
						String clickCategoryIds = StringUtils.trimComma(clickCateGoryIdsBuffer.toString());
						long visitLength = (endTime.getTime() - startTime.getTime())/1000;
						
						// 我们这里统一定义，使用key=value|key=value
						String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
								+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
								+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
								+ Constants.FIELD_STEP_LENGTH + "=" + stepLength   + "|"
								+ Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime);    
						return new Tuple2<Long, String>(userId,partAggrInfo);
					}
		});
		
		String sql = "select * from user_info";  
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
		
		JavaPairRDD<Long,Row> userId2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, Row> call(Row row) throws Exception {
				return new Tuple2<Long,Row>(row.getLong(0),row);
			}
		});
		
		JavaPairRDD<Long, Tuple2<String,Row>> userId2FullInfoRDD = userId2PartAggrInfoRDD.join(userId2InfoRDD);
		JavaPairRDD<String,String> sessionid2FullAggrInfoRDD = userId2FullInfoRDD.mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(
					Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
				String partAggrInfo = tuple._2._1;
				Row userInfoRow = tuple._2._2;
				
				String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
				
				int age = userInfoRow.getInt(3);
				String professional = userInfoRow.getString(4);
				String city = userInfoRow.getString(5);
				String sex = userInfoRow.getString(6);
				
				String fullAggrInfo = partAggrInfo + "|"
						+ Constants.FIELD_AGE + "=" + age + "|"
						+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
						+ Constants.FIELD_CITY + "=" + city + "|"
						+ Constants.FIELD_SEX + "=" + sex;
				
				return new Tuple2<String, String>(sessionId, fullAggrInfo);
			}
		});
		
		return sessionid2FullAggrInfoRDD;
	}
	
	private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext, JSONObject taskParam){
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		String sql = " select * " 
					+ " from user_visit_action "
					+ "where date>='" + startDate + "' "
					+ "and date<='" + endDate + "'";  
		DataFrame actionDF = sqlContext.sql(sql);
		return actionDF.javaRDD();
	}
	
	private static SQLContext getSQLContext(SparkContext sc){
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			return new SQLContext(sc);
		}else{
			return new HiveContext(sc);
		}
	}
	
	private static void mockData(JavaSparkContext sc, SQLContext sqlContext){
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			//生成一个临时表
			MockData.mock(sc,sqlContext);
		}
	}
	
	private static JavaPairRDD<String,String> filterSession(JavaPairRDD<String, String> sessionId2AggreInfoRDD, final JSONObject taskParam,
			final Accumulator<String> sessionAggrStatAccumulator){
		/*System.out.println("2========================="+ taskParam);*/
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		//用户的职业
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
		
		//拼接条件？
		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "");
		
		
		
		if(_parameter.endsWith("\\|")){
			_parameter = _parameter.substring(0,_parameter.length() - 1);
		}
		
		// ??? 
		final String parameter = _parameter;
		
		System.out.println("拼接条件："+ parameter);
		
		JavaPairRDD<String, String> filteredSessionId2AggrInfoRDD = sessionId2AggreInfoRDD.filter(new Function<Tuple2<String,String>,Boolean>(){
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
				String aggrInfo = tuple._2;
				/*System.out.println("aggrInfo："+ aggrInfo);*/
				if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)){
					return false;
				}
				
				if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, 
						parameter, Constants.PARAM_PROFESSIONALS)) {
					return false;
				}
				
				// 按照城市范围进行过滤（cities）
				// 北京,上海,广州,深圳
				// 成都
				if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, 
						parameter, Constants.PARAM_CITIES)) {
					return false;
				}
				
				// 按照性别进行过滤
				// 男/女
				// 男，女
				if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, 
						parameter, Constants.PARAM_SEX)) {
					return false;
				}
				
				// 按照搜索词进行过滤
				// 我们的session可能搜索了 火锅,蛋糕,烧烤
				// 我们的筛选条件可能是 火锅,串串香,iphone手机
				// 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
				// 任何一个搜索词相当，即通过
				if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, 
						parameter, Constants.PARAM_KEYWORDS)) {
					return false;
				}
				
				// 按照点击品类id进行过滤
				if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, 
						parameter, Constants.PARAM_CATEGORY_IDS)) {
					return false;
				}
				
				sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
				long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(
						aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH)); 
				long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(
						aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));  
				calculateVisitLength(visitLength); 
				calculateStepLength(stepLength);  
				return true;
			}
			
			//计算访问时长范围
			private void calculateVisitLength(long visitLength){
				if(visitLength >=1 && visitLength <= 3) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);  
				} else if(visitLength >=4 && visitLength <= 6) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);  
				} else if(visitLength >=7 && visitLength <= 9) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);  
				} else if(visitLength >=10 && visitLength <= 30) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);  
				} else if(visitLength > 30 && visitLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);  
				} else if(visitLength > 60 && visitLength <= 180) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);  
				} else if(visitLength > 180 && visitLength <= 600) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);  
				} else if(visitLength > 600 && visitLength <= 1800) {  
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);  
				} else if(visitLength > 1800) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);  
				}
			}
			
			private void calculateStepLength(long stepLength) {
				if(stepLength >= 1 && stepLength <= 3) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);  
				} else if(stepLength >= 4 && stepLength <= 6) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);  
				} else if(stepLength >= 7 && stepLength <= 9) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);  
				} else if(stepLength >= 10 && stepLength <= 30) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);  
				} else if(stepLength > 30 && stepLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);  
				} else if(stepLength > 60) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);    
				}
			}
		});
		  return filteredSessionId2AggrInfoRDD;
		}
	
	private static void randomExtractSession(final long taskId, 
											 JavaPairRDD<String,String> session2AggrInfoRDD,
											 JavaPairRDD<String,Row> sessionid2actionRDD){
		
		JavaPairRDD<String,String> time2sessionIDRDD = session2AggrInfoRDD.mapToPair(new PairFunction<Tuple2<String,String>,String,String>(){

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, String> call(Tuple2<String, String> t)
					throws Exception {
				String aggrInfo = t._2;
				String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
				String dateHour = DateUtils.getDateHour(startTime);
				return new Tuple2<String,String>(dateHour,aggrInfo);
			}
		});
		
		Map<String,Object> countMap = time2sessionIDRDD.countByKey();
		
		Map<String,Map<String, Long>> dateHourCountMap = new HashMap<String,Map<String, Long>>();
		for(Map.Entry<String, Object> countEntry : countMap.entrySet()){
			String dateHour = countEntry.getKey();
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];
			
			long count = Long.valueOf(String.valueOf(countEntry.getValue()));
			Map<String,Long> hourCountMap = dateHourCountMap.get(date);
			if(hourCountMap == null){
				hourCountMap = new HashMap<String,Long>();
				dateHourCountMap.put(date, hourCountMap);
			}
			hourCountMap.put(hour, count);
		}
		
		// 总共要抽取100个session，先按照天数，进行平分
		int extractNumberPerDay = 100 / dateHourCountMap.size();
		
		// <date,<hour,(3,5,20,102)>>  
		final Map<String, Map<String, List<Integer>>> dateHourExtractMap = 
				new HashMap<String, Map<String, List<Integer>>>();
		
		Random random = new Random();
				
		for(Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
			String date = dateHourCountEntry.getKey();
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
			
			// 计算出这一天的session总数
			long sessionCount = 0L;
			for(long hourCount : hourCountMap.values()) {
				sessionCount += hourCount;
			}
			
			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
			if(hourExtractMap == null) {
				hourExtractMap = new HashMap<String, List<Integer>>();
				dateHourExtractMap.put(date, hourExtractMap);
			}
			
			// 遍历每个小时
			for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
				String hour = hourCountEntry.getKey();
				long count = hourCountEntry.getValue();
				
				// 计算每个小时的session数量，占据当天总session数量的比例，直接乘以每天要抽取的数量
				// 就可以计算出，当前小时需要抽取的session数量
				int hourExtractNumber = (int)(((double)count / (double)sessionCount) 
						* extractNumberPerDay);
				if(hourExtractNumber > count) {
					hourExtractNumber = (int) count;
				}
				
				// 先获取当前小时的存放随机数的list
				List<Integer> extractIndexList = hourExtractMap.get(hour);
				if(extractIndexList == null) {
					extractIndexList = new ArrayList<Integer>();
					hourExtractMap.put(hour, extractIndexList);
				}
				
				// 生成上面计算出来的数量的随机数
				for(int i = 0; i < hourExtractNumber; i++) {
					int extractIndex = random.nextInt((int) count);
					while(extractIndexList.contains(extractIndex)) {
						extractIndex = random.nextInt((int) count);
					}
					extractIndexList.add(extractIndex);
				}
			}
		}

		JavaPairRDD<String,Iterable<String>> time2sessionsRDD = time2sessionIDRDD.groupByKey();
		
		JavaPairRDD<String, String> extractSessionsRDD =  time2sessionsRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<String>>,String,String>(){
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, String>> call(
					Tuple2<String, Iterable<String>> tuple)
					throws Exception {
				List<Tuple2<String, String>> extractSessionids = 
						new ArrayList<Tuple2<String, String>>();
				
				String dateHour = tuple._1;
				String date = dateHour.split("_")[0];
				String hour = dateHour.split("_")[1];
				Iterator<String> iterator = tuple._2.iterator();
				
				List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);  
				
				ISessionRandomExtractDAO sessionRandomExtractDAO = 
						DAOFactory.getSessionRandomExtractDAO();
				
				int index = 0;
				while(iterator.hasNext()) {
					String sessionAggrInfo = iterator.next();
					
					if(extractIndexList.contains(index)) {
						String sessionid = StringUtils.getFieldFromConcatString(
								sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						
						// 将数据写入MySQL
						SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
						sessionRandomExtract.setTaskid(taskId);  
						sessionRandomExtract.setSessionid(sessionid);  
						sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
								sessionAggrInfo, "\\|", Constants.FIELD_START_TIME));  
						sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
								sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
						sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
								sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
						
						sessionRandomExtractDAO.insert(sessionRandomExtract);  
						
						// 将sessionid加入list
						extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));  
					}
					index++;
				}
				return extractSessionids;
			}
		});
		JavaPairRDD<String,Tuple2<String,Row>> extractActioinDetailRDD = extractSessionsRDD.join(sessionid2actionRDD);
		extractActioinDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {  
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				Row row = tuple._2._2;
				SessionDetail sessionDetail = new SessionDetail();
				sessionDetail.setTaskid(taskId);  
				sessionDetail.setUserid(row.getLong(1));  
				sessionDetail.setSessionid(row.getString(2));  
				sessionDetail.setPageid(row.getLong(3));  
				sessionDetail.setActionTime(row.getString(4));
				sessionDetail.setSearchKeyword(row.getString(5));  
				sessionDetail.setClickCategoryId(row.getLong(6));  
				sessionDetail.setClickProductId(row.getLong(7));   
				sessionDetail.setOrderCategoryIds(row.getString(8));  
				sessionDetail.setOrderProductIds(row.getString(9));  
				sessionDetail.setPayCategoryIds(row.getString(10)); 
				sessionDetail.setPayProductIds(row.getString(11));  
				
				ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
				sessionDetailDAO.insert(sessionDetail);  
			}
		});
		
		/*System.out.println(extractSessionsRDD.take(10));
		System.out.println(extractSessionsRDD.count());*/
		
	}
	
	/**
	 * 获取top10热门品类
	 * @param filteredSessionid2AggrInfoRDD
	 * @param sessionid2actionRDD
	 */
	private static void getTop10Category(  
			JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) {
		/**
		 * 第一步：获取符合条件的session访问过的所有品类
		 */
		
		// 获取符合条件的session的访问明细
		JavaPairRDD<String, Row> sessionid2detailRDD = filteredSessionid2AggrInfoRDD
				.join(sessionid2actionRDD)
				.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {
		
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Row> call(
							Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
						return new Tuple2<String, Row>(tuple._1, tuple._2._2);
					}
					
				});
		
		// 获取session访问过的所有品类id
		// 访问过：指的是，点击过、下单过、支付过的品类
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						
						Long clickCategoryId = row.getLong(6);
						if(clickCategoryId != null) {
							list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));   
						}
						
						String orderCategoryIds = row.getString(8);
						if(orderCategoryIds != null) {
							String[] orderCategoryIdsSplited = orderCategoryIds.split(",");  
							for(String orderCategoryId : orderCategoryIdsSplited) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),
										Long.valueOf(orderCategoryId)));
							}
						}
						
						String payCategoryIds = row.getString(10);
						if(payCategoryIds != null) {
							String[] payCategoryIdsSplited = payCategoryIds.split(",");  
							for(String payCategoryId : payCategoryIdsSplited) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
										Long.valueOf(payCategoryId)));
							}
						}
						
						return list;
					}
					
				});
		
		// 计算各个品类的点击次数
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = 
				getClickCategoryId2CountRDD(sessionid2detailRDD);
		// 计算各个品类的下单次数
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = 
				getOrderCategoryId2CountRDD(sessionid2detailRDD);
		// 计算各个品类的支付次数
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = 
				getPayCategoryId2CountRDD(sessionid2detailRDD);
		
		JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, 
				payCategoryId2CountRDD);
		
		
		//自定义二次排序（以相应的Key为键）
		JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD.mapToPair(new PairFunction<Tuple2<Long,String>, CategorySortKey, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<CategorySortKey, String> call(
					Tuple2<Long, String> tuple) throws Exception {
				String countInfo = tuple._2;
				long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
						countInfo, "\\|", Constants.FIELD_CLICK_COUNT));  
				long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
						countInfo, "\\|", Constants.FIELD_ORDER_COUNT));  
				long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
						countInfo, "\\|", Constants.FIELD_PAY_COUNT));  
				
				CategorySortKey sortKey = new CategorySortKey(clickCount,
						orderCount, payCount);
				
				return new Tuple2<CategorySortKey, String>(sortKey, countInfo);  
			}
			
		});
		
		JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = 
				sortKey2countRDD.sortByKey(false);

		System.out.println(sortedCategoryCountRDD.take(100));
	}
	
	/**
	 * 获取各品类点击次数RDD
	 * @param sessionid2detailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(
				
				new Function<Tuple2<String,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;  
						return Long.valueOf(row.getLong(6)) != null ? true : false;
					}
					
				});
		
		JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
				
				new PairFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Long> call(Tuple2<String, Row> tuple)
							throws Exception {
						long clickCategoryId = tuple._2.getLong(6);
						return new Tuple2<Long, Long>(clickCategoryId, 1L);
					}
					
				});
		
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
				
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
					
				});
		
		return clickCategoryId2CountRDD;
	}
	
	/**
	 * 获取各品类的下单次数RDD
	 * @param sessionid2detailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(
				
				new Function<Tuple2<String,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;  
						return row.getString(8) != null ? true : false;
					}
					
				});
		
		JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String orderCategoryIds = row.getString(8);
						String[] orderCategoryIdsSplited = orderCategoryIds.split(",");  
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						
						for(String orderCategoryId : orderCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));  
						}
						
						return list;
					}
					
				});
		
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
				
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
					
				});
		
		return orderCategoryId2CountRDD;
	}
	
	/**
	 * 获取各个品类的支付次数RDD
	 * @param sessionid2detailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(
				
				new Function<Tuple2<String,Row>, Boolean>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;  
						return row.getString(10) != null ? true : false;
					}
					
				});
		
		JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(
							Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String payCategoryIds = row.getString(10);
						String[] payCategoryIdsSplited = payCategoryIds.split(",");  
						
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						
						for(String payCategoryId : payCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));  
						}
						
						return list;
					}
					
				});
		
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
				
				new Function2<Long, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
					
				});
		
		return payCategoryId2CountRDD;
	}
	
	/**
	 * 连接品类RDD与数据RDD
	 * @param categoryidRDD
	 * @param clickCategoryId2CountRDD
	 * @param orderCategoryId2CountRDD
	 * @param payCategoryId2CountRDD
	 * @return
	 */
	private static JavaPairRDD<Long, String> joinCategoryAndData(
			JavaPairRDD<Long, Long> categoryidRDD,
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
			JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
			JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
		// 解释一下，如果用leftOuterJoin，就可能出现，右边那个RDD中，join过来时，没有值
		// 所以Tuple中的第二个值用Optional<Long>类型，就代表，可能有值，可能没有值
		JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = 
				categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);
		
		JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(
				
				new PairFunction<Tuple2<Long,Tuple2<Long,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						Optional<Long> optional = tuple._2._2;
						long clickCount = 0L;
						
						if(optional.isPresent()) {
							clickCount = optional.get();
						}
						
						String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" + 
								Constants.FIELD_CLICK_COUNT + "=" + clickCount;
						
						return new Tuple2<Long, String>(categoryid, value);  
					}
					
				});
		
		tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(
				
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;
						
						Optional<Long> optional = tuple._2._2;
						long orderCount = 0L;
						
						if(optional.isPresent()) {
							orderCount = optional.get();
						}
						
						value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;  
						
						return new Tuple2<Long, String>(categoryid, value);  
					}
				
				});
		
		tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(
				
				new PairFunction<Tuple2<Long,Tuple2<String,Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;
						
						Optional<Long> optional = tuple._2._2;
						long payCount = 0L;
						
						if(optional.isPresent()) {
							payCount = optional.get();
						}
						
						value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;  
						
						return new Tuple2<Long, String>(categoryid, value);  
					}
				
				});
		
		return tmpMapRDD;
	}
	
	
	private static void calculateAndPersistAggrStat(String value,long taskid){
		long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));
		long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
		long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30m));
		
		long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_60));
		
		double visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s/session_count, 2);
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
				visit_length_4s_6s / session_count, 2);  
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
				visit_length_7s_9s / session_count, 2);  
		double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
				visit_length_10s_30s / session_count, 2);  
		double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
				visit_length_30s_60s / session_count, 2);  
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
				visit_length_1m_3m / session_count, 2);
		double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
				visit_length_3m_10m / session_count, 2);  
		double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
				visit_length_10m_30m / session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble(
				visit_length_30m / session_count, 2);  
		
		double step_length_1_3_ratio = NumberUtils.formatDouble(
				step_length_1_3 / session_count, 2);  
		double step_length_4_6_ratio = NumberUtils.formatDouble(
				step_length_4_6 / session_count, 2);  
		double step_length_7_9_ratio = NumberUtils.formatDouble(
				step_length_7_9 / session_count, 2);  
		double step_length_10_30_ratio = NumberUtils.formatDouble(
				step_length_10_30 / session_count, 2);  
		double step_length_30_60_ratio = NumberUtils.formatDouble(
				step_length_30_60 / session_count, 2);  
		double step_length_60_ratio = NumberUtils.formatDouble(
				step_length_60 / session_count, 2);  
		
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(session_count);  
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);  
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);  
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);  
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);  
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);  
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio); 
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);  
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio); 
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);  
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);  
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);  
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);  
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);  
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);  
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);  
		
		ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
		sessionAggrStatDAO.insert(sessionAggrStat);
	}
	
	/**
	 * 获取top10活跃session
	 * @param taskid
	 * @param sessionid2detailRDD
	 */
	private static void getTop10Session(
			JavaSparkContext sc,
			final long taskid,
			List<Tuple2<CategorySortKey, String>> top10CategoryList,
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		/**
		 * 第一步：将top10热门品类的id，生成一份RDD
		 */
		List<Tuple2<Long, Long>> top10CategoryIdList = 
				new ArrayList<Tuple2<Long, Long>>();
		
		for(Tuple2<CategorySortKey, String> category : top10CategoryList) {
			long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
					category._2, "\\|", Constants.FIELD_CATEGORY_ID));
			top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid, categoryid));  
		}
		
		JavaPairRDD<Long, Long> top10CategoryIdRDD = 
				sc.parallelizePairs(top10CategoryIdList);
		
		/**
		 * 第二步：计算top10品类被各session点击的次数
		 */
		JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD =
				sessionid2detailRDD.groupByKey();
		
		JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, String>> call(
							Tuple2<String, Iterable<Row>> tuple) throws Exception {
						String sessionid = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();
						
						Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
						
						// 计算出该session，对每个品类的点击次数
						while(iterator.hasNext()) {
							Row row = iterator.next();
							
							if(row.get(6) != null) {
								long categoryid = row.getLong(6);
								
								Long count = categoryCountMap.get(categoryid);
								if(count == null) {
									count = 0L;
								}
								
								count++;
								
								categoryCountMap.put(categoryid, count);
							}
						}
						
						// 返回结果，<categoryid,sessionid,count>格式
						List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
						
						for(Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
							long categoryid = categoryCountEntry.getKey();
							long count = categoryCountEntry.getValue();
							String value = sessionid + "," + count;
							list.add(new Tuple2<Long, String>(categoryid, value));  
						}
						
						return list;
					}
					
				}) ;
		
		// 获取到to10热门品类，被各个session点击的次数
		JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD
				.join(categoryid2sessionCountRDD)
				.mapToPair(new PairFunction<Tuple2<Long,Tuple2<Long,String>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(
							Tuple2<Long, Tuple2<Long, String>> tuple)
							throws Exception {
						return new Tuple2<Long, String>(tuple._1, tuple._2._2);
					}
					
				});
		
		/**
		 * 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
		 */
		JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
				top10CategorySessionCountRDD.groupByKey();
		
		JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
				
				new PairFlatMapFunction<Tuple2<Long,Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<String, String>> call(
							Tuple2<Long, Iterable<String>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						Iterator<String> iterator = tuple._2.iterator();
						
						// 定义取topn的排序数组
						String[] top10Sessions = new String[10];  
						
						while(iterator.hasNext()) {
							String sessionCount = iterator.next();
							long count = Long.valueOf(sessionCount.split(",")[1]);  
							
							// 遍历排序数组
							for(int i = 0; i < top10Sessions.length; i++) {
								// 如果当前i位，没有数据，那么直接将i位数据赋值为当前sessionCount
								if(top10Sessions[i] == null) {
									top10Sessions[i] = sessionCount;
									break;
								} else {
									long _count = Long.valueOf(top10Sessions[i].split(",")[1]);  
									
									// 如果sessionCount比i位的sessionCount要大
									if(count > _count) {
										// 从排序数组最后一位开始，到i位，所有数据往后挪一位
										for(int j = 9; j > i; j--) {
											top10Sessions[j] = top10Sessions[j - 1];
										}
										// 将i位赋值为sessionCount
										top10Sessions[i] = sessionCount;
										break;
									}
									
									// 比较小，继续外层for循环
								}
							}
						}
						
						// 将数据写入MySQL表
						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
						
						for(String sessionCount : top10Sessions) {
							String sessionid = sessionCount.split(",")[0];
							long count = Long.valueOf(sessionCount.split(",")[1]);  
							
							// 将top10 session插入MySQL表
							Top10Session top10Session = new Top10Session();
							top10Session.setTaskid(taskid);  
							top10Session.setCategoryid(categoryid);  
							top10Session.setSessionid(sessionid);  
							top10Session.setClickCount(count);  
							
							ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
							top10SessionDAO.insert(top10Session);  
							
							// 放入list
							list.add(new Tuple2<String, String>(sessionid, sessionid));
						}
						
						return list;
					}
					
				});
		
		/**
		 * 第四步：获取top10活跃session的明细数据，并写入MySQL
		 */
		JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
				top10SessionRDD.join(sessionid2detailRDD);  
		sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {  
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
				Row row = tuple._2._2;
				
				SessionDetail sessionDetail = new SessionDetail();
				sessionDetail.setTaskid(taskid);  
				sessionDetail.setUserid(row.getLong(1));  
				sessionDetail.setSessionid(row.getString(2));  
				sessionDetail.setPageid(row.getLong(3));  
				sessionDetail.setActionTime(row.getString(4));
				sessionDetail.setSearchKeyword(row.getString(5));  
				sessionDetail.setClickCategoryId(row.getLong(6));  
				sessionDetail.setClickProductId(row.getLong(7));   
				sessionDetail.setOrderCategoryIds(row.getString(8));  
				sessionDetail.setOrderProductIds(row.getString(9));  
				sessionDetail.setPayCategoryIds(row.getString(10)); 
				sessionDetail.setPayProductIds(row.getString(11));  
				
				ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
				sessionDetailDAO.insert(sessionDetail);  
			}
		});
	}
	
}
