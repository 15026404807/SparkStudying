package com.TomYang.sparkProject.spark.session;

import org.apache.spark.AccumulatorParam;
import com.TomYang.sparkProject.constant.Constants;
import com.TomYang.sparkProject.util.StringUtils;

public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

	private static final long serialVersionUID = 6311074555136039130L;
	
	@Override
	  /**
	   * 初始值
	   * Return the "zero" (identity) value for an accumulator type, given its initial value. For
	   * example, if R was a vector of N dimensions, this would return a vector of N zeroes.
	   */
	public String zero(String v) {
		return Constants.SESSION_COUNT + "=0|"
				+ Constants.TIME_PERIOD_1s_3s + "=0|"
				+ Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|"
				+ Constants.STEP_PERIOD_1_3 + "=0|"
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0";
	}
	
	//合并两个累加器的值
	@Override
	public String addInPlace(String v1, String v2) {
		return add(v1, v2);
	}
	
	//向累加器中添加值 
	@Override
	public String addAccumulator(String v1, String v2) {
		return add(v1, v2);
	}  
	
	/**
	 * session统计计算逻辑
	 * @param v1 连接串
	 * @param v2 范围区间
	 * 	/**
	 * addInPlace和addAccumulator
	 * 可以理解为是一样的
	 * 
	 * 这两个方法，其实主要就是实现，v1可能就是我们初始化的那个连接串
	 * v2，就是我们在遍历session的时候，判断出某个session对应的区间，然后会用Constants.TIME_PERIOD_1s_3s
	 * 所以，我们，要做的事情就是
	 * 在v1中，找到v2对应的value，累加1，然后再更新回连接串里面去
	 * 
	 * @return 更新以后的连接串
	 */
	private String add(String v1, String v2) {
		if(StringUtils.isEmpty(v1)){
			return v2;
		}
		
		String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
		
		if(oldValue != null){
			int newValue = Integer.valueOf(oldValue) + 1;
			return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
		}
		
		return v1;
	}
	
}
