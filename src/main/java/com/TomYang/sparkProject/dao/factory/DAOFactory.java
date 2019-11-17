package com.TomYang.sparkProject.dao.factory;


import com.TomYang.sparkProject.dao.IAdBlacklistDAO;
import com.TomYang.sparkProject.dao.IAdClickTrendDAO;
import com.TomYang.sparkProject.dao.IAdProvinceTop3DAO;
import com.TomYang.sparkProject.dao.IAdStatDAO;
import com.TomYang.sparkProject.dao.IAdUserClickCountDAO;
import com.TomYang.sparkProject.dao.IAreaTop3ProductDAO;
import com.TomYang.sparkProject.dao.IPageSplitConvertRateDAO;
import com.TomYang.sparkProject.dao.ISessionAggrStatDAO;
import com.TomYang.sparkProject.dao.ISessionDetailDAO;
import com.TomYang.sparkProject.dao.ISessionRandomExtractDAO;
import com.TomYang.sparkProject.dao.ITaskDAO;
import com.TomYang.sparkProject.dao.ITop10CategoryDAO;
import com.TomYang.sparkProject.dao.ITop10SessionDAO;
import com.TomYang.sparkProject.dao.impl.AdBlacklistDAOImpl;
import com.TomYang.sparkProject.dao.impl.AdClickTrendDAOImpl;
import com.TomYang.sparkProject.dao.impl.AdProvinceTop3DAOImpl;
import com.TomYang.sparkProject.dao.impl.AdStatDAOImpl;
import com.TomYang.sparkProject.dao.impl.AdUserClickCountDAOImpl;
import com.TomYang.sparkProject.dao.impl.AreaTop3ProductDAOImpl;
import com.TomYang.sparkProject.dao.impl.PageSplitConvertRateDAOImpl;
import com.TomYang.sparkProject.dao.impl.SessionAggrStatDAOImpl;
import com.TomYang.sparkProject.dao.impl.SessionDetailDAOImpl;
import com.TomYang.sparkProject.dao.impl.SessionRandomExtractDAOImpl;
import com.TomYang.sparkProject.dao.impl.TaskDAOImpl;
import com.TomYang.sparkProject.dao.impl.Top10CategoryDAOImpl;
import com.TomYang.sparkProject.dao.impl.Top10SessionDAOImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {


	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}
	
	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO() {
		return new Top10SessionDAOImpl();
	}
	
	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}
	
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
		return new AreaTop3ProductDAOImpl();
	}
	
	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOImpl();
	}
	
	public static IAdBlacklistDAO getAdBlacklistDAO() {
		return new AdBlacklistDAOImpl();
	}
	
	public static IAdStatDAO getAdStatDAO() {
		return new AdStatDAOImpl();
	}
	
	public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
		return new AdProvinceTop3DAOImpl();
	}
	
	public static IAdClickTrendDAO getAdClickTrendDAO() {
		return new AdClickTrendDAOImpl();
	}
	
}
