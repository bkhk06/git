import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FME_msg_test extends Thread {
	private static Log logger = LogFactory.getLog(FME_msg_test.class);
	private static int num_mfg = 0;
	private static int num_test = 0;

	private static Connection ct_mfg = null;
	private static PreparedStatement ps_mfg = null;
	private static ResultSet rs_mfg = null;

	private static Connection ct_test = null;
	private static PreparedStatement ps_test = null;
	private static ResultSet rs_test = null;

	// SQL statement
	private static String sql_mfg = null;
	private static String sql_test = null;
	private static String key_word = null;
	private static long time_interval = 0;

	public FME_msg_test() {

		try {

			InputStream is_db = Connection.class.getResourceAsStream("/config/db.properties");// db.properties
																								// 是一个用户配置文件传用户名密码
			Properties prop_db = new Properties();
			try {
				prop_db.load(is_db);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error(e.fillInStackTrace());
			}

			InputStream is_sql = Connection.class.getResourceAsStream("/config/sql.properties");// sql.properties
																								// 是一个用户配置文件传sql
			Properties prop_sql = new Properties();
			try {
				prop_sql.load(is_sql);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error(e.fillInStackTrace());
			}
			sql_mfg = prop_sql.getProperty("sql_mfg");
			sql_test = prop_sql.getProperty("sql_test");
			key_word = prop_sql.getProperty("key_word");
			String time_interval_s = prop_sql.getProperty("time_interval");

			/*String to int conversion*/ 
			try {
				time_interval = Integer.parseInt(time_interval_s);
				logger.info("Sleep time_interval: " + time_interval + " ms");
			} catch (NumberFormatException e) {
				e.printStackTrace();
				logger.error(e.fillInStackTrace());
			}

			// 加载驱动
			try {
				Class.forName("oracle.jdbc.driver.OracleDriver");
				logger.info("Oracle Database loaded successfully!");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				logger.error("Oracle Database driver loaded unsuccessfully!", e.fillInStackTrace());

			}
			// 返回连接成功信息

			try{
				ct_mfg = DriverManager.getConnection(prop_db.getProperty("url_mfg"), prop_db.getProperty("user_mfg"),prop_db.getProperty("password_mfg"));
				ct_test = DriverManager.getConnection(prop_db.getProperty("url_test"), prop_db.getProperty("user_test"),prop_db.getProperty("password_test"));
				logger.info("Oracle Database connection were created successfully"); // 返回连接成功信息
			} catch(SQLException e){
				logger.error("Oracle connection failed!", e.fillInStackTrace());
			}
			// 创建PreparedStatement
			ps_mfg = ct_mfg.prepareStatement(sql_mfg);
			// 创建PreparedStatement
			ps_test = ct_mfg.prepareStatement(sql_test);
			// 执行SQL
			
			//保存上一次查询报文结果，便于比较
			int tmp_mfg=0;
			int tmp_test=0;

			while (!isInterrupted()) {
				tmp_mfg=num_mfg;
				tmp_test=num_test;
				try {
					Thread.sleep(time_interval);//
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					logger.error(e.fillInStackTrace());
				}
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// 设置日期格式
				logger.info(" \n\n########################### "+df.format(new Date())+" FME msg quantity comparision between production and test databases: "+time_interval/1000+"s time interval");// new
																												// Date()为获取当前系统时间
				rs_mfg = ps_mfg.executeQuery();
				// 对获取的数据进行操作
				while (rs_mfg.next()) {
					logger.info("Stock quantity of production database: " + rs_mfg.getInt(key_word));
					num_mfg = rs_mfg.getInt(key_word);
				}
				

				rs_test = ps_test.executeQuery();
				// 对获取的数据进行操作
				while (rs_test.next()) {
					logger.info("Stock quantity of test database: " + rs_test.getInt(key_word));
					num_test = rs_test.getInt(key_word);
				}

				// 比较正式库和测试库报文增量
				logger.info("Stock D-Value of message  between  production and test databases: " + (num_mfg - num_test));
				if((num_mfg - num_test)!=0)
					logger.error("WARNING!!!! Stock message  between  production and test databases are different!!!");
				logger.info("Increment quantity of message for production database in "+time_interval/1000+"s: " + (num_mfg - tmp_mfg));
				logger.info("Increment quantity of message for test database in "+time_interval/1000+"s: " + (num_test -tmp_test ));
				logger.info("Increment D-Value of message comparision between  production and test databases: " + (num_mfg - tmp_mfg-(num_test -tmp_test)));
				if((((num_mfg - tmp_mfg)-(num_test -tmp_test))!=0))
					logger.error("WARNING!!!! Increment message  between  production and test databases are different!!!");
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error(e.fillInStackTrace());
		} finally {
			try {
				if (rs_mfg != null) {
					rs_mfg.close();
				}
				if (ps_mfg != null) {
					ps_mfg.close();
				}
				if (ct_mfg != null) {
					ct_mfg.close();
				}
				if (rs_test != null) {
					rs_test.close();
				}
				if (ps_test != null) {
					ps_test.close();
				}
				if (ct_test != null) {
					ct_test.close();
				}
				logger.info("Connection,ResultSet,Statement  were closed in sucess!");
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error(e);
				logger.error(e.fillInStackTrace());
			}
		}
	}

	public static void main(String[] args) {
		new FME_msg_test();
	}
}