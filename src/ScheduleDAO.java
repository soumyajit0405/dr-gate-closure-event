

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;


public class ScheduleDAO {

	static Connection con;
	Statement stmt = null;
	PreparedStatement pStmt = null;
	 public ArrayList<HashMap<String,Object>> getEvents(String date, String time) throws SQLException, ClassNotFoundException
	 {
		 ArrayList<HashMap<String,Object>> al=new ArrayList<>();
	try {
		 PreparedStatement pstmt = null;
		  time=time+":00";
		 
		//JDBCConnection connref =new JDBCConnection();
		 if (con == null ) {
				con = JDBCConnection.getOracleConnection();
		 }
		//	System.out.println("select aso.sell_order_id,ubc.private_key,ubc.public_key,abc.order_id from all_sell_orders aso,all_blockchain_orders abc, user_blockchain_keys ubc where aso.transfer_start_ts ='"+date+" "+time+"' and abc.general_order_id=aso.sell_order_id and abc.order_type='SELL_ORDER' and ubc.user_id  = aso.seller_id and aso.order_status_id=1");
		 String query="select a.event_id from all_events a where a.event_status_id= 2 and a.event_start_time ='"+date+" "+time+"'";
		 // String query="select a.event_id from all_events a where a.event_status_id= 2 and a.event_start_time ='2020-06-21 15:30:00'";
		 
//		 String query="select a.event_id from all_events a, event_customer_mapping b where a.event_id= \n" + 
//				 "b.event_id and (b.event_customer_status_id = 3 || b.event_customer_status_id = 4) and a.event_status_id= 2 and a.event_start_time ='"+date+" "+time+"'";
//			String query="select a.event_id from all_events a, event_customer_mapping b where a.event_id= \n" + 
//					"b.event_id and (b.event_customer_status_id = 3 || b.event_customer_status_id = 4)and a.event_status_id= 2 and a.event_start_time ='2020-06-19 21:00:00'";
		 	System.out.println(query);
		 pstmt=con.prepareStatement(query);
		// pstmt.setString(1,controllerId);
		 ResultSet rs= pstmt.executeQuery();
		
		 while(rs.next())
		 {
			 HashMap<String,Object> data=new HashMap<>();
			 data.put("eventId",(rs.getInt("event_id")));
			 al.add(data);
			// initiateActions(rs.getString("user_id"),rs.getString("status"),rs.getString("controller_id"),rs.getInt("device_id"),"Timer");
			//topic=rs.getString(1);
		 }
		 // updateEventsManually(date, time);
		 updateEventsToExpire(date, time);
	} catch(Exception e) {
		e.printStackTrace();
	}
		return  al;
	 }
	 
	 public void updateEventStatus(int eventId) throws SQLException, ClassNotFoundException
	 {
		 PreparedStatement pstmt = null;
		//JDBCConnection connref =new JDBCConnection();
		 if (con == null ) {
				con = JDBCConnection.getOracleConnection();
		 }
		 String query="update all_events set event_status_id=5 where event_id =?";
		 pstmt=con.prepareStatement(query);
		pstmt.setInt(1,eventId);
		 pstmt.executeUpdate();
		 
		 query="update event_customer_mapping set event_customer_status_id=5 where event_id =? and event_customer_status_id=3";
		 pstmt=con.prepareStatement(query);
		pstmt.setInt(1,eventId);
		 pstmt.executeUpdate();
		 
		 query="update event_customer_mapping set event_customer_status_id=14 where event_id =? and event_customer_status_id=2";
		 pstmt=con.prepareStatement(query);
		pstmt.setInt(1,eventId);
		 pstmt.executeUpdate();
		 
	 }
	 
	 public void updateEventsManually(String date, String time) throws SQLException, ClassNotFoundException
	 {
		 PreparedStatement pstmt = null;
		//JDBCConnection connref =new JDBCConnection();
		 if (con == null ) {
				con = JDBCConnection.getOracleConnection();
		 }
//		 String query="select all_events.event_id,event_customer_mapping.customer_id from all_events,event_customer_mapping where all_events.event_id not in (select a.event_id from all_events a, event_customer_mapping b where a.event_id= \n" + 
//					"b.event_id and (b.event_customer_status_id =3 || b.event_customer_status_id =4) and a.event_status_id= 2  and a.event_start_time ='"+date+" "+time+"') and all_events.event_status_id= 2  and all_events.event_start_time ='2020-06-19 21:00:00' and all_events.event_id=event_customer_mapping.event_id";
		 String query="select all_events.event_id,event_customer_mapping.customer_id from all_events,event_customer_mapping where all_events.event_id not in (select a.event_id from all_events a, event_customer_mapping b where a.event_id= \n" + 
					"b.event_id and (b.event_customer_status_id =3 || b.event_customer_status_id =4) and a.event_status_id= 2  and a.event_start_time ='"+date+" "+time+"') and all_events.event_status_id= 2  and all_events.event_start_time ='"+date+" "+time+"'' and all_events.event_id=event_customer_mapping.event_id";
		 
//		 String query="select a.event_id from all_events a, event_customer_mapping b where a.event_id= \n" + 
//					"b.event_id and b.event_customer_status_id <> 3 and a.event_status_id= 2 and  a.event_start_time ='2020-06-08 21:00:00'";
		 pstmt=con.prepareStatement(query);
		//pstmt.setInt(1,eventId);
		 ResultSet rs= pstmt.executeQuery();
		 ArrayList<HashMap<String,Object>> al=new ArrayList<>();
		 while(rs.next())
		 {
			 HashMap<String,Object> data=new HashMap<>();
			 data.put("eventId",(rs.getInt("event_id")));
			 data.put("customerId",(rs.getInt("customer_id")));
			 al.add(data);
			// initiateActions(rs.getString("user_id"),rs.getString("status"),rs.getString("controller_id"),rs.getInt("device_id"),"Timer");
			//topic=rs.getString(1);
		 }
		for (int i =0;i <al.size();i++) {
			query="update all_events set event_status_id=6 where event_id =?";
			pstmt=con.prepareStatement(query);
			pstmt.setInt(1,(int)(al.get(i).get("eventId")));
			 pstmt.executeUpdate();
			 
			 query="update event_customer_mapping set event_customer_status_id=14 where event_id =? and customer_id=?";
				pstmt=con.prepareStatement(query);
				pstmt.setInt(1,(int)(al.get(i).get("eventId")));
				pstmt.setInt(2,(int)(al.get(i).get("customerId")));
				 pstmt.executeUpdate();
			
		}
		 
	 }
	 
	 
	 public void updateEventsToExpire(String date, String time) throws SQLException, ClassNotFoundException
	 {
		 PreparedStatement pstmt = null;
		//JDBCConnection connref =new JDBCConnection();
		 if (con == null ) {
				con = JDBCConnection.getOracleConnection();
		 }
		String query="update all_events set event_status_id=6 where event_status_id =1 and event_start_time ='"+date+" "+time+"'";
			pstmt=con.prepareStatement(query);
			 pstmt.executeUpdate();
			  
	 }
	 
}
