import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;



public class DBHelper {

	static Connection con;
	 public String getPlayerId(String email) throws SQLException, ClassNotFoundException
		{
			//JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 }
		 Connection	con = JDBCConnection.getOracleConnection(); 
			PreparedStatement pstmt = null;
			String playerId="";
			if(con!=null)
			{
					String query="select extra_info1 from wh_user where user_id=?";
					 pstmt=con.prepareStatement(query);
					 pstmt.setString(1,email);
					 ResultSet rs= pstmt.executeQuery();
					 while(rs.next())
					 {
						 playerId=rs.getString(1);
					 }
					 
					
			}
			return playerId;
			}

	 public String getDeviceName(String controllerId,int deviceId) throws SQLException, ClassNotFoundException
		{
			//JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
			PreparedStatement pstmt = null;
			String deviceName="";
			if(con!=null)
			{
					String query="select device_name from wh_device where device_id="+deviceId+ " and controller_id='"+controllerId+"'";
					 pstmt=con.prepareStatement(query);
					 
					 ResultSet rs= pstmt.executeQuery();
					 while(rs.next())
					 {
						 deviceName=rs.getString(1);
					 }
					 
					
			}
			return deviceName;
			}

	 
	 public void createBlockchainTx(String txId,String status, int blockChainOrderId) throws SQLException, ClassNotFoundException
		{
		try {	
		 //JDBCConnection connref =new JDBCConnection();
		 if (ScheduleDAO.con == null ) {
				con = JDBCConnection.getOracleConnection();
		 } 
			PreparedStatement pstmt = null;
			String deviceName="";
			if(ScheduleDAO.con!=null)
			{
					String query="insert into all_blockchain_transactions(blockchain_trx_id,transaction_type,blockchain_order_id) values(?,?,?)";
					  pstmt=ScheduleDAO.con.prepareStatement(query);
					  pstmt.setString(1,txId);
					  pstmt.setString(2,status);
					  pstmt.setInt(3,blockChainOrderId); 
					 
					  pstmt.execute();
					 
					 
					
			}
			}
		
	 catch(Exception e) {
		 e.printStackTrace();
	 }

		}
	 
}
