
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.json.JSONObject;

class WorkerThread implements Runnable {
	private int eventId;
	
	public WorkerThread(int eventId) {
		this.eventId = eventId;
	}

	public void run() {
		try {
			ScheduleDAO sdc= new ScheduleDAO();
			sdc.updateEventStatus(eventId);
			
	} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		finally {
			if (ScheduleDAO.con != null) {
//				try {
//			//		ScheduleDAO.con.close();  Close later
//				} catch (SQLException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
				}
		System.out.println(Thread.currentThread().getName() + " (End)");// prints thread name
	}

	private void processmessage() {
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}