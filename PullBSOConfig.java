
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ResourceBundle;

public class PullBSOConfig {

	/**
	 * @param args
	 */
	static ResourceBundle resource = ResourceBundle.getBundle("Queries");

	public Connection getConnection()throws Exception{

		Connection con = null;
		try {

			String driverName = getProperty("DRIVERNAME");
			Class.forName(driverName);
			String serverName = getProperty("PROVSERVERNAME");
			String portNumber = getProperty("PROVPORTNUMBER");
			String sid = getProperty("PROVSID");
			String url = "jdbc:oracle:thin:@" + serverName + ":" + portNumber
					+ ":" + sid;
		//	logger.debug("The Database URL is " + url);
			String username = getProperty("PROVUSERNAME");
			String password = getProperty("PROVPASSWORD");
		//	logger.debug(url);

			con = DriverManager.getConnection(url, username, password);

		} catch (Exception sqlEx) {
			throw sqlEx;

		}
		return con;

	}



public static String getProperty(String key) throws Exception {
	String property = null;
	//logger.debug("In the getProperty method");
	try {
		//logger.debug("Reading key " + key);
		property = resource.getString(key);
		//logger.debug(" Reading key = "+key+"Value of the key = " + property);
	} catch (Exception e) {

		throw e;
	}

	return property;

}

public void generateFile(String sql,String fileName,String strColumn) throws Exception{
	Connection con = null;
	Statement stmt = null;
	String query = null;
	ResultSet rslt = null;
	int columns =0 ;
	String appKeyName = null;

	try{

		query = sql;
		con = getConnection();
		//System.out.println("1");
		stmt = con.createStatement();
		//System.out.println("2");
        rslt = stmt.executeQuery(query.toString());
    	//System.out.println("3");

        BufferedWriter out = new BufferedWriter(new FileWriter("sle/"+fileName+".sql"));
        columns  = Integer.parseInt(strColumn);

        //System.out.println(query);
        while (rslt!=null && rslt.next()) {
			String strInsert = "insert into "+fileName+" values ('";
			out.write(strInsert);
        	//System.out.println("1");
        	for(int i = 1; i <= columns; i++){
        		//System.out.println("2");
        		appKeyName = rslt.getString(i);
                //System.out.println("  The Appkey name obtained from resultset ===>  " + appKeyName);

                if(appKeyName != null){
                	out.write(appKeyName);
                }else {
                	appKeyName = "null";

                }

				if(i<columns)
                	out.write("','");
                else
                	out.write("');");
        	}// end of for
        	   out.write("\n");
        }// end of while

        out.close();



	}catch (Exception e){

		System.out.println("Exception"+e);

	}



}


	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {

			PullBSOConfig sap = new PullBSOConfig();
			sap.generateFile((String)getProperty("BSO_ATTRIBUTE_TO_XML"),"BSO_ATTRIBUTE_TO_XML",(String)getProperty("BSO_ATTRIBUTE_TO_XML_COLUMNS"));
			sap.generateFile((String)getProperty("BSO_KEY"),"BSO_KEY",(String)getProperty("BSO_KEY_COLUMNS"));
			sap.generateFile((String)getProperty("BSO_MANDPARAM"),"BSO_MANDPARAM",(String)getProperty("BSO_MANDPARAM_COLUMNS"));
			sap.generateFile((String)getProperty("BSO_TO_MANDATORY_SERVICE"),"BSO_TO_MANDATORY_SERVICE",(String)getProperty("BSO_TO_MANDATORY_SERVICE_COLUMNS"));
			sap.generateFile((String)getProperty("BSO_UPSTREAM_ATTRIBUTES"),"BSO_UPSTREAM_ATTRIBUTES",(String)getProperty("BSO_UPSTREAM_ATTRIBUTES_COLUMNS"));
			sap.generateFile((String)getProperty("BSO_PROFILE"),"BSO_PROFILE",(String)getProperty("BSO_PROFILE_COLUMNS"));
			sap.generateFile((String)getProperty("BSO_VALIDATION"),"BSO_VALIDATION",(String)getProperty("BSO_VALIDATION_COLUMNS"));
			sap.generateFile((String)getProperty("BSO_TASKORDER"),"BSO_TASKORDER",(String)getProperty("BSO_TASKORDER_COLUMNS"));
			sap.generateFile((String)getProperty("SPML3DS_MAPPING"),"SPML3DS_MAPPING",(String)getProperty("SPML3DS_MAPPING_COLUMNS"));
		}catch (Exception e){
		System.out.println("Exception"+e);
		}

	}

}
