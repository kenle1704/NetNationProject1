import java.util.*;
import java.io.*;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.XMLFormatter;

import java.sql.*;
public class Partner_Report_Importer {
	private static final String logPath="log/Partner_Report_Importer.log";
	private static final String className="Partner_Report_Importer";
	private static final String ignorePartnerIDs="26392"; // comma delimited for ignore list 
	private static final String csvFilePath ="resource/Sample_Report.csv";
	private static final String jsonFilePath ="resource/typemap.json";
	public static void main(String[] args) {
	    //assume that first args is input file name 
	    //second args is input map 
		HashMap<String,String[]> conds = new HashMap<String,String[]>();
		HashMap<String,HashMap<String,String>> mapValues = new HashMap<String,HashMap<String,String>>();
		HashMap<String,HashMap<String,String>> mapUsages = new HashMap<String,HashMap<String,String>>();
		HashMap<String,String> unitReduction = new HashMap<String,String>();
		String isVarcharValues = "A_product,A_partnerpurchasedplanid,A_plan,B_partnerpurchasedplanid,B_domain";
		List<String> input = read_csv_file(csvFilePath);
                HashMap<String,String> json = read_json_file(jsonFilePath);
		HashMap<String,String> mapHeadersCharable = new HashMap<>();
		HashMap<String,String> mapHeadersDomains = new HashMap<>();
		// TODO 
		conds.put("partnumber",new String[]{"==","","NO PartNumber ::: " });
		conds.put("itemcount",new String[]{"<","1","Item Count Negative ::: " });//non positive 
		conds.put("partnerid",new String[]{"contains",ignorePartnerIDs,"Item contains PartnerID ::: "});
		conds.put("accountguid",new String[]{"maxlength","32",""});
		mapValues.put("partnerid",json); 
		mapHeadersCharable.put("partnumber","A_product");
		mapHeadersCharable.put("partnerid","A_partnerid");
		mapHeadersCharable.put("accountguid","A_partnerpurchasedplanid");
		mapHeadersCharable.put("plan","A_plan");
		mapHeadersCharable.put("itemcount","A_usage"); // v 
		mapHeadersDomains.put("accountguid","B_partnerpurchasedplanid");
		mapHeadersDomains.put("domains","B_domain");
		unitReduction.put("partnerid-EA000001GB0O","1000");
		unitReduction.put("partnerid-PMQ00005GB0R","5000");
		unitReduction.put("partnerid-SSX006NR","1000");
		unitReduction.put("partnerid-SPQ00001MB0R","2000");
		mapUsages.put("itemcount",unitReduction);
		List<HashMap<String,String>> filteredDataChargeable = filterContent(input,conds);
		List<HashMap<String,String>> filteredDataDomains = filterContent(input,new HashMap<String,String[]>());
		List<HashMap<String,String>> mappedChargeable = mapTable(mapHeadersCharable,mapValues,mapUsages,filteredDataChargeable);
		List<HashMap<String,String>> mappedDomains = mapTable(mapHeadersDomains,mapValues,mapUsages,filteredDataDomains);
		// insert to A_chargeable
		Connection conn = getConnection();
		String queries="";
		// get total itemCount;
		int itemCountSum=0;
		for(int i=0;i<filteredDataChargeable.size();i++){
			HashMap<String,String> row = filteredDataChargeable.get(i);
			int ic = Integer.parseInt(row.get("itemcount"));
			itemCountSum++;
		}
		System.out.println("Total Item Count : " + itemCountSum);
		for(int i=0; i<mappedChargeable.size();i++){
			HashMap<String,String> row = mappedChargeable.get(i);
			String query = insertQuery("A_chargeable",row,isVarcharValues);
			System.out.println(query);
			queries += query;
		}
		HashMap<String,Integer> executed = new HashMap<String,Integer>();
		for(int i=0; i<mappedDomains.size();i++){
                        HashMap<String,String> row = mappedDomains.get(i);
			String domain = row.get("B_domain");
			if( executed.get(domain) == null) {
				executed.put(domain,1);
				String query = insertQuery("B_domains",row,isVarcharValues);
                        	System.out.println(query);
				queries += query;
			}
                }
		executeQuery(queries);
		closeConnection();
	}
	public static List<HashMap<String,String>> mapTable(HashMap<String,String> mapHeaders,HashMap<String,HashMap<String,String>> mapValues,HashMap<String,HashMap<String,String>> mapUsages,List<HashMap<String,String>> input ){
		List<HashMap<String,String>> output = new ArrayList<>();
		for( HashMap<String,String> row : input ) {
			HashMap<String,String> mapped = new HashMap<String,String>();
			for( String key : row.keySet() ) {
				String value = row.get(key);
				if( mapValues.get(key) != null ) {
					HashMap<String,String> mapValue = mapValues.get(key);
					if( mapValue.get(value) != null ) value = mapValue.get(value);
				}
				if( mapUsages.get(key) != null ) {
					HashMap<String,String> unitReduction = mapUsages.get(key);
					for(String _key : unitReduction.keySet() ){
						String[] idValues = _key.split("-");
						String id = idValues[0];
						String val = idValues[1];
						if( val == row.get(id) ) {
							value = "" + ( Integer.parseInt(value) / Integer.parseInt(unitReduction.get(_key)));
						}
					}
				}
				if( mapHeaders.get(key) != null ) {
					mapped.put(mapHeaders.get(key),value);
				}
			}
			if( mapped.size() > 0 ) output.add(mapped);
		}
		return output;

	}
    	public static List<String> read_csv_file(String filepath){
                List<String> csv = new ArrayList<>();
                Scanner sc;
                try {
                        sc = new Scanner(new File(filepath));
                        while (sc.hasNext())  //returns a boolean value
                        {
                                csv.add(sc.next());
                        }
                        sc.close();  //closes the scanner
                } catch (FileNotFoundException e ) {
                        e.printStackTrace();
                }
                return csv;
        }
        // assume that json file only have one object
        // we can use generic type depend on the input data
        public static HashMap<String,String> read_json_file(String filepath){
                HashMap<String,String> map = new HashMap<String,String>();
                try{
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(filepath));
                        Gson gson = new Gson();
                        Object json = gson.fromJson(bufferedReader, Object.class);
                        map = new Gson().fromJson(
                                json.toString(), new TypeToken<HashMap<String, String>>() {}.getType()
                        );
                } catch (FileNotFoundException e ) {
                        e.printStackTrace();
                }
                return map;
        }
	public static List<HashMap<String,String>> filterContent(List<String> input,HashMap<String,String[]> conds){
                List<HashMap<String,String>> output = new ArrayList<>();
                List<String> loggers=new ArrayList<>();
                String firstLine = (  input.size() > 0 ) ? input.get(0) : ""; // get header list
                String [] header = firstLine.split(",");
                for(int i =1;i< input.size();i++)  //returns a boolean value
                {
                        String line = input.get(i);
                        String [] values = line.split(",");
                        HashMap<String,String> mapColumns = new HashMap<String,String>();
                        Boolean isSkip = false;
                        for( int j =0; j < header.length;j++){
				if( j >= values.length ) break;
				String[] cond = conds.get(header[j].toLowerCase());
				String value = values[j];
				if( cond != null && cond.length > 0 ) {
                                               //TODO
                                         String operator = cond[0];
                                         String val = cond[1];
                                         String reason = cond[2];
                                         switch (operator ) {
                                                case "<":
                                                      if( Integer.parseInt(values[j]) < Integer.parseInt(val) ) isSkip = true;
                                                      break;
                                                case ">":
                                                      if( Integer.parseInt(values[j]) > Integer.parseInt(val) ) isSkip = true;
                                                      break;
                                                case "==":
                                                      if( values[j].equals(val) ) isSkip = true;
                                                      break;
                                                case "contains":
                                                      String[] skipList = val.split(",");
                                                      List<String> _skipList = Arrays.asList(skipList);
                                                      if( _skipList.contains(values[j]) ) isSkip = true;
                                                      break;
						case "maxlength":
			   			      value = value.substring(0,Math.min(value.length(),Integer.parseInt(val)));
						      break;
                                                default:
                                                      break;
                                         }
                                         if(isSkip) {
                                               line = reason + line;
					}
				}
				mapColumns.put(header[j].toLowerCase(),value);

                        }
                        if ( isSkip ) loggers.add(line+"\n");
                        else if( mapColumns.size() > 0 ) output.add(mapColumns);
			else System.out.println(line);
                }
                if( loggers.size() > 0 ) logger(loggers.toString());
                return output;
        }
	public static void logger(String log){
		Logger logger = Logger.getLogger(className);
		FileHandler fh;
		try {
			fh = new FileHandler(logPath,true);
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);
			logger.info(log);

		} catch(IOException e) {
			e.printStackTrace();
		}
	
	}
	// query builder
	public static String insertQuery(String table,HashMap<String,String> inputs,String isVarcharValues){
                // insert data
                LinkedList<String> columns = new LinkedList<String>();
                LinkedList<String> values = new LinkedList<String>();
                for (String key : inputs.keySet()) {
                        String val = inputs.get(key);
                        if( val != "" ) {
                                columns.add(key);
				if( isVarcharValues.indexOf(key) > -1 ) val = "'"+val+"'";
                                values.add(val);
                        }
                }
                return "INSERT INTO " + table + "( " + String.join(", ", columns) + " ) VALUES (" + String.join(", ", values) + " );";
        }
	private static String url = "jdbc:mysql://localhost/netnation?allowMultiQueries=true";
        private static String driverName = "com.mysql.cj.jdbc.Driver";
        private static String username = "user";
        private static String password = "password";
        private static Connection con;

        public static Connection getConnection() {
                try {
                        Class.forName(driverName);
                        try {
                                con = DriverManager.getConnection(url, username, password);
                        } catch (SQLException ex) {
                        // log an exception. fro example:
                        System.out.println("Failed to create the database connection.");
                        }
                } catch (ClassNotFoundException ex) {
                        // log an exception. for example:
                        System.out.println("Driver not found.");
                }
                return con;
        }
        public static void closeConnection(){
                if (con != null) {
                	try {
                        	con.close();
                        } catch (SQLException e) {
				System.out.println(e.getSQLState());
			}
                }
        }
        public static void executeQuery(String query){
                if (con != null) {
                        try {
                                Statement st = con.createStatement();
                                st.executeUpdate(query);
                                st.close();
                        } catch(SQLException e){
                                System.out.println(e.getSQLState());
                        }
                } else {
                        System.out.println("No Database Connection.");
                }
        }
}
