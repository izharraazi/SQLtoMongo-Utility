
package migrate;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
/**
 * @author IzharR
 * * 09-Mar-2016 
 */


public class Zlogmigration {

	/**
	 * @param args
	 */
	
	private static final String QUERY = "SELECT * FROM ";
	
	public static JSONArray getSQLdata(String p_tablename ){
		System.out.println("---------------------------------------------"+p_tablename+"--------------------------------------------------");
		Connection con = DBConnection.getConnection();
		try(
				PreparedStatement stmt = con.prepareStatement(QUERY+p_tablename);
				ResultSet rs = stmt.executeQuery()) {	
			JSONArray result = new JSONArray();
			while(rs.next()){
				result = convert(rs);
				rs.close();
				stmt.close();
				return result;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			if (con != null) {
		        try {
		            con.close();
		        } catch (SQLException e) { /* ignored */}
		    }
		}
		return null;
		
	}
	
	public static JSONArray convert(ResultSet rs) throws SQLException,
    JSONException {
JSONArray json = new JSONArray();
ResultSetMetaData rsmd = rs.getMetaData();
int numColumns = rsmd.getColumnCount();
while (rs.next()) {

    JSONObject obj = new JSONObject();

    for (int i = 1; i < numColumns + 1; i++) {
        String column_name = rsmd.getColumnName(i);

        if (rsmd.getColumnType(i) == java.sql.Types.ARRAY && (!rs.getArray(column_name).equals("N/A"))) {
            obj.put(column_name, rs.getArray(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.BIGINT && (!rs.getArray(column_name).equals("N/A"))) {
            obj.put(column_name, rs.getLong(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.REAL && (!rs.getArray(column_name).equals("N/A"))) {
            obj.put(column_name, rs.getFloat(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.BOOLEAN && (!rs.getArray(column_name).equals("N/A"))) {
            obj.put(column_name, rs.getBoolean(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.BLOB && (!rs.getArray(column_name).equals("N/A"))) {
            obj.put(column_name, rs.getBlob(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE && (!rs.getArray(column_name).equals("N/A"))) {
            obj.put(column_name, rs.getDouble(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.FLOAT && (!rs.getArray(column_name).equals("N/A"))) {
            obj.put(column_name, rs.getDouble(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.INTEGER ) {
            obj.put(column_name, rs.getInt(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.NVARCHAR && (!rs.getString(column_name).equals("N/A"))) {
            obj.put(column_name, rs.getNString(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR && (!("N/A".equals(rs.getString(column_name))))) {
            obj.put(column_name, rs.getString(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.CHAR && (!("N/A".equals(rs.getString(column_name))))) {
            obj.put(column_name, rs.getString(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.NCHAR &&(!("N/A".equals(rs.getString(column_name))))) {
            obj.put(column_name, rs.getNString(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.LONGNVARCHAR && (!("N/A".equals(rs.getString(column_name))))) {
            obj.put(column_name, rs.getNString(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.LONGVARCHAR && (!("N/A".equals(rs.getString(column_name))))) {
            obj.put(column_name, rs.getString(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.TINYINT ) {
            obj.put(column_name, rs.getByte(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.SMALLINT ) {
            obj.put(column_name, rs.getShort(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.DATE ) {
            obj.put(column_name, rs.getDate(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.TIME ) {
            obj.put(column_name, rs.getTime(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.TIMESTAMP) {
            obj.put(column_name, rs.getTimestamp(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.BINARY ) {
            obj.put(column_name, rs.getBytes(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.VARBINARY ) {
            obj.put(column_name, rs.getBytes(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.LONGVARBINARY ) {
            obj.put(column_name, rs.getBinaryStream(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.BIT ) {
            obj.put(column_name, rs.getBoolean(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.CLOB ) {
            obj.put(column_name, rs.getClob(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.NUMERIC ) {
            obj.put(column_name, rs.getBigDecimal(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.DECIMAL ) {
            obj.put(column_name, rs.getBigDecimal(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.DATALINK ) {
            obj.put(column_name, rs.getURL(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.REF ) {
            obj.put(column_name, rs.getRef(column_name));
        } else if (rsmd.getColumnType(i) == java.sql.Types.STRUCT ) {
            obj.put(column_name, rs.getObject(column_name)); // must be a custom mapping consists of a class that implements the interface SQLData and an entry in a java.util.Map object.
        } else if (rsmd.getColumnType(i) == java.sql.Types.DISTINCT ) {
            obj.put(column_name, rs.getObject(column_name)); // must be a custom mapping consists of a class that implements the interface SQLData and an entry in a java.util.Map object.
        } else if (rsmd.getColumnType(i) == java.sql.Types.JAVA_OBJECT ) {
            obj.put(column_name, rs.getObject(column_name));
        }
    }

    json.put(obj);
}

return json;
}
	
	public static void main(String[] args) throws IOException {
		Integer count=0;
		Long start = System.currentTimeMillis();
		String str ="zlog_140925019";
		//		"zlog_140927037,zlog_140927036,zlog_140927035,zlog_140927034,zlog_140927033,zlog_140927032,zlog_140927031,zlog_140927030,zlog_140927029,zlog_140927028,zlog_140927027,zlog_140927026,zlog_140927025,zlog_140927024,zlog_140927023,zlog_140927022,zlog_140927021,zlog_140927020,zlog_140927019,zlog_140927018,zlog_140927017,zlog_140927016,zlog_140927015,zlog_140927014,zlog_140927013,zlog_140927012,zlog_140927011,zlog_140927010,zlog_140927009,zlog_140927008,zlog_140927007,zlog_140927006,zlog_140927005,zlog_140927004,zlog_140927003,zlog_140927002,zlog_140927001,zlog_140926033,zlog_140926032,zlog_140926031,zlog_140926030,zlog_140926029,zlog_140926028,zlog_140926027,zlog_140926026,zlog_140926025,zlog_140926024,zlog_140926023,zlog_140926022,zlog_140926021,zlog_140926020,zlog_140926019,zlog_140926018,zlog_140926017,zlog_140926016,zlog_140926015,zlog_140926014,zlog_140926013,zlog_140926012,zlog_140926011,zlog_140926010,zlog_140926009,zlog_140926008,zlog_140926007,zlog_140926006,zlog_140926005,zlog_140926004,zlog_140926003,zlog_140926002,zlog_140926001,zlog_140925054,zlog_140925053,zlog_140925052,zlog_140925051,zlog_140925050,zlog_140925049,zlog_140925048,zlog_140925047,zlog_140925046,zlog_140925045,zlog_140925044,zlog_140925043,zlog_140925042,zlog_140925041,zlog_140925040,zlog_140925039,zlog_140925038,zlog_140925037,zlog_140925036,zlog_140925035,zlog_140925034,zlog_140925033,zlog_140925032,zlog_140925031,zlog_140925030,zlog_140925029,zlog_140925028,zlog_140925027,zlog_140925026,zlog_140925025,zlog_140925024,zlog_140925023,zlog_140925022,zlog_140925021,zlog_140925020,zlog_140925019,zlog_140925018,zlog_140925017,zlog_140925016,zlog_140925015,zlog_140925014,zlog_140925013,zlog_140925012,zlog_140925011,zlog_140925010,zlog_140925009,zlog_140925008,zlog_140925007,zlog_140925006,zlog_140923001,zlog_140822035,zlog_140815031,zlog_140815029,zlog_140815028,zlog_140808027,zlog_140808026,zlog_140808024,zlog_140722019,zlog_140606011,zlog_140526005,zlog_140506003,zlog_000000087641,zlog_000000084d6c,zlog_000000082710,zlog_000000081111,zlog_000000";
	//	String str = "zlog_140927062,zlog_140927061,zlog_140927060,zlog_140927059,zlog_140927058,zlog_140927057,zlog_140927056,zlog_140927055,zlog_140927054,zlog_140927053,zlog_140927052,zlog_140927051,zlog_140927050,zlog_140927049,zlog_140927048,zlog_140927047,zlog_140927046,zlog_140927045,zlog_140927044,zlog_140927043,zlog_140927042,zlog_140927041,zlog_140927040,zlog_140927039,zlog_140927038,zlog_140927037,zlog_140927036,zlog_140927035,zlog_140927034,zlog_140927033,zlog_140927032,zlog_140927031,zlog_140927030,zlog_140927029,zlog_140927028,zlog_140927027,zlog_140927026,zlog_140927025,zlog_140927024,zlog_140927023,zlog_140927022,zlog_140927021,zlog_140927020,zlog_140927019,zlog_140927018,zlog_140927017,zlog_140927016,zlog_140927015,zlog_140927014,zlog_140927013,zlog_140927012,zlog_140927011,zlog_140927010,zlog_140927009,zlog_140927008,zlog_140927007,zlog_140927006,zlog_140927005,zlog_140927004,zlog_140927003,zlog_140927002,zlog_140927001,zlog_140926033,zlog_140926032,zlog_140926031,zlog_140926030,zlog_140926029,zlog_140926028,zlog_140926027,zlog_140926026,zlog_140926025,zlog_140926024,zlog_140926023,zlog_140926022,zlog_140926021,zlog_140926020,zlog_140926019,zlog_140926018,zlog_140926017,zlog_140926016,zlog_140926015,zlog_140926014,zlog_140926013,zlog_140926012,zlog_140926011,zlog_140926010,zlog_140926009,zlog_140926008,zlog_140926007,zlog_140926006,zlog_140926005,zlog_140926004,zlog_140926003,zlog_140926002,zlog_140926001,zlog_140925054,zlog_140925053,zlog_140925052,zlog_140925051,zlog_140925050,zlog_140925049,zlog_140925048,zlog_140925047,zlog_140925046,zlog_140925045,zlog_140925044,zlog_140925043,zlog_140925042,zlog_140925041,zlog_140925040,zlog_140925039,zlog_140925038,zlog_140925037,zlog_140925036,zlog_140925035,zlog_140925034,zlog_140925033,zlog_140925032,zlog_140925031,zlog_140925030,zlog_140925029,zlog_140925028,zlog_140925027,zlog_140925026,zlog_140925025,zlog_140925024,zlog_140925023,zlog_140925022,zlog_140925021,zlog_140925020,zlog_140925019,zlog_140925018,zlog_140925017,zlog_140925016,zlog_140925015,zlog_140925014,zlog_140925013,zlog_140925012,zlog_140925011,zlog_140925010,zlog_140925009,zlog_140925008,zlog_140925007,zlog_140925006,zlog_140923001,zlog_140822035,zlog_140815031,zlog_140815029,zlog_140815028,zlog_140808027,zlog_140808026,zlog_140808024,zlog_140722019,zlog_140606011,zlog_140526005,zlog_140506003,zlog_000000087641,zlog_000000084d6c,zlog_000000082710,zlog_000000081111,zlog_000000";
		List<String> items = Arrays.asList(str.split("\\s*,\\s*"));
		MongoClient mongo = null;
        
		for(int j =0; j<items.size();j++){
			JSONArray resjson = getSQLdata(items.get(j));
			System.out.println("Zlogmigration.main() :"+resjson.length());
			try {
        		mongo = DBConnection.getMongoConnection();
				MongoDatabase database = mongo.getDatabase("greytest");
		        MongoCollection<BasicDBObject>  collection = database.getCollection("zlog_transactions",BasicDBObject.class);
		        for(int i=0;i<resjson.length();i++){
			        //	System.out.println("Zlogmigration.main()"+resjson.getJSONObject(i).toString());
			        	String json = resjson.getJSONObject(i).toString();
			        	System.out.println("Zlogmigration.main() ::" + json);
			        	DBObject dbObject = (DBObject)JSON.parse(json);
			    		collection.insertOne((BasicDBObject) dbObject);
			    		count++;
			        }
	    		
			} catch (MongoException e) {
				e.printStackTrace();
				// TODO: handle exception
			}finally{
				if (mongo != null) {
			        try {
			            mongo.close();
			        } catch (MongoException e) { /* ignored */}
			    }
		
			}
	        
	      }
		System.out.println("Total Record migrated :: "+count);
		System.out.println("Total time needed to migrate :: "+(System.currentTimeMillis()-start));
	}

}