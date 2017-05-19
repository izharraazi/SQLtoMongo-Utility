package migrate;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;

public class DBConnection {

	public static Connection getConnection() {
		Properties props = new Properties();
		FileInputStream fis = null;
		Connection con = null;
		try {
			fis = new FileInputStream("src/main/java/db.properties");
			props.load(fis);

			// load the Driver Class
			Class.forName(props.getProperty("DB_DRIVER_CLASS"));

			// create the connection now
			con = DriverManager.getConnection(props.getProperty("DB_URL"),
					props.getProperty("DB_USERNAME"),
					props.getProperty("DB_PASSWORD"));
		} catch (IOException | ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return con;
	}
	public static MongoClient getMongoConnection() throws IOException {
		Properties props = new Properties();
		FileInputStream fis = null;
		Connection con = null;
		String hostname=null;
		String port =null;
		String dbname = null;
		String username=null;
		String pwd = null;
		try {
			fis = new FileInputStream("src/main/java/db.properties");
			props.load(fis);
			port =props.getProperty("M_PORT");
			hostname =props.getProperty("M_HOST");
			MongoClient mongo = new MongoClient(hostname,Integer.parseInt(port));
			return mongo;
		} catch (MongoClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
}
