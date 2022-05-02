package dbconnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.UpdateManyModel;
import redis.clients.jedis.Jedis;
import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;

import util.CassandraTypeConverter;
import util.SQLTypeConverter;

public class DBConnectionMgr {

	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DBConnectionMgr.class);
	private static Map<String, DBConnection> mapDB = new HashMap<String, DBConnection>(); 
	private static Map<String, MongoClient> mapMongoConnection = new HashMap<>();
	private static Map<String, Connection> mapJDBCConnection = new HashMap<>();
	private static Map<String, CqlSession> mapCassandraSession = new HashMap<>();
	
	public static Map<String, DBConnection> getMapDB(){
		return mapDB;
	}

	public static void update(Bson filter, Bson updateOp, String struct, String db){
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if(credentials.getDbType().equals("mongodb")){ 
			MongoDatabase mongoDatabase = getMongoClient(credentials).getDatabase(db);
	        MongoCollection<Document> structCollection = mongoDatabase.getCollection(struct);
	        structCollection.updateMany(filter, updateOp);
			logger.info("Update many on collection [{}], filter [{}]",db+ " - "+struct, filter);
		}
		else{
			logger.error("Can't perform update, wrong database type, need document db. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}

	public static void bulkUpdatesInMongoDB(List<UpdateManyModel<Document>> updates, String struct, String db) {
		if(updates == null || updates.size() == 0)
			return;
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if(credentials.getDbType().equals("mongodb")){ 
			MongoDatabase mongoDatabase = DBConnectionMgr.getMongoClient(credentials).getDatabase(db);
	        MongoCollection<Document> structCollection = mongoDatabase.getCollection(struct);
	        structCollection.bulkWrite(updates);
			logger.info("Update bulk many on collection [{}]",db+ " - "+struct);
		}
		else{
			logger.error("Can't perform update, wrong database type, need document db. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}

	public static void upsertMany(Bson filter, Bson updateOp, String struct, String db){
		upsertMany(filter, updateOp, true, struct, db);
	}

	public static void upsertMany(Bson filter, Bson updateOp, boolean upsert, String struct, String db){
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if(credentials.getDbType().equals("mongodb")){ 
			MongoDatabase mongoDatabase = DBConnectionMgr.getMongoClient(credentials).getDatabase(db);
	        MongoCollection<Document> structCollection = mongoDatabase.getCollection(struct);
			UpdateOptions options = new UpdateOptions().upsert(upsert);
	        structCollection.updateMany(filter, updateOp, options);
			logger.info("Update many on collection [{}], filter [{}]",db+ " - "+struct, filter);
		}
		else{
			logger.error("Can't perform update, wrong database type, need document db. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}

	public static void upsertMany(Bson filter, Bson updateOp, List<Bson> arrayFiltersConditions, String struct, String db){
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if(credentials.getDbType().equals("mongodb")){ 
			MongoDatabase mongoDatabase = DBConnectionMgr.getMongoClient(credentials).getDatabase(db);
	        MongoCollection<Document> structCollection = mongoDatabase.getCollection(struct);
			UpdateOptions updateOptions = new UpdateOptions().arrayFilters(arrayFiltersConditions);
	        structCollection.updateMany(filter, updateOp, updateOptions);
			logger.info("Update many on collection [{}], filter [{}]",db+ " - "+struct, filter);
		}
		else{
			logger.error("Can't perform update, wrong database type, need document db. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}


	public static void insertMany(List<Document> documents, String struct, String db) {
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if(!credentials.getDbType().equals("mongodb")){
			logger.error("Can't perform update, wrong database type, need document db. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
		else{
			MongoDatabase mongoDatabase = getMongoClient(credentials).getDatabase(db);
			MongoCollection<Document> structCollection = mongoDatabase.getCollection(struct);
			structCollection.insertMany(documents);
			logger.info("Insert many on collection [{}],  [{}] documents",db+ " - "+struct, documents.size());
		}
	}

	public static MongoClient getMongoClient(DBCredentials credentials) {
		MongoClient mongoClient = null;
		mongoClient = mapMongoConnection.get(credentials.getUrl()+credentials.getPort());
		if (mongoClient == null) {
			mongoClient = MongoClients.create(
					MongoClientSettings.builder()
							.applyToClusterSettings(builder ->
									builder.hosts(Arrays.asList(new ServerAddress(credentials.getUrl(), credentials.getPort()))))
							.build());
			mapMongoConnection.put(credentials.getUrl()+credentials.getPort(), mongoClient);
		}
		return mongoClient;
	}

	public static void insertInTable(List<String> columns, List<List<Object>> rows, String struct, String db) {
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if((credentials.getDbType().equals("mariadb") || credentials.getDbType().equals("sqlite") || credentials.getDbType().equals("postgresql") || credentials.getDbType().equals("mysql") )){
			PreparedStatement statement = null;
			try {
				String sql = "INSERT INTO " + struct + "("+String.join(",",columns)+") VALUES ("+String.join(",", Collections.nCopies(columns.size(),"?"))+")";
				Connection conn = getJDBCConnection(credentials);
				// Map<String, String> columnTypes = getSQLColumns(conn, struct);
				statement = conn.prepareStatement(sql);
				for (List<Object> row : rows) {
					for (int i = 1; i <= row.size(); i++) {
						// statement.setObject(i, SQLTypeConverter.get(row.get(i-1), columnTypes.get(columns.get(i-1))));
						statement.setObject(i, row.get(i-1));
					}
					statement.addBatch();
					statement.clearParameters();
				}
				int[] result = statement.executeBatch();
				logger.info("BATCH INSERT INTO '{}' - '{}' lines ", struct, result.length);  
			} catch (SQLException e) {
				logger.error("SQL Error in preparedStatement");
				e.printStackTrace();
				throw new RuntimeException("Error in insert SQL statement");
			} finally {
				if(statement != null) {
					try {
						statement.close();
					} catch(SQLException e2) {}
				}
			}
		} else
			if(credentials.getDbType().equals("cassandra")) {
				CqlSession session = getCassandraConnection(credentials);
				Map<String, DataType> schema = getCassandraColumns(session, db, struct);
				List<DataType> columnTypes = new ArrayList<DataType>();
				for(String colName : columns)
					columnTypes.add(schema.get(colName));
				
				String cql = "INSERT INTO " + struct + "("+String.join(",",columns)+") VALUES ("+String.join(",", Collections.nCopies(columns.size(),"?"))+")";
				
				com.datastax.oss.driver.api.core.cql.PreparedStatement statement = getCassandraConnection(credentials).prepare(cql);
				
				BoundStatement[] bounds = new BoundStatement[rows.size()];
				int counter = 0;
				for (List<Object> row : rows) {
					Object[] cqlRow = new Object[row.size()];
					for(int i = 0; i < row.size(); i++)
						cqlRow[i] = (CassandraTypeConverter.get(row.get(i), columnTypes.get(i)));
					
					bounds[counter] = statement.bind(cqlRow);
					counter++;
				}
				
				BatchStatement batchStmt = BatchStatement.newInstance(BatchType.LOGGED, bounds);
				session.execute(batchStmt);
			}
			else{
				logger.error("Can't perform update, wrong database type, need relational (mariadb or sqlite or postgresql or mysql) or cassandra database. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
				throw new RuntimeException("Can't perform update, wrong database type, need document db");
			}
	}

	private static Map<String, DataType> getCassandraColumns(CqlSession session, String db, String table) {
		Map<String, DataType> res = new HashMap<String, DataType>();
		for(Entry<CqlIdentifier, ColumnMetadata> entry : session.getMetadata().getKeyspace(db).get().getTable(table).get().getColumns().entrySet()) {
			String colName = entry.getKey().asCql(true);
			DataType colType = entry.getValue().getType();
			res.put(colName, colType);
		}
		
		return res;
	}

	private static Map<String, String> getSQLColumns(Connection conn, String table) {
		Map<String, String> res = new HashMap<String, String>();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement("select * from " + table + " LIMIT 0");
			rs = ps.executeQuery();
			ResultSetMetaData rsm = rs.getMetaData();
			for (int i = 1; i <= rsm.getColumnCount(); i++) {
				String colName = rsm.getColumnLabel(i);
				String colType = rsm.getColumnClassName(i);
				res.put(colName, colType);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
			}
			try {
				if (ps != null)
					ps.close();
			} catch (SQLException e) {
			}
		}

		return res;
	}

	public static void updatesInTable(List<String> updates, String db) {
		if(updates == null || updates.size() == 0)
			return;

		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		java.sql.Statement stmt = null;
		try {
			stmt = getJDBCConnection(credentials).createStatement();
			
			for(int i = 0; i < updates.size(); i++) {
				stmt.addBatch(updates.get(i));
				if(i % 1000 == 0) {
					stmt.executeBatch();
				}

			}

			stmt.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("SQL Error in update Query");
		} finally {
			if(stmt != null)
				try {
					stmt.close();
				} catch (SQLException e2) {
					e2.printStackTrace();
				}
		}
	}

	public static void updateInTable(String updateSQL, String db) {
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		java.sql.Statement stmt = null;
		try {
			stmt = getJDBCConnection(credentials).createStatement();
			int count = stmt.executeUpdate(updateSQL);
			logger.debug("SQL query :" + updateSQL + " => " + count + " rows updated.");
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("SQL Error in update Query");
		} finally {
			if(stmt != null)
				try {
					stmt.close();
				} catch (SQLException e2) {
					e2.printStackTrace();
				}
		}
	}

	public static void updateInTable(String filtercolumn, Object filtervalue, List<String> columns, List<Object> values, String struct, String db) {
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if((credentials.getDbType().equals("mariadb") || credentials.getDbType().equals("sqlite") || credentials.getDbType().equals("postgresql") || credentials.getDbType().equals("mysql"))){
			try {
				StringJoiner joiner = new StringJoiner(",", "", "= ?");
				for (String s : columns) {
					joiner.add(s);
				}
				String sql = "UPDATE " + struct + " SET " + joiner +" WHERE "+filtercolumn +"= ?";
				PreparedStatement statement = getJDBCConnection(credentials).prepareStatement(sql);
				for (int i = 1; i <= values.size(); i++) {
					statement.setObject(i,values.get(i-1));
				}
				statement.setObject(values.size()+1,filtervalue);
				statement.addBatch();
				int[] result = statement.executeBatch();
				logger.info("BATCH UPDATE INTO '{}' - '{}' lines ", struct, result.length);  
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("SQL Error in preparedStatement");
			}
		}
		else{
			logger.error("Can't perform update, wrong database type, need relational (mariadb or sqlite or postgresql) database. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}

	public static void writeKeyValueList(String key, String value, String dbName) {
		DBCredentials credentials = DBCredentials.getDbPorts().get(dbName);
		logger.debug("Jedis LPush key [{}] value [{}] to redis db [{}]", key, value, dbName);
		Jedis jedis;
		jedis = new Jedis(credentials.getUrl(), credentials.getPort());
		jedis.lpush(key, value);
		jedis.close();
	}

	public static Connection getJDBCConnection(DBCredentials credentials) {
		Connection res = mapJDBCConnection.get(credentials.getUrl() + credentials.getPort());
		try {
			if (res == null) {
				res = DriverManager.getConnection("jdbc:"+credentials.getDbTypeForJDBCConnection()+"://" + credentials.getUrl() + ":" + credentials.getPort() + "/" + credentials.getDbName(), credentials.getUserName(), credentials.getUserPwd());
				mapJDBCConnection.put(credentials.getUrl() + credentials.getPort(), res);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Immpossible to connect to relational db [{} - {} : {}] ", credentials.getDbName(), credentials.getUrl(),credentials.getPort());
		}
		return res;
	}

	public static CqlSession getCassandraConnection(DBCredentials credentials) {
		CqlSession res = mapCassandraSession.get(credentials.getUrl() + credentials.getDbName() + credentials.getPort());
		
			if (res == null) {
				res = CqlSession
                		.builder()
                   		.addContactPoint(new InetSocketAddress(credentials.getUrl(), credentials.getPort()))
                    	.withLocalDatacenter("datacenter1")
                    	.withKeyspace(credentials.getDbName())
                    	.build();
				mapCassandraSession.put(credentials.getUrl() + credentials.getDbName() + credentials.getPort(), res);
			}
		
		return res;
	}

}


