package Design.QueryLayer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.protobuf.ServiceException;

public class QueryLayer {

	public static String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";;
	public static String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
	public static String HBASE_CONFIGURATION_HBASE_MASTER = "hbase.master";

	public static void main(String[] args) throws IOException,
			ServiceException, JSONException {
		// TODO Auto-generated method stub

		String hbaseZookeeperQuorumIP = "192.168.1.9";
		String hbaseZookeeperClientPort = "2181";
		String hbaseMasterIPandPort = "192.168.1.9:60000";
		String tableName = "SensorValues";

		connectToHbase(hbaseZookeeperQuorumIP, hbaseZookeeperClientPort,
				hbaseMasterIPandPort, tableName);
	}

	private static JSONArray connectToHbase(String hbaseZookeeperQuorumIP,
			String hbaseZookeeperClientPort, String hbaseMasterIPandPort,
			String tableName) throws IOException, ServiceException,
			JSONException {

		Configuration hConf = HBaseConfiguration.create();
		hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorumIP);
		hConf.set(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT,
				hbaseZookeeperClientPort);
		hConf.set(HBASE_CONFIGURATION_HBASE_MASTER, hbaseMasterIPandPort);
		HBaseAdmin.checkHBaseAvailable(hConf);

		HBaseAdmin hbadmin = new HBaseAdmin(hConf);

		HTableDescriptor[] tabdesc = hbadmin.listTables();
		for (int i = 0; i < tabdesc.length; i++) {
			System.out.println("Table = " + new String(tabdesc[i].getName()));
		}

		HTable hTable = new HTable(hConf, tableName);

		Scan scan = new Scan();
		scan.setCaching(20);

		scan.addFamily(Bytes.toBytes("d"));

		ResultScanner scanner = hTable.getScanner(scan);

		JSONArray array = new JSONArray();

		for (Result result = scanner.next(); (result != null); result = scanner
				.next()) {
			// gets rowkey
			String rowKey = Bytes.toString(result.getRow());
			//String sensorID = rowKey.substring(0, rowKey.indexOf("-"));
			
			//split row key into id and timestamp
			String [] rowKeyArray = rowKey.split("-",2);

			String value = Bytes.toString(result.getValue(
					Bytes.toBytes("d"), Bytes.toBytes("val")));
			
			JSONObject obj = new JSONObject();
			obj.put("id", rowKeyArray[0]);
			obj.put("timestamp", rowKeyArray[1]);
			obj.put("value", value);
			array.put(obj);
		}
		
		scanner.close();
		hTable.close();
		hbadmin.close();

		return array;
	}
}
