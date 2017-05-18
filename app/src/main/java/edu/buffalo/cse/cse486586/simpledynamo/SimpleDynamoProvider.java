package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDynamoProvider extends ContentProvider {


	String myPort;
	int Final_port = 0;
	static final int SERVER_PORT = 10000;
	ArrayList<String> missed_keys = new ArrayList<String>();
	String failed_port = "Nothing_yet";
	Boolean recovery_flag = false;
	Boolean some_node_recovery = true;
	HashMap<String, String> key_details = new HashMap<String, String>();
	int[] ringOrder = {11124, 11112, 11108, 11116, 11120};
	String[] ordered;

	private SQLiteDatabase dbase;
	static final String db_name = "Messenger3";
	static final String table_name = "basetable";
	static final int db_version = 1;
	static final String create_table = " CREATE TABLE " + table_name + " (key TEXT," + " value TEXT);";

	private static class db_help extends SQLiteOpenHelper {
		db_help(Context context){
			super(context, db_name, null, db_version);
		}

		@Override
		public void onCreate(SQLiteDatabase dbase) {
			dbase.execSQL(create_table);

		}

		@Override
		public void onUpgrade(SQLiteDatabase dbase, int oldVersion, int newVersion) {
			dbase.execSQL("DROP TABLE IF EXISTS " +  table_name);
			onCreate(dbase);
		}
	}


	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		if(selection.equals("@")) {
			db_help mDbHelper = new db_help(getContext());
			SQLiteDatabase db = mDbHelper.getWritableDatabase();
			db.delete(table_name, null, null);
		}

		else if(selection.equals("*")){
			db_help mDbHelper = new db_help(getContext());
			SQLiteDatabase db = mDbHelper.getWritableDatabase();
			db.delete(table_name, null, null);

			for(int port : ringOrder) {
				if (port != Final_port) {
					try {
						Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								port);
						String Insert_msg = "Delete_all" + "~@#" + Final_port;
						DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
						Output_stream_1.writeUTF(Insert_msg);
						Output_stream_1.flush();
						socket_1.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}

		else{
			String ports = findPorts(selection);
			String[] port_list = ports.split("~@#");
			for(String port : port_list){
				try {
//						Log.v(TAG, port);
					int sending_port = (Integer.parseInt(port));
					if (sending_port != Final_port) {
						Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								sending_port);
						String Insert_msg = "Delete_key" + "~@#" + selection;
						DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
						Output_stream_1.writeUTF(Insert_msg);
						Output_stream_1.flush();
						socket_1.close();
					}
					else {
//							Log.v(TAG, "Query entered in else");
						db_help mDbHelper = new db_help(getContext());
						SQLiteDatabase db = mDbHelper.getWritableDatabase();
						String selection_final = "key = ?";
						String[] selection_filter = { selection };
						db.delete(table_name, selection_final, selection_filter);
					}
				}catch(Exception e){
					e.printStackTrace();
				}
			}
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		//reference https://developer.android.com/training/basics/data-storage/databases.html
		while (true){
			Log.v(TAG,"Insert Infinite blocking");

			if(some_node_recovery==true){
				break;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		String KEY_FIELD = "key";
		ArrayList<String> keys = new ArrayList<String>();
		ArrayList<String> value = new ArrayList<String>();
		HashMap<String, String> key_value = new HashMap<String, String>();
		for (String str : values.keySet()) {
			if (str.equals(KEY_FIELD)) {
				keys.add(values.getAsString(str));
			}
			else
				value.add(values.getAsString(str));
		}

		for (int i = 0; i< keys.size(); i++){
			key_value.put(keys.get(i), value.get(i));
		}
		for(String key : keys){
			try {
//				String key_hash = genHash(key);
				String ports = findPorts(key);
				String[] port_list = ports.split("~@#");
				for(String port : port_list){
					try {
//						Log.v(TAG, port);
						int sending_port = (Integer.parseInt(port));
						if (sending_port != Final_port) {
							Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									sending_port);
							String Insert_msg = "Insert" + "~@#" + key + "~@#" + key_value.get(key);
							DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
							Output_stream_1.writeUTF(Insert_msg);
							Output_stream_1.flush();
							DataInputStream Input_stream_1 = new DataInputStream(socket_1.getInputStream());
							String reply;
							while (true){
								reply = Input_stream_1.readUTF();
								reply = reply.trim();
								if (reply.length()>0){
									break;
								}
							}
							socket_1.close();
						}
						else {
//							Log.v(TAG, "Query entered in else");
							ContentValues cv = new ContentValues();
							cv.put("key", key);
							cv.put("value", key_value.get(key));

							db_help mDbHelper = new db_help(getContext());
							SQLiteDatabase db = mDbHelper.getWritableDatabase();

							db.insertWithOnConflict(table_name, null, cv, 5);
						}
					}catch(Exception e){
						e.printStackTrace();
						String key_pair = key + "~~" + key_value.get(key);
						missed_keys.add(key_pair);
						failed_port = port;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		Log.v("insert", values.toString());
		return uri;
	}

	@Override
	public synchronized boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		Final_port = Integer.parseInt(myPort);
		setuplist();
		db_help mDbHelper = new db_help(getContext());
		SQLiteDatabase db = mDbHelper.getWritableDatabase();

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}

		String msg = "Recovery";
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);

		while (true){

			if (recovery_flag == true){
				recovery_flag = false;
				try {
					Thread.sleep(600);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				break;
			}

		}

		return false;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		while (true){
			Log.v(TAG,"Query Infinite blocking");
			if(some_node_recovery==true){
				break;
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// TODO Auto-generated method stub
//		String[] column_name = {"key", "value"};
//		MatrixCursor query_result = new MatrixCursor(column_name);
		Cursor query_result;




		if (selection.equals("@")){
			db_help mDbHelper = new db_help(getContext());
			SQLiteDatabase db = mDbHelper.getReadableDatabase();

			query_result = db.query(table_name, null, null, null, null, null, null);
		}

		else if (selection.equals("*")){
			String[] column_name = {"key", "value"};
			MatrixCursor query_result1 = new MatrixCursor(column_name);
			String All_query = "";
			for(int port : ringOrder){
				if(port != Final_port){
					try{
						Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								port);
						String Insert_msg = "Global_DHT" + "~@#" + Final_port;
						DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
						Output_stream_1.writeUTF(Insert_msg);
						Output_stream_1.flush();
						DataInputStream Input_stream_1 = new DataInputStream(socket_1.getInputStream());
						String reply;
						while (true){
							reply = Input_stream_1.readUTF();
							reply = reply.trim();
							if (reply.length()>0){
								break;
							}
						}
						All_query = All_query.concat(reply);
						socket_1.close();
					}catch (Exception e){
						e.printStackTrace();
					}
				}
				else {
//					String delim_1 = "~@#";
//					String delim_2 = "~~";
//                Log.v(TAG, String.valueOf(local_key_value.size()));
					db_help mDbHelper = new db_help(getContext());
					SQLiteDatabase db = mDbHelper.getReadableDatabase();

					Cursor temp_result = db.query(table_name, null, null, null, null, null, null);

					try {
						while (temp_result.moveToNext()) {
							String key = temp_result.getString(temp_result.getColumnIndex("key"));
							String value = temp_result.getString(temp_result.getColumnIndex("value"));
							All_query = All_query.concat("~@#" + key +"~~" + value);
						}
					} finally {
						temp_result.close();
					}

				}
			}
			String[] final_outcome = All_query.split("~@#");
			for (int i = 1; i<final_outcome.length; i++){
				String[] key_value = final_outcome[i].split("~~");
				String[] row = {key_value[0], key_value[1]};
				query_result1.addRow(row);
			}

			query_result = query_result1;
		}

		else{
			String[] column_name = {"key", "value"};
			MatrixCursor query_result1 = new MatrixCursor(column_name);
			String ports = findPorts(selection);
			String[] port_list = ports.split("~@#");
			String All_query = "";
			for(String port : port_list){
				if(Integer.parseInt(port) != Final_port){
					try{
						Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
								Integer.parseInt(port));
						String Insert_msg = "Find_key" + "~@#" + selection;
						DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
						Output_stream_1.writeUTF(Insert_msg);
						Output_stream_1.flush();
						DataInputStream Input_stream_1 = new DataInputStream(socket_1.getInputStream());
						String reply;
						while (true){
							reply = Input_stream_1.readUTF();
							reply = reply.trim();
							if (reply.length()>0){
								break;
							}
						}
						All_query = All_query.concat(reply);
						socket_1.close();
					}catch (Exception e){
						e.printStackTrace();
					}
				}
				else{
					db_help mDbHelper = new db_help(getContext());
					SQLiteDatabase db = mDbHelper.getReadableDatabase();
					String selection_final = "key = ?";
					String[] selection_filter = { selection };

					Cursor temp_result = db.query(table_name,null,selection_final,selection_filter,null,null,null);
					try {
						while (temp_result.moveToNext()) {
							String key = temp_result.getString(temp_result.getColumnIndex("key"));
							String value = temp_result.getString(temp_result.getColumnIndex("value"));
							All_query = All_query.concat("~@#" + key +"~~" + value);
						}
					} finally {
						temp_result.close();
					}
				}
			}

			String[] final_outcome = All_query.split("~@#");
			for (int i = 1; i<final_outcome.length; i++){
				String[] key_value = final_outcome[i].split("~~");
				String[] row = {key_value[0], key_value[1]};
				query_result1.addRow(row);
			}

			query_result = query_result1;

		}



		return query_result;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			Socket socket_server = null;
			try {

				while(true) {
					//Code entirely similar to PA1.
					socket_server = serverSocket.accept();
					DataInputStream Server_input = new DataInputStream(socket_server.getInputStream());
					String Input_string = Server_input.readUTF();
					String[] msg_type = Input_string.split("~@#");

					if (msg_type[0].equals("Insert")) {
						String key = msg_type[1];
						String value = msg_type[2];

						ContentValues cv = new ContentValues();
						cv.put("key", key);
						cv.put("value", value);

						db_help mDbHelper = new db_help(getContext());
						SQLiteDatabase db = mDbHelper.getWritableDatabase();
						db.insertWithOnConflict(table_name, null, cv, 5);

						DataOutputStream ack = new DataOutputStream(socket_server.getOutputStream());
						ack.writeUTF("Insert_done");
						ack.flush();
						ack.close();
					}

					else if (msg_type[0].equals("Turn_off_Insert")){
						some_node_recovery = false;
					}

					else if (msg_type[0].equals("Turn_on_Insert")){
						some_node_recovery = true;
					}

					else if (msg_type[0].equals("Global_DHT")){
//						String delim_1 = "~@#";
//						String delim_2 = "~~";
						String tempQuery = "";
						db_help mDbHelper = new db_help(getContext());
						SQLiteDatabase db = mDbHelper.getReadableDatabase();

						Cursor temp_result = db.query(table_name, null, null, null, null, null, null);

						try {
							while (temp_result.moveToNext()) {
								String key = temp_result.getString(temp_result.getColumnIndex("key"));
								String value = temp_result.getString(temp_result.getColumnIndex("value"));
								tempQuery = tempQuery.concat("~@#" + key +"~~" + value);
							}
						} finally {
							temp_result.close();
						}
						DataOutputStream ack = new DataOutputStream(socket_server.getOutputStream());
						ack.writeUTF(tempQuery);
						ack.flush();
						ack.close();
					}

					else if (msg_type[0].equals("Find_key")){
//						String delim_1 = "~@#";
//						String delim_2 = "~~";
						String tempQuery = "";
						db_help mDbHelper = new db_help(getContext());
						SQLiteDatabase db = mDbHelper.getReadableDatabase();
						String selection_final = "key = ?";
						String[] selection_filter = { msg_type[1] };

						Cursor temp_result = db.query(table_name,null,selection_final,selection_filter,null,null,null);

						try {
							while (temp_result.moveToNext()) {
								String key = temp_result.getString(temp_result.getColumnIndex("key"));
								String value = temp_result.getString(temp_result.getColumnIndex("value"));
								tempQuery = tempQuery.concat("~@#" + key +"~~" + value);
							}
						} finally {
							temp_result.close();
						}
						DataOutputStream ack = new DataOutputStream(socket_server.getOutputStream());
						ack.writeUTF(tempQuery);
						ack.flush();
						ack.close();
					}

					else if (msg_type[0].equals("I_am_Back")){
						if(!failed_port.equals("Nothing_yet")){
							Log.v(TAG, "Recovery request recieved from " + msg_type[1]);
							String reply = "";
							for(int i = 0; i<missed_keys.size(); i++){
								String combine_vals = missed_keys.get(i);
								reply = reply.concat("~@#").concat(combine_vals);
							}
							missed_keys.clear();
							DataOutputStream ack = new DataOutputStream(socket_server.getOutputStream());
							ack.writeUTF(reply);
							ack.flush();
							ack.close();
						}else{
							DataOutputStream ack = new DataOutputStream(socket_server.getOutputStream());
							ack.writeUTF("Nothing");
							ack.flush();
							ack.close();
						}
					}

					else if (msg_type[0].equals("Delete_all")){
						db_help mDbHelper = new db_help(getContext());
						SQLiteDatabase db = mDbHelper.getWritableDatabase();
						db.delete(table_name, null, null);
					}

					else if (msg_type[0].equals("Delete_key")){
						db_help mDbHelper = new db_help(getContext());
						SQLiteDatabase db = mDbHelper.getWritableDatabase();
						String selection_final = "key = ?";
						String[] selection_filter = { msg_type[1] };
						db.delete(table_name, selection_final, selection_filter);
					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
			return null;
		}
	}


	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			String msg_recieved = msgs[0];
			String portFrom = msgs[1];


			if(msg_recieved.equals("Recovery")) {
				db_help mDbHelper = new db_help(getContext());
				SQLiteDatabase db = mDbHelper.getWritableDatabase();
				String All_query = new String();

				for (int port : ringOrder) {

					if (port != Final_port) {
						try {
							Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									port);
							String Insert_msg = "Turn_off_Insert" + "~@#" + Final_port;
							DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
							Output_stream_1.writeUTF(Insert_msg);
							Output_stream_1.flush();
							socket_1.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}

				for (int port : ringOrder) {

					if (port != Final_port) {
						try {
							Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									port);
							String Insert_msg = "I_am_Back" + "~@#" + Final_port;
							DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
							Output_stream_1.writeUTF(Insert_msg);
							Output_stream_1.flush();
							DataInputStream Input_stream_1 = new DataInputStream(socket_1.getInputStream());
							String reply;
							Log.v(TAG, "Recovery request sent to: " + port);

							while (true) {
								reply = Input_stream_1.readUTF();
								reply = reply.trim();
								if (reply.length() > 0) {
									break;
								}
							}
							if (!reply.equals("Nothing")) {
								All_query = All_query.concat(reply);
								Log.v(TAG, "Recovery recieved from : " + port);
							}
							socket_1.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				if (All_query.length() > 0) {
					String[] final_outcome = All_query.split("~@#");
					for (int i = 1; i < final_outcome.length; i++) {
						String[] key_value = final_outcome[i].split("~~");
						ContentValues cv = new ContentValues();
						cv.put("key", key_value[0]);
						cv.put("value", key_value[1]);
						db.insertWithOnConflict(table_name, null, cv, 5);
					}
					Log.v(TAG, "Recovery complete");
				}

				for (int port : ringOrder) {

					if (port != Final_port) {
						try {
							Socket socket_1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									port);
							String Insert_msg = "Turn_on_Insert" + "~@#" + Final_port;
							DataOutputStream Output_stream_1 = new DataOutputStream(socket_1.getOutputStream());
							Output_stream_1.writeUTF(Insert_msg);
							Output_stream_1.flush();
							socket_1.close();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}


				recovery_flag = true;
			}


			return null;
		}
	}


	public int setuplist(){
		try {
			String no1 = genHash("5562");
			String no2 = genHash("5556");
			String no3 = genHash("5554");
			String no4 = genHash("5558");
			String no5 = genHash("5560");
			ordered = new String[]{no1, no2, no3, no4, no5};
			Arrays.sort(ordered);

//			Log.v(TAG, "Ordered created");
//			Log.v(TAG, no1 + "    " + no2 + "    " + no3 + "    " + no4 + "    " +no5);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public String findPorts(String key){
		String contend = "";
		String result;
		try {
			contend = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		Log.v(TAG, contend);
		if((ordered[1].compareTo(contend)> 0) && (ordered[0].compareTo(contend)<0)){
			result = "11112" + "~@#" + "11108"+ "~@#"+ "11116";
//			Log.v(TAG, "first else");
			//value belongs to 5556 5554 5558
		}
		else if((ordered[2].compareTo(contend)>0) && (ordered[1].compareTo(contend)<0)){
			result = "11108" + "~@#" + "11116"+ "~@#"+ "11120";
//			Log.v(TAG, "second else");
			//value belongs to 5554 5558 5560
		}
		else if((ordered[3].compareTo(contend)>0) && (ordered[2].compareTo(contend)<0)){
			result = "11116" + "~@#" + "11120"+ "~@#"+ "11124";
//			Log.v(TAG, "third else");
			//value belongs to 5558 5560 5562
		}
		else if((ordered[4].compareTo(contend)>0) && (ordered[3].compareTo(contend)<0)){
			result = "11120" + "~@#" + "11124"+ "~@#"+ "11112";
//			Log.v(TAG, "fourth else");
			//value belongs to 5560 5562 5556
		}
		else {
			result = "11124" + "~@#" + "11112"+ "~@#"+ "11108";
			//value belongs to 5562 5556 5554
		}

		return result;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
