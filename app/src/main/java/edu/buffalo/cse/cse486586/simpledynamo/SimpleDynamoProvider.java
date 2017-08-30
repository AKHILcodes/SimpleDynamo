package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.SystemClock;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	static final String TAG = SimpleDynamoActivity.class.getSimpleName();
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";

	static final String PORT0_ID = "5554";
	static final String PORT1_ID = "5556";
	static final String PORT2_ID = "5558";
	static final String PORT3_ID = "5560";
	static final String PORT4_ID = "5562";

	String hash_PORT0_ID = genHash(PORT0_ID);
	String hash_PORT1_ID = genHash(PORT1_ID);
	String hash_PORT2_ID = genHash(PORT2_ID);
	String hash_PORT3_ID = genHash(PORT3_ID);
	String hash_PORT4_ID = genHash(PORT4_ID);

	String myPort; // values like 5554,5556
	String predPort; // values like 5554,5556
	String succPort; // values like 5554,5556
	String succSuccPort;
	String predPredPort;
	String hashedMyPort;
	String hashedPredPort;
	String hashedSuccPort;
	HashMap<String,String> mapOriginalPort = new HashMap<String, String>();
	HashMap<String,String> mapPredPort = new HashMap<String, String>();
	HashMap<String,String> mapPredPredPort = new HashMap<String, String>();
	/*HashMap<String,String> mapSuccPort = new HashMap<String, String>();*/

	String hashedArrayOfPorts[] = {hash_PORT4_ID,hash_PORT1_ID,hash_PORT0_ID,hash_PORT2_ID,hash_PORT3_ID};
	String nonHashedArrayOfPorts[] = {"5562","5556","5554","5558","5560"};

	int posOrig = -1;
	int posPred = -1;
	int posSucc = -1;
	int posPredPred = -1;
	int posSuccSucc = -1;

	String hashedKey;
	private final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo");
	boolean queryResultReceived = false;
	public static Cursor ResultAnswerCursor;
	DatabaseHelper mOpenHelper;
	SQLiteDatabase db;
	boolean queryFailedFlag = false;

	public SimpleDynamoProvider() throws NoSuchAlgorithmException {
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		int deletePortPos = posOrig;

		if(selection.equals("@")){
			mapOriginalPort.clear();
			mapPredPort.clear();
			mapPredPredPort.clear();/*
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteAt", (Integer.parseInt(succPort) * 2) + "","No;Yes;No");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteAt", (Integer.parseInt(predPort) * 2) + "","No;No;Yes");*/
			/*fraudMap.clear();*/

		} else if(selection.equals("*")){

			mapPredPort.clear();
			mapOriginalPort.clear();
			mapPredPredPort.clear();
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteAll", (Integer.parseInt(myPort) * 2) + "");
			/*fraudMap.clear();*/

		} else{
			try {
				String hashedDeleteKey = genHash(selection);

				if((hashedDeleteKey.compareTo(hashedMyPort) <= 0 && hashedDeleteKey.compareTo(hashedPredPort) > 0)||
						((hashedMyPort.compareTo(hashedPredPort) < 0) && ((hashedDeleteKey.compareTo(hashedPredPort)>0) || (hashedDeleteKey.compareTo(hashedMyPort)<=0)))){

					mapOriginalPort.remove(selection);

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteSingle", (Integer.parseInt(nonHashedArrayOfPorts[(deletePortPos+1)%5]) * 2) + "",selection,"No;Yes;No");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteSingle", (Integer.parseInt(nonHashedArrayOfPorts[(deletePortPos+2)%5]) * 2) + "",selection,"No;No;Yes");

				} else if((hashedDeleteKey.compareTo(hashedArrayOfPorts[(deletePortPos+1)%5]) <= 0 && hashedDeleteKey.compareTo(hashedArrayOfPorts[deletePortPos]) > 0)||
						((hashedArrayOfPorts[(deletePortPos+1)%5].compareTo(hashedArrayOfPorts[deletePortPos]) < 0) &&
								((hashedDeleteKey.compareTo(hashedArrayOfPorts[deletePortPos])>0) || (hashedDeleteKey.compareTo(hashedArrayOfPorts[(deletePortPos+1)%5])<=0)))){

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteSingle", (Integer.parseInt(nonHashedArrayOfPorts[(deletePortPos+1)%5]) * 2) + "",selection,"Yes;No;No");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteSingle", (Integer.parseInt(nonHashedArrayOfPorts[(deletePortPos+2)%5]) * 2) + "",selection,"No;Yes;No");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteSingle", (Integer.parseInt(nonHashedArrayOfPorts[(deletePortPos+3)%5]) * 2) + "",selection,"No;No;Yes");

				} else if((hashedDeleteKey.compareTo(hashedArrayOfPorts[(deletePortPos+2)%5]) <= 0 && hashedDeleteKey.compareTo(hashedArrayOfPorts[(deletePortPos+1)%5]) > 0)||
						((hashedArrayOfPorts[(deletePortPos+2)%5].compareTo(hashedArrayOfPorts[(deletePortPos+1)%5]) < 0) &&
								((hashedDeleteKey.compareTo(hashedArrayOfPorts[(deletePortPos+1)%5])>0) || (hashedDeleteKey.compareTo(hashedArrayOfPorts[(deletePortPos+2)%5])<=0)))){

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteSingle", (Integer.parseInt(nonHashedArrayOfPorts[(deletePortPos+2)%5]) * 2) + "",selection,"Yes;No;No");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteSingle", (Integer.parseInt(nonHashedArrayOfPorts[(deletePortPos+3)%5]) * 2) + "",selection,"No;Yes;No");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteSingle", (Integer.parseInt(nonHashedArrayOfPorts[(deletePortPos+4)%5]) * 2) + "",selection,"No;No;Yes");

				} else if((hashedDeleteKey.compareTo(hashedArrayOfPorts[(deletePortPos+3)%5]) <= 0 && hashedDeleteKey.compareTo(hashedArrayOfPorts[(deletePortPos+2)%5]) > 0)||
						((hashedArrayOfPorts[(deletePortPos+3)%5].compareTo(hashedArrayOfPorts[(deletePortPos+2)%5]) < 0) &&
								((hashedDeleteKey.compareTo(hashedArrayOfPorts[(deletePortPos+2)%5])>0) || (hashedDeleteKey.compareTo(hashedArrayOfPorts[(deletePortPos+3)%5])<=0)))){

					mapPredPredPort.remove(selection);

					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteSingle", (Integer.parseInt(nonHashedArrayOfPorts[(deletePortPos+3)%5]) * 2) + "",selection,"Yes;No;No");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteSingle", (Integer.parseInt(nonHashedArrayOfPorts[(deletePortPos+4)%5]) * 2) + "",selection,"No;Yes;No");

				} else {
					mapPredPort.remove(selection);
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteSingle", (Integer.parseInt(nonHashedArrayOfPorts[(deletePortPos+1)%5]) * 2) + "",selection,"No;No;Yes");
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "DeleteSingle", (Integer.parseInt(nonHashedArrayOfPorts[(deletePortPos+4)%5]) * 2) + "",selection,"Yes;No;No");
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
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
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key=values.getAsString("key");
		String msg = values.getAsString("value");
		Log.e(TAG,"insert request received for "+key + " by "+myPort);
		try {
			hashedKey = genHash(key);
		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG,"Gen Hash Key");
		}
		int insertPortPos = posOrig;
		if((hashedKey.compareTo(hashedMyPort) <= 0 && hashedKey.compareTo(hashedPredPort) > 0)||
				((hashedMyPort.compareTo(hashedPredPort) < 0) && ((hashedKey.compareTo(hashedPredPort)>0) || (hashedKey.compareTo(hashedMyPort)<=0)))){

			Log.e(TAG,"key "+ key+ " belongs to "+myPort);
			mapOriginalPort.put(key,msg);
			Log.e(TAG,"Key " + key + " inserted in " + myPort);
			StringBuilder sbInsert = new StringBuilder("");
			sbInsert.append(key);
			sbInsert.append(";");
			sbInsert.append(msg);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", sbInsert.toString(),"No;Yes;No",
					(Integer.parseInt(nonHashedArrayOfPorts[(insertPortPos+1)%5]) * 2) + "");
			Log.e(TAG,"key "+key+ " sent to "+ nonHashedArrayOfPorts[(insertPortPos+1)%5]);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", sbInsert.toString(),"No;No;Yes",
					(Integer.parseInt(nonHashedArrayOfPorts[(insertPortPos+2)%5]) * 2) + "");

			Log.e(TAG,"key "+key+ " sent to "+ nonHashedArrayOfPorts[(insertPortPos+2)%5]);

		} else if((hashedKey.compareTo(hashedArrayOfPorts[(insertPortPos+1)%5]) <= 0 && hashedKey.compareTo(hashedArrayOfPorts[insertPortPos]) > 0)||
				((hashedArrayOfPorts[(insertPortPos+1)%5].compareTo(hashedArrayOfPorts[insertPortPos]) < 0) &&
						((hashedKey.compareTo(hashedArrayOfPorts[insertPortPos])>0) || (hashedKey.compareTo(hashedArrayOfPorts[(insertPortPos+1)%5])<=0)))){

			Log.e(TAG,"key "+ key+ " belongs to "+nonHashedArrayOfPorts[(insertPortPos+1)%5]);

			StringBuilder sbInsert = new StringBuilder("");
			sbInsert.append(key);
			sbInsert.append(";");
			sbInsert.append(msg);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", sbInsert.toString(),"Yes;No;No",
					(Integer.parseInt(nonHashedArrayOfPorts[(insertPortPos+1)%5]) * 2) + "");
			Log.e(TAG,"key "+key+ " sent to "+ nonHashedArrayOfPorts[(insertPortPos+1)%5]);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", sbInsert.toString(),"No;Yes;No",
					(Integer.parseInt(nonHashedArrayOfPorts[(insertPortPos+2)%5]) * 2) + "");
			Log.e(TAG,"key "+key+ " sent to "+ nonHashedArrayOfPorts[(insertPortPos+2)%5]);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", sbInsert.toString(),"No;No;Yes",
					(Integer.parseInt(nonHashedArrayOfPorts[(insertPortPos+3)%5]) * 2) + "");
			Log.e(TAG,"key "+key+ " sent to "+ nonHashedArrayOfPorts[(insertPortPos+3)%5]);

		} else if((hashedKey.compareTo(hashedArrayOfPorts[(insertPortPos+2)%5]) <= 0 && hashedKey.compareTo(hashedArrayOfPorts[(insertPortPos+1)%5]) > 0)||
				((hashedArrayOfPorts[(insertPortPos+2)%5].compareTo(hashedArrayOfPorts[(insertPortPos+1)%5]) < 0) &&
						((hashedKey.compareTo(hashedArrayOfPorts[(insertPortPos+1)%5])>0) || (hashedKey.compareTo(hashedArrayOfPorts[(insertPortPos+2)%5])<=0)))){


			Log.e(TAG,"key "+ key+ " belongs to "+nonHashedArrayOfPorts[(insertPortPos+2)%5]);
			StringBuilder sbInsert = new StringBuilder("");
			sbInsert.append(key);
			sbInsert.append(";");
			sbInsert.append(msg);


			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", sbInsert.toString(),"Yes;No;No",
					(Integer.parseInt(nonHashedArrayOfPorts[(insertPortPos+2)%5]) * 2) + "");
			Log.e(TAG,"key "+key+ " sent to "+ nonHashedArrayOfPorts[(insertPortPos+2)%5]);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", sbInsert.toString(),"No;Yes;No",
					(Integer.parseInt(nonHashedArrayOfPorts[(insertPortPos+3)%5]) * 2) + "");
			Log.e(TAG,"key "+key+ " sent to "+ nonHashedArrayOfPorts[(insertPortPos+3)%5]);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", sbInsert.toString(),"No;No;Yes",
					(Integer.parseInt(nonHashedArrayOfPorts[(insertPortPos+4)%5]) * 2) + "");
			Log.e(TAG,"key "+key+ " sent to "+ nonHashedArrayOfPorts[(insertPortPos+4)%5]);

		} else if((hashedKey.compareTo(hashedArrayOfPorts[(insertPortPos+3)%5]) <= 0 && hashedKey.compareTo(hashedArrayOfPorts[(insertPortPos+2)%5]) > 0)||
				((hashedArrayOfPorts[(insertPortPos+3)%5].compareTo(hashedArrayOfPorts[(insertPortPos+2)%5]) < 0) &&
						((hashedKey.compareTo(hashedArrayOfPorts[(insertPortPos+2)%5])>0) || (hashedKey.compareTo(hashedArrayOfPorts[(insertPortPos+3)%5])<=0)))){

			mapPredPredPort.put(key,msg);
			Log.e(TAG,"key "+ key+ " belongs to "+nonHashedArrayOfPorts[(insertPortPos+3)%5]);

			Log.e(TAG,"Key " + key + " inserted in " + myPort + " in original");
			StringBuilder sbInsert = new StringBuilder("");
			sbInsert.append(key);
			sbInsert.append(";");
			sbInsert.append(msg);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", sbInsert.toString(),"Yes;No;No",
					(Integer.parseInt(nonHashedArrayOfPorts[(insertPortPos+3)%5]) * 2) + "");
			Log.e(TAG,"key "+key+ " sent to "+ nonHashedArrayOfPorts[(insertPortPos+3)%5]);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", sbInsert.toString(),"No;Yes;No",
					(Integer.parseInt(nonHashedArrayOfPorts[(insertPortPos+4)%5]) * 2) + "");
			Log.e(TAG,"key "+key+ " sent to "+ nonHashedArrayOfPorts[(insertPortPos+4)%5]);

		} else{

			mapPredPort.put(key,msg);
			Log.e(TAG,"key "+ key+ " belongs to "+nonHashedArrayOfPorts[(insertPortPos+4)%5]);
			Log.e(TAG,"Key " + key + " inserted in " + myPort + " in original");
			StringBuilder sbInsert = new StringBuilder("");
			sbInsert.append(key);
			sbInsert.append(";");
			sbInsert.append(msg);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", sbInsert.toString(),"No;No;Yes",
					(Integer.parseInt(nonHashedArrayOfPorts[(insertPortPos+1)%5]) * 2) + "");
			Log.e(TAG,"key "+key+ " sent to "+ nonHashedArrayOfPorts[(insertPortPos+1)%5]);

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"insert", sbInsert.toString(),"Yes;No;No",
					(Integer.parseInt(nonHashedArrayOfPorts[(insertPortPos+4)%5]) * 2) + "");
			Log.e(TAG,"key "+key+ " sent to "+ nonHashedArrayOfPorts[(insertPortPos+4)%5]);
		}
		/*SystemClock.sleep(500);*/
		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort= String.valueOf((Integer.parseInt(portStr) )); //myPort is String & will have values like 5554, 5556
		for(int i = 0; i < 5; i++){
			if(nonHashedArrayOfPorts[i].equals(myPort)) {
				posOrig = i;
				posSucc = (posOrig + 1)%5;
				posPred = ((posOrig-1) < 0) ? 4 : posOrig-1;
				posSuccSucc = (posSucc + 1)%5;
				posPredPred = ((posPred-1) < 0) ? 4 : posPred-1;
				predPort = nonHashedArrayOfPorts[posPred];
				succPort = nonHashedArrayOfPorts[posSucc];
				predPredPort = nonHashedArrayOfPorts[posPredPred];
				succSuccPort = nonHashedArrayOfPorts[posSuccSucc];
				break;
			}
		}
		try {
			hashedMyPort = genHash(myPort);
			hashedPredPort = genHash(predPort);
			hashedSuccPort = genHash(succPort);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		mOpenHelper = new DatabaseHelper(getContext());
		db = mOpenHelper.getWritableDatabase();
		Cursor cursor = db.rawQuery("SELECT count(*) FROM " + mOpenHelper.TABLE_NAME,null);
		cursor.moveToFirst();
		if (cursor.getInt(0) > 0) {

			Log.e(TAG, "Rebirth of " + myPort);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"recover", (Integer.parseInt(nonHashedArrayOfPorts[posPred]) * 2) + "", "OriginalMap", "Pred");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"recover", (Integer.parseInt(nonHashedArrayOfPorts[posSucc]) * 2) + "" , "PredMap", "Original");
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"recover", (Integer.parseInt(nonHashedArrayOfPorts[posPredPred]) * 2) + "", "OriginalMap", "PredPred");

		} else {
			ContentValues contentValues = new ContentValues();
			String key = "I am born";
			contentValues.put("key", key);
			db.insert(mOpenHelper.TABLE_NAME, null, contentValues);
			Log.e(TAG,"Hail the birth of "+myPort);
		}
		cursor.close();

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
		}

		return false;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				while (true) {
					String str;
					Socket clSocket = serverSocket.accept();
					DataInputStream br = new DataInputStream(clSocket.getInputStream());
					str = br.readUTF();
					String[] tokens = str.split(";");
					DataOutputStream out = new DataOutputStream(clSocket.getOutputStream());

					if (tokens[0].equals("insert")) {
						String keyInServer=tokens[1];
						String msginServer = tokens[2];
						if(tokens[3].equals("Yes"))
							insertCurrMap(keyInServer,msginServer);
						if(tokens[4].equals("Yes"))
							insertPredMap(keyInServer,msginServer);
						if(tokens[5].equals("Yes"))
							insertPredPredMap(keyInServer,msginServer);
						/*if (mapOriginalPort.containsKey(tokens[1]))
							Log.e(TAG, "key " + tokens[1] + " overwritten");
						mapOriginalPort.put(tokens[1], tokens[2]);
						Log.e(TAG, "Server of " + myPort + " has inserted key " + tokens[1]);*/

					} else if (str.equals("QueryAll")) {
						String results = "";
						Cursor queryCursor = query(mUri, null, "@", null, null, null);
						queryCursor.moveToFirst();
						if (queryCursor.moveToFirst()) {
							do {
								results += queryCursor.getString(0);
								Log.e(TAG, "query all key is " + results);
								results += ";";
								results += queryCursor.getString(1);
								results += "/";
							} while (queryCursor.moveToNext());
						}
						if (!results.equals(""))
							Log.e(TAG, "query all result string is " + results);
						out.writeUTF(results);

					} else if (tokens[0].equals("QuerySingle")) {

						out.writeUTF(tokens[1] + ";" + mapOriginalPort.get(tokens[1]));

					} else if (tokens[0].equals("DeleteAll")) {
						deletePredMap();
						deleteCurrMap();
						deletePredPredMap();

					/*} else if(tokens[0].equals("DeleteAt")){
						if(tokens[1].equals("Yes"))
							deletePredMap();
						if(tokens[3].equals("Yes"))
							deletePredPredMap();*/

					} else if (tokens[0].equals("DeleteSingle")) {
						if(tokens[2].equals("Yes"))
							deleteSingleCurrMap(tokens[1]);
						if(tokens[3].equals("Yes"))
							deleteSinglePredMap(tokens[1]);
						if(tokens[4].equals("Yes"))
							deleteSinglePredPredMap(tokens[1]);
					}

					else if (tokens[0].equals("recover")) {
						String results = "";
						if(tokens[1].equals("OriginalMap")) {
							if (tokens[2].equals("Pred")){
								results += "UpdatePredMap" + "$";
							} else if (tokens[2].equals("PredPred")){
								results += "UpdatePredPredMap" + "$";
							}

							Iterator it = mapOriginalPort.entrySet().iterator();
							while (it.hasNext()) {
								HashMap.Entry pair = (HashMap.Entry) it.next();
								String keyInMap = (String) pair.getKey();
								String msgInMap = (String) pair.getValue();
								Log.e(TAG,"key is "+keyInMap+" msg is "+msgInMap);
								results = results + keyInMap + ";"+ msgInMap + "/";
							}
						} else if (tokens[1].equals("PredMap")){
							results += "UpdateOriginalMap" + "$";
							Iterator it = mapPredPort.entrySet().iterator();
							while (it.hasNext()) {
								HashMap.Entry pair = (HashMap.Entry) it.next();
								String keyInMap = (String) pair.getKey();
								String msgInMap = (String) pair.getValue();
								Log.e(TAG,"key is "+keyInMap+" msg is "+msgInMap);
								results = results + keyInMap + ";"+ msgInMap + "/";
							}
						}

						if (!results.equals("")) {
							out.writeUTF(results);
							Log.e(TAG, "Server side result for single query " + results);
						}
					}
				}
			} catch (IOException e) {
				Log.e(TAG,"IOException in server");
			}
			return null;
		}
	}

	public void deleteSinglePredPredMap(String keyToDelete) {
		mapPredPredPort.remove(keyToDelete);
	}

	public void deleteSingleCurrMap(String keyToDelete) {
		mapOriginalPort.remove(keyToDelete);
	}

	public void deleteSinglePredMap(String keyToDelete) {
		mapPredPort.remove(keyToDelete);
	}

	public void deletePredPredMap() {
		mapPredPredPort.clear();
	}

	public void deleteCurrMap() {
		mapOriginalPort.clear();
	}

	public void deletePredMap() {
		mapPredPort.clear();
	}

	public void insertPredPredMap(String keyReceived, String msgReceived) {
		mapPredPredPort.put(keyReceived,msgReceived);
		Log.e(TAG,"Key " + keyReceived + " inserted in " + myPort + " in succ");
	}

	public void insertCurrMap(String keyReceived, String msgReceived) {
		mapOriginalPort.put(keyReceived,msgReceived);
		Log.e(TAG,"Key " + keyReceived + " inserted in " + myPort + " in original");
	}

	public void insertPredMap(String keyReceived, String msgReceived) {
		mapPredPort.put(keyReceived,msgReceived);
		Log.e(TAG,"Key " + keyReceived + " inserted in " + myPort + " in pred");
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {
			String[] array = new String[5];
			array[0] = REMOTE_PORT0;
			array[1] = REMOTE_PORT1;
			array[2] = REMOTE_PORT2;
			array[3] = REMOTE_PORT3;
			array[4] = REMOTE_PORT4;
			String queryDetails;
			if(msgs[0].equals("recover")){
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
					String msgToSend = msgs[0] + ";" + msgs[2] + ";" + msgs[3];
					DataOutputStream pw = new DataOutputStream(socket.getOutputStream());
					pw.writeUTF(msgToSend);
					pw.flush();
					socket.setSoTimeout(1000);
					/*int posPred2 = ((posPred-1) < 0) ? 4 : posPred-1;
					int posPred3 = ((posPred2-1) < 0) ? 4 : posPred2-1;
					String hashedPred2Port = hashedArrayOfPorts[posPred2];
					String hashedPred3Port = hashedArrayOfPorts[posPred3];*/
					try {
						InputStream inFromServer = socket.getInputStream();

						DataInputStream in = new DataInputStream(inFromServer);
						queryDetails = in.readUTF();
						if(!queryDetails.isEmpty())
						{
							String[] specialTokens = queryDetails.split("$");


							if (specialTokens[0].equals("UpdateOriginalMap")) {
								String[] queryTokens = specialTokens[1].split("/");
								for (int j = 0; j < queryTokens.length; j++) {
									String[] insideTokens = queryTokens[j].split(";");
								/*Log.e(TAG, "length of insideTokens having keys and value " + insideTokens.length);*/
								/*String hashedRecoveredKey = genHash(insideTokens[0]);*/
									mapOriginalPort.put(insideTokens[0], insideTokens[1]);
								}
							} else if (specialTokens[0].equals("UpdatePredMap")) {
								String[] queryTokens = specialTokens[1].split("/");
								for (int j = 0; j < queryTokens.length; j++) {
									String[] insideTokens = queryTokens[j].split(";");
								/*Log.e(TAG, "length of insideTokens having keys and value " + insideTokens.length);*/
								/*String hashedRecoveredKey = genHash(insideTokens[0]);*/
									mapPredPort.put(insideTokens[0], insideTokens[1]);
								}
							} else if (specialTokens[0].equals("UpdatePredPredMap")) {
								String[] queryTokens = specialTokens[1].split("/");
								for (int j = 0; j < queryTokens.length; j++) {
									String[] insideTokens = queryTokens[j].split(";");
								/*Log.e(TAG, "length of insideTokens having keys and value " + insideTokens.length);*/
								/*String hashedRecoveredKey = genHash(insideTokens[0]);*/
									mapPredPredPort.put(insideTokens[0], insideTokens[1]);
								}
							}

								/*Log.e(TAG, "length of insideTokens having keys and value " + insideTokens.length);*/
								/*String hashedRecoveredKey = genHash(insideTokens[0]);*/

								/*if((hashedRecoveredKey.compareTo(hashedMyPort) <= 0 && hashedRecoveredKey.compareTo(hashedPredPort) > 0)||
										((hashedMyPort.compareTo(hashedPredPort) < 0) && ((hashedRecoveredKey.compareTo(hashedPredPort)>0) || (hashedRecoveredKey.compareTo(hashedMyPort)<=0)))){

									mapOriginalPort.put(insideTokens[0],insideTokens[1]);
									*//*Log.e(TAG,"key " +insideTokens[0] + " inserted during recovery in "+myPort+" by virtue of "+myPort);*//*

								} else if((hashedRecoveredKey.compareTo(hashedPredPort) <= 0 && hashedRecoveredKey.compareTo(hashedPred2Port) > 0)||
										((hashedPredPort.compareTo(hashedPred2Port) < 0) && ((hashedRecoveredKey.compareTo(hashedPred2Port)>0) || (hashedRecoveredKey.compareTo(hashedPredPort)<=0)))){

									mapOriginalPort.put(insideTokens[0],insideTokens[1]);
									*//*Log.e(TAG,"key " +insideTokens[0] + " inserted during recovery in "+myPort+" by virtue of "+predPort);*//*

								} else if((hashedRecoveredKey.compareTo(hashedPred2Port) <= 0 && hashedRecoveredKey.compareTo(hashedPred3Port) > 0)||
										((hashedPred2Port.compareTo(hashedPred3Port) < 0) && ((hashedRecoveredKey.compareTo(hashedPred3Port)>0) || (hashedRecoveredKey.compareTo(hashedPred2Port)<=0)))){

									mapOriginalPort.put(insideTokens[0],insideTokens[1]);
									*//*Log.e(TAG,"key " +insideTokens[0] + " inserted during recovery in "+myPort+" by virtue of "+nonHashedArrayOfPorts[posPred2]);*//*
								}*/


						}
					} catch (SocketTimeoutException ste) {
						Log.e(TAG, "Timeout exception while waiting for recovery from " + myPort);
					}
				} catch (IOException e) {
					Log.e(TAG,"IO Exception in Client while recovering");
				}
			} else if(msgs[0].equals("insert")){
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[3]));
					String msgToSend = msgs[0] + ";" + msgs[1] + ";" + msgs[2];
					DataOutputStream pw = new DataOutputStream(socket.getOutputStream());
					pw.writeUTF(msgToSend);
					pw.flush();
				} catch (UnknownHostException e) {
					Log.e(TAG, "ClientTask UnknownHostException");
				} catch (IOException e) {
					Log.e(TAG, "ClientTask socket IOException");
				}
			} else if(msgs[0].equals("QueryAll")) {
				for (int i = 0; i < 5; i++) {
					if (!array[i].equals(msgs[1])) {
						try {
							Log.d(TAG, "entered query loop client");

							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(array[i]));
							String msgToSend = msgs[0];
							DataOutputStream pw = new DataOutputStream(socket.getOutputStream());
							pw.writeUTF(msgToSend);
							pw.flush();
							socket.setSoTimeout(1000);
							try {
								InputStream inFromServer = socket.getInputStream();
								Log.e(TAG, "got query delete response from server");
								DataInputStream in = new DataInputStream(inFromServer);
								queryDetails = in.readUTF();
								Log.e(TAG, "qerydet " + queryDetails);
								MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
								if (!queryDetails.isEmpty()) {
									String[] queryTokens = queryDetails.split("/");
									for (int j = 0; j < queryTokens.length; j++) {
										String[] insideTokens = queryTokens[j].split(";");
										Log.e(TAG, "length of insideTokens having keys and value " + insideTokens.length);
										matrixCursor.addRow(new String[]{insideTokens[0], insideTokens[1]});
									}

									Cursor[] temp = {ResultAnswerCursor, matrixCursor};
									ResultAnswerCursor = new MergeCursor(temp);
									Log.e(TAG, "number of rows is " + ResultAnswerCursor.getCount());
								}
							} catch (SocketTimeoutException ste) {
								Log.e(TAG, "Timeout exception while waiting for join");
							}

						} catch (UnknownHostException e) {
							Log.e(TAG, "ClientTask UnknownHostException");
						} catch (IOException e) {
							Log.e(TAG, "ClientTask socket IOException");
						}
					}
				}
				queryResultReceived = true;
			} else if(msgs[0].equals("DeleteAll")){
				for (int i = 0; i < 5; i++) {
					if (!array[i].equals(msgs[1])) {
						try {
							Log.d(TAG, "entered delete loop client");
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(array[i]));
							String msgToSend = msgs[0];
							DataOutputStream pw = new DataOutputStream(socket.getOutputStream());
							pw.writeUTF(msgToSend);
							pw.flush();

						} catch (UnknownHostException e) {
							Log.e(TAG, "ClientTask UnknownHostException");
						} catch (IOException e) {
							Log.e(TAG, "ClientTask socket IOException");
						}
					}
				}
			} else if(msgs[0].equals("DeleteAt")){
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
					String msgToSend = msgs[0] + ";" + msgs[2];
					DataOutputStream pw = new DataOutputStream(socket.getOutputStream());
					pw.writeUTF(msgToSend);
					pw.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}

			} else if(msgs[0].equals("DeleteSingle")){
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
					String msgToSend = msgs[0] + ";" + msgs[2] + ";" + msgs[3];
					DataOutputStream pw = new DataOutputStream(socket.getOutputStream());
					pw.writeUTF(msgToSend);
					pw.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return null;
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		SystemClock.sleep(80);
		Log.e(TAG,myPort +" has received request for query "+selection);
		Cursor prCursor = null;
		if(selection.equals("@")){
			Cursor cOrig = hashMapToCursor(mapOriginalPort);
			Cursor cPred = hashMapToCursor(mapPredPort);
			Cursor cPredPred = hashMapToCursor(mapPredPredPort);
			Cursor[] temp = {cOrig, cPred, cPredPred};
			prCursor = new MergeCursor(temp);

		} else if(selection.equals("*")){
			Cursor cOrig = hashMapToCursor(mapOriginalPort);
			Cursor cPred = hashMapToCursor(mapPredPort);
			Cursor cPredPred = hashMapToCursor(mapPredPredPort);
			String requesterPort = (Integer.parseInt(myPort)*2)+""; //requesterPort is String and will have values like 11108, 11112
			Log.e(TAG,"* query request received by "+ myPort);
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "QueryAll", requesterPort);
			while (!queryResultReceived) {
			}
			queryResultReceived = false;

			Cursor[] temp = {ResultAnswerCursor, cOrig, cPred, cPredPred};
			prCursor = new MergeCursor(temp);

			if (ResultAnswerCursor != null)
				Log.e(TAG, "final * cursor :" + ResultAnswerCursor.getCount());
			ResultAnswerCursor = null;
			Log.e(TAG, "final * cursor :" + prCursor.getCount());

		} else{
			int queryPortPos = posOrig;
			try {
				String messageHashedKey = genHash(selection);
				if((messageHashedKey.compareTo(hashedMyPort) <= 0 && messageHashedKey.compareTo(hashedPredPort) > 0)||
						((hashedMyPort.compareTo(hashedPredPort) < 0) && ((messageHashedKey.compareTo(hashedPredPort)>0) || (messageHashedKey.compareTo(hashedMyPort)<=0)))){

					Log.e(TAG,selection + " queried at "+myPort+" is found at "+myPort);
					MatrixCursor matrixCursor = new MatrixCursor(new String[] {"key","value"});
					String valueInMap = mapOriginalPort.get(selection);
					matrixCursor.addRow(new String[]{selection,valueInMap});
					prCursor = matrixCursor;
					Log.e(TAG,selection + " queried at "+myPort+" is found at "+myPort + " and now returned ");

				} else if((messageHashedKey.compareTo(hashedArrayOfPorts[(queryPortPos+1)%5]) <= 0 && messageHashedKey.compareTo(hashedArrayOfPorts[queryPortPos]) > 0)||
						((hashedArrayOfPorts[(queryPortPos+1)%5].compareTo(hashedArrayOfPorts[queryPortPos]) < 0) &&
								((messageHashedKey.compareTo(hashedArrayOfPorts[queryPortPos])>0) || (messageHashedKey.compareTo(hashedArrayOfPorts[(queryPortPos+1)%5])<=0)))){

					Log.e(TAG,selection + " queried at "+myPort+" is found at "+nonHashedArrayOfPorts[(queryPortPos+1)%5]);
					prCursor = querySingle(selection, Integer.parseInt(nonHashedArrayOfPorts[(queryPortPos+1)%5]) * 2);

					if(queryFailedFlag){
						queryFailedFlag = false;
						prCursor = querySingle(selection, Integer.parseInt(nonHashedArrayOfPorts[(queryPortPos+2)%5]) * 2);
					}
					Log.e(TAG,selection + " queried at "+myPort+" is found at "+nonHashedArrayOfPorts[(queryPortPos+1)%5] + " and now returned ");

				} else if((messageHashedKey.compareTo(hashedArrayOfPorts[(queryPortPos+2)%5]) <= 0 && messageHashedKey.compareTo(hashedArrayOfPorts[(queryPortPos+1)%5]) > 0)||
						((hashedArrayOfPorts[(queryPortPos+2)%5].compareTo(hashedArrayOfPorts[(queryPortPos+1)%5]) < 0) &&
								((messageHashedKey.compareTo(hashedArrayOfPorts[(queryPortPos+1)%5])>0) || (messageHashedKey.compareTo(hashedArrayOfPorts[(queryPortPos+2)%5])<=0)))){

					Log.e(TAG,selection + " queried at "+myPort+" is found at "+nonHashedArrayOfPorts[(queryPortPos+2)%5]);
					prCursor = querySingle(selection, Integer.parseInt(nonHashedArrayOfPorts[(queryPortPos+2)%5]) * 2);

					if(queryFailedFlag){
						queryFailedFlag = false;
						prCursor = querySingle(selection, Integer.parseInt(nonHashedArrayOfPorts[(queryPortPos+3)%5]) * 2);
					}
					Log.e(TAG,selection + " queried at "+myPort+" is found at "+nonHashedArrayOfPorts[(queryPortPos+2)%5] + " and now returned ");

				} else if((messageHashedKey.compareTo(hashedArrayOfPorts[(queryPortPos+3)%5]) <= 0 && messageHashedKey.compareTo(hashedArrayOfPorts[(queryPortPos+2)%5]) > 0)||
						((hashedArrayOfPorts[(queryPortPos+3)%5].compareTo(hashedArrayOfPorts[(queryPortPos+2)%5]) < 0) &&
								((messageHashedKey.compareTo(hashedArrayOfPorts[(queryPortPos+2)%5])>0) || (messageHashedKey.compareTo(hashedArrayOfPorts[(queryPortPos+3)%5])<=0)))){

					Log.e(TAG,selection + " queried at "+myPort+" is found at "+nonHashedArrayOfPorts[(queryPortPos+3)%5]);
					prCursor = querySingle(selection, Integer.parseInt(nonHashedArrayOfPorts[(queryPortPos+3)%5]) * 2);

					if(queryFailedFlag){
						queryFailedFlag = false;
						prCursor = querySingle(selection, Integer.parseInt(nonHashedArrayOfPorts[(queryPortPos+4)%5]) * 2);
					}
					Log.e(TAG,selection + " queried at "+myPort+" is found at "+nonHashedArrayOfPorts[(queryPortPos+3)%5] + " and now returned ");

				} else {
					Log.e(TAG,selection + " queried at "+myPort+" is found at "+nonHashedArrayOfPorts[(queryPortPos+4)%5]);

					prCursor = querySingle(selection, Integer.parseInt(nonHashedArrayOfPorts[(queryPortPos + 4) % 5]) * 2);

					if(queryFailedFlag){
						queryFailedFlag = false;
						prCursor = querySingle(selection, Integer.parseInt(nonHashedArrayOfPorts[(queryPortPos+5)%5]) * 2);
					}
					Log.e(TAG,selection + " queried at "+myPort+" is found at "+nonHashedArrayOfPorts[(queryPortPos+4)%5] + " and now returned ");
				}

			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		// TODO Auto-generated method stub
		return prCursor;
	}

	public Cursor querySingle(String selection, int i) {
		try {
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), i);
			String msgToSend = "QuerySingle" + ";" + selection;
			DataOutputStream pw = new DataOutputStream(socket.getOutputStream());
			pw.writeUTF(msgToSend);
			pw.flush();
			socket.setSoTimeout(1000);
			try {
				InputStream inFromServer = socket.getInputStream();
				DataInputStream in = new DataInputStream(inFromServer);
				String[] insideTokens = in.readUTF().split(";");

				MatrixCursor matrixCursor = new MatrixCursor(new String[] {"key","value"});
				matrixCursor.addRow(new String[]{insideTokens[0],insideTokens[1]});
				ResultAnswerCursor =  matrixCursor;
				Log.e(TAG,"Client Side result for query for key: " + insideTokens[0] + " msg is "+ insideTokens[1]);


			} catch (SocketTimeoutException ste) {
				Log.e(TAG, "Timeout exception while waiting for query result for " + myPort);
			}
		} catch (IOException e) {
			queryFailedFlag  = true;
			Log.e(TAG,"IO Exception in Client while QuerySingle");
		}
		return ResultAnswerCursor;
	}

	private Cursor hashMapToCursor(HashMap<String, String> map) {
		Log.e(TAG,"hashMapToCursor func called by "+myPort);
		MatrixCursor matrixCursor = new MatrixCursor(new String[] {"key","value"});
		Iterator it = map.entrySet().iterator();
		while (it.hasNext()) {
			HashMap.Entry pair = (HashMap.Entry) it.next();
			String keyInMap = (String) pair.getKey();
			String msgInMap = (String) pair.getValue();
			Log.e(TAG,"key is "+keyInMap+" msg is "+msgInMap);
			matrixCursor.addRow(new String[]{keyInMap, msgInMap});
			/*it.remove();*/
		}
		Log.e(TAG,"matrixCursor count is "+matrixCursor.getCount()+"");
		return matrixCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
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