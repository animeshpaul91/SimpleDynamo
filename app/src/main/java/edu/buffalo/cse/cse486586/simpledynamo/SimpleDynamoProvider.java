package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

public class SimpleDynamoProvider extends ContentProvider {

	//Variables that we will need
	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	private static String ePort = null; //Emulator Port
	private static String myPortHash;
	private static final int SERVER_PORT = 10000;
	private static final String key_field = "key";
	private static final String  value_field = "value";
	private static final String my_dht = "@";
	private static final String all_dht = "*";
	private static final String IC = "IC"; //Forward Insert request to Coordinator and 2 Successors
	private static final String DA = "DA"; //Delete all Keys from entire Ring
	private static final String D = "D"; //Delete specific key
	private static final String QA = "QA"; //Query all keys
	private static final String Q = "Q"; //Query specific key
	private static final String S = "S"; //Synchronize signal
	private static final String P = "P"; //Ping next node
	private static final String A = "ACK"; //Send Ack on Ping
	private static Uri provideruri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider"); //URI
	List<Node> nodeList;
	private static ArrayList<String> REMOTE_PORTS = new ArrayList<String>();
	private static int no_of_avds;
	private static String prev2prevNode, prevNode, nextnode;

	private class Node implements Comparable<Node>
	{
		String port, hash, succ = null, nextsucc=null, pred = null, prepred=null; //Every Object stores its predecessor and its next two successors.

		Node(String port, String hash)
		{
			this.port = port;
			this.hash = hash;
		}

		@Override
		public String toString()
		{
			return ("NodeID: "+this.port+" Pred: "+this.pred+" PrePred: "+this.prepred+" Succ: "+this.succ+" NextSucc: "+this.nextsucc);
		}

		@Override
		public int compareTo(Node another) {
			return this.hash.compareTo(another.hash);
		}
	}

	public String getHash(String key)
	{
		String keyhash=null;
		try
		{
			keyhash = genHash(key); //Hash of current key
		}
		catch (NoSuchAlgorithmException e)
		{
			Log.e(TAG, "Main_Delete: "+ePort+" No Such Algorithm Exception Occurred");
			e.printStackTrace();
		}
		return keyhash;
	}

	public void set_remote_ports()
	{
		int start = 5554, end = 5562;
		for (int i = start; i<=end; i+=2)
			REMOTE_PORTS.add(Integer.toString(i));
		no_of_avds = REMOTE_PORTS.size();
	}

	public boolean compareKey(String prevHash, String myHash, String keyHash)
	{
		if (myHash.compareTo(prevHash) > 0)
		{
			if (keyHash.compareTo(prevHash) > 0 && keyHash.compareTo(myHash) <= 0)
				return true;
			else
				return false;
		}
		else //Special Condition between First Node and Last Node
		{
			if (keyHash.compareTo(prevHash) > 0 || keyHash.compareTo(myHash) <= 0)
				return true;
			else
				return false;
		}
	}

	public String sendMsgCT(String msg)
	{
		String result=null;

		try {
			result = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg).get();
		}

		catch (InterruptedException e)
		{
			Log.e(TAG, "Main: "+ePort+" Interrupted Exception Occurred");
		}
		catch (ExecutionException e)
		{
			Log.e(TAG, "Main: "+ePort+" Execution Exception Occurred");
		}

		catch (CancellationException e)
		{
			Log.e(TAG, "Main: "+ePort+" Cancellation Exception Occurred");
		}
		return result;
	}

	public void set_neighbors()
	{
		int i;
		//Setting succ and pred
		for (i=0;i<no_of_avds;i++)
		{
			nodeList.get(i).succ = nodeList.get((i + 1) % no_of_avds).port;
			nodeList.get(i).nextsucc = nodeList.get((i + 2) % no_of_avds).port;
			nodeList.get(i).pred = nodeList.get((i + 4) % no_of_avds).port;
			nodeList.get(i).prepred = nodeList.get((i + 3) % no_of_avds).port;

			if (nodeList.get(i).port.equals(ePort)) //this gets only executed once
			{
				prev2prevNode = nodeList.get(i).prepred;
				prevNode = nodeList.get(i).pred;
				nextnode = nodeList.get(i).succ;
			}
		}
	}

	public void synchronize_keys() {
		String msgtosend = S + ";" + prev2prevNode + ";" + prevNode + ";" + nextnode+";"+ePort;
		String[] source_nodes = {prev2prevNode, prevNode, nextnode};
		FileOutputStream fileOutputStream;
		Context con = getContext();
		String result = sendMsgCT(msgtosend);
		Log.d(TAG, "Main: " + ePort + " Synchronization Beginning at Recovered Node: " + ePort);
		int n = -1;

		String[] keyvalues = result.split("#");
		for (String keyvalue : keyvalues) {
			n++;
			String[] temp = keyvalue.split("\\|");
			for (String t : temp) {
				String[] kv = t.split(":");
				try {
					List<String> myFiles = Arrays.asList(con.fileList());
					if (!myFiles.contains(kv[0])) {
						fileOutputStream = con.openFileOutput(kv[0], Context.MODE_PRIVATE);
						fileOutputStream.write(kv[1].getBytes());
						fileOutputStream.close();
						Log.d(TAG, "Main_Sync: " + ePort + " Synchronized Key: " + kv[0] + " and Value: " + kv[1] + " from Node: " + source_nodes[n]);
					}
				} catch (FileNotFoundException e) {
					Log.e(TAG, "Main_Sync: " + ePort + " FileNotfound Exception Occurred");
					e.printStackTrace();
				} catch (IOException e) {
					Log.e(TAG, "Main_Sync: " + ePort + " IO Exception Occurred");
					e.printStackTrace();
				}
			}
		}
		Log.d(TAG, "Main: "+ePort+" Synchronization Completed at Recovered Node: "+ePort);
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String key = selection, keyhash=null;
		Context con = getContext();
		List<String> myFiles = Arrays.asList(con.fileList());
		String msgtosend;

		if (key.equals(my_dht)) //Delete all files in my Avd
		{
			for (String file: myFiles)
				con.deleteFile(file);
			Log.d(TAG, "Main_Delete: "+ePort+" All files from this Avd are deleted");
			return 1;
		}

		else if (key.equals(all_dht)) //Delete all files in entire Ring
		{
			msgtosend = DA+";";
			sendMsgCT(msgtosend); //Signal all avds to delete all files
			return 1;
		}

		else //Any Particular Key
		{
			keyhash = getHash(key);
			for (Node node : nodeList) //Find right partition for the key and forward msg to Coordinator
			{
				String prevHash = getHash(node.pred);
				String myHash = getHash(node.port);
				boolean result = compareKey(prevHash, myHash, keyhash);
				if (result)
				{
					msgtosend = D+";"+key+";"+node.port+";"+node.succ+";"+node.nextsucc;
					sendMsgCT(msgtosend);
					Log.d(TAG, "Main_Delete: "+ePort+" Key: "+key+" deleted from Coordinator and Replicas");
					return 1;
				}
			}
			return 0;
		}
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key, value, keyhash = null;
		key = values.getAsString(key_field);
		value = values.getAsString(value_field);
		boolean result;
		keyhash = getHash(key);

		for (Node node : nodeList) //Find right partition for the key and forward msg to Coordinator
		{
			String prevHash = getHash(node.pred);
			String myHash = getHash(node.port);
			result = compareKey(prevHash, myHash, keyhash);
			if (result)
			{
				String msgtosend = IC+";"+key+";"+value+";"+node.port+";"+node.succ+";"+node.nextsucc;
				String ack = sendMsgCT(msgtosend);
				Log.d(TAG, "Main_Insert: "+ePort+" Insert Ack received" +ack);
				break;
			}
		}
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		ePort = String.valueOf(Integer.parseInt(portStr)); //Emulator Port ( eg.5554)
		Log.d(TAG, "Main_Oncreate: "+ePort+" My Port is: "+ePort);
		myPortHash = getHash(ePort);

		set_remote_ports();
		Log.d(TAG, "Main_Oncreate: "+ePort+" Remote Ports Obtained");
		nodeList = new ArrayList<Node>(); //Instantiate nodeList

		for (int i=0;i < no_of_avds; i++) //Every Avd stores the Global state of the Dynamo Ring
		{
			String port = REMOTE_PORTS.get(i);
			String currentHash = getHash(port);
			Node node = new Node(port, currentHash);
			nodeList.add(node);
		}

		Collections.sort(nodeList);

		try{
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}
		catch (IOException e)
		{
			Log.e(TAG, "Main_Oncreate: "+ePort+" Can't Create a ServerSocket");
			e.printStackTrace();
			return false;
		}

		set_neighbors(); //sets neighbours for all Nodes in System
		String ack = sendMsgCT(P+";"+nextnode);
		String []temp = ack.split("#");

		if (temp[0].equals(A)) { //Recovery Condition
			Log.d(TAG, "Main_Oncreate: "+ePort+" I have just Recovered from a Failure");
			Log.d(TAG, "Main_Oncreate: "+ePort+" Deleting all stale files first");
			List <String> myFiles = Arrays.asList(getContext().fileList());
			for (String file: myFiles)
				getContext().deleteFile(file);
			Log.d(TAG, "Main_OnCreate: "+ePort+" Sending Key Sync Signal to Nodes: "+prev2prevNode+" ,"+prevNode+" ,"+nextnode);
			synchronize_keys();
		}
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		String key = selection, keyhash=null;
		FileInputStream fileInputStream;
		String[] columns = {key_field, value_field};
		MatrixCursor mc = new MatrixCursor(columns);
		String[] splitter;
		String value, msgtosend;
		Context con = getContext();
		List<String> myFiles = Arrays.asList(con.fileList());

		if (key.equals(my_dht)) //return all records in my avd
		{
			for (String file : myFiles)
			{
				try
				{
					fileInputStream = con.openFileInput(file);
					BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream));
					splitter = br.readLine().split(";");
					value = splitter[0];
					String[] row = {file, value};
					mc.addRow(row);
					br.close();
					fileInputStream.close();
				}
				catch (IOException e)
				{
					Log.e(TAG, "Main_Query: "+ePort+" IO Exception Occurred in Opening File for Read Operation");
				}
			}
			return mc;
		}

		else if (key.equals(all_dht)) //return all records in entire ring
		{
			msgtosend = QA+";";
			String result = sendMsgCT(msgtosend); //Signal all avds to run @ query
			String [] nodes = result.split("#");
			for (String node: nodes)
			{
				String [] keyvalues = node.split(";");
				for (String keyvalue: keyvalues)
				{
					String []kv = keyvalue.split(":");
					String []row = {kv[0], kv[1]};
					mc.addRow(row);
				}
			}
			return mc;
		}

		else //return the most recent value for a particular key
		{
			List <String> values = new ArrayList<String>();
			List<Integer> versions = new ArrayList<Integer>();
			String val_versions=null;
			keyhash = getHash(key); //Hash of current key

			for (Node node : nodeList) //Find right partition for the key and forward msg to Coordinator
			{
				String prevHash = getHash(node.pred);
				String myHash = getHash(node.port);
				boolean result = compareKey(prevHash, myHash, keyhash);
				if (result)
				{
					msgtosend = Q+";"+key+";"+node.port+";"+node.succ+";"+node.nextsucc;
					val_versions  = sendMsgCT(msgtosend);
					Log.d(TAG, "Server: "+ePort+" Values and Versions Received "+val_versions);
					break;
				}
			}

			String[] v = val_versions.split("#");
			for (String vers: v)
			{
				String[] temp = vers.split(";");
				values.add(temp[0]);
				versions.add(Integer.parseInt(temp[1]));
			}
			Log.d(TAG, "Main_Query: "+ePort+" Values for Key "+values.toString());
			Log.d(TAG, "Main_Query: "+ePort+" Versions for Key "+versions.toString());

			int maxindex = versions.indexOf(Collections.max(versions));
			Log.d(TAG, "Main_Query: "+ePort+" Max Version Returned: "+values.get(maxindex));
			String[] row = {key, values.get(maxindex)};
			mc.addRow(row);
			return mc;
		}
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

	//Server Task starts here
	private class ServerTask extends AsyncTask<ServerSocket, String, Void>
	{
		@Override
		protected Void doInBackground(ServerSocket... sockets)
		{
			ServerSocket serverSocket = sockets[0];
			String msgfromclient;
			String [] pieces;
			Context con = getContext();

			while (true)
			{
				try {
					Socket server = serverSocket.accept();
					DataInputStream in = new DataInputStream(server.getInputStream());
					DataOutputStream out = new DataOutputStream(server.getOutputStream());
					msgfromclient = in.readUTF();
					pieces = msgfromclient.split(";");
					Log.d(TAG, "Server: "+ePort+" Received Message: "+msgfromclient);

					if (pieces[0].equals(IC))
					{
						List<String> myFiles = Arrays.asList(con.fileList());
						String key = pieces[1], version=null, splitter[];
						FileOutputStream fileOutputStream;
						FileInputStream fileInputStream;
						if (myFiles.contains(key)) //If the file already exists in AVD, re-version it
						{
							Log.d(TAG, "Server: "+ePort+" File: "+key+" Located with Stale Version");
							fileInputStream = con.openFileInput(key);

							if (fileInputStream != null) {
								BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream));
								splitter = br.readLine().split(";");
								int curr_version = Integer.parseInt(splitter[1]); //Existing Version
								version = Integer.toString(curr_version + 1);
								Log.d(TAG, "Server: " + ePort + " Version of file: " + key + " updated from " + curr_version + " to " + version);
								br.close();
								fileInputStream.close();
							}
						}
						else
							version = "1"; //New File
						String value = pieces[2]+";"+version; //value;Version written to File
						try
						{
							fileOutputStream = con.openFileOutput(key, Context.MODE_PRIVATE);
							fileOutputStream.write(value.getBytes());
							fileOutputStream.close();
							out.writeUTF(ePort); //Acknowledging Insert
							Log.d(TAG, "Server: "+ePort+" File Created with Key: "+key+" and Value: "+value);
							out.flush();
							out.close();
							in.close();
						}
						catch (NullPointerException e)
						{
							Log.e(TAG, "Server_Insert: " +ePort+ " Nullpointer Exception Occurred");
							e.printStackTrace();
						}
						catch (IOException e)
						{
							Log.e(TAG, "Server_Insert: " +ePort+ " IO Exception Occurred");
							e.printStackTrace();
						}
					}

					else if (pieces[0].equals(DA)) //Delete all keys
					{
						con.getContentResolver().delete(provideruri, my_dht, null);
						Log.d(TAG, "Server: "+ePort+" All keys in my Avd are deleted");
						out.writeUTF(ePort);
						out.flush();
						out.close();
						in.close();
					}

					else if (pieces[0].equals(D))
					{
						String key = pieces[1];
						con.deleteFile(key);
						out.writeUTF(ePort);
						out.flush();
						out.close();
						in.close();
					}

					else if (pieces[0].equals(QA)) //Delete all keys
					{
						String result="";
						Cursor cursor = con.getContentResolver().query(provideruri, null, my_dht, null, null);
						while (cursor.moveToNext())
						{
							result+=cursor.getString(cursor.getColumnIndex(key_field)); //Key
							result+=":"+cursor.getString(cursor.getColumnIndex(value_field)); //Value
							result+=";";
						}
						cursor.close();
						out.writeUTF(result);
						out.flush();
						out.close();
						in.close();
						Log.d(TAG, "Server: "+ePort+" All Keys in my Avd are queried");
					}

					else if (pieces[0].equals(Q)) //Query particular key
					{
						String key = pieces[1];
						FileInputStream fileInputStream;
						String message;
						fileInputStream = con.openFileInput(key);

						if (fileInputStream != null)
						{
							BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream));
							message = br.readLine();
							out.writeUTF(message);
							out.flush();
							br.close();
							fileInputStream.close();
							out.close();
							in.close();
						}
					}

					else if (pieces[0].equals(P)) //Ping request
					{
						List <String> myFiles = Arrays.asList(con.fileList());
						if(!myFiles.isEmpty())
							out.writeUTF(A);
						out.flush();
						out.close();
						in.close();
					}

					else //Synchronize keys
					{
						List <String> myFiles = Arrays.asList(con.fileList());
						FileInputStream fileInputStream;
						String keyHash, prevHash, prev2prevHash, message, object="";
						boolean result;
						prev2prevHash = getHash(prev2prevNode);
						prevHash = getHash(prevNode);
						for (String file: myFiles)
						{
							keyHash = getHash(file);

							if (pieces[3].equals(ePort)) //I am the successor of failed node
								result = compareKey(prev2prevHash, prevHash, keyHash);
							else //I am either of the Predecessors
								result = compareKey(prevHash, myPortHash, keyHash);

							if (result)
							{
								fileInputStream = con.openFileInput(file);
								BufferedReader br = new BufferedReader(new InputStreamReader(fileInputStream));
								message = br.readLine();
								object+=file+":"+message+"|";
								br.close();
								fileInputStream.close();
							}
						}
						out.writeUTF(object);
						System.out.println("Server: "+ePort+" Sent Message: "+object);
						out.flush();
						out.close();
						in.close();
						Log.d(TAG, "Server: "+ePort+" Correct Keys from my node sent to Recovered Node: "+pieces[4]);
					}
				}
				catch (IOException e)
				{
					Log.e(TAG, "Server: " +ePort+ " IO Exception Occurred");
					e.printStackTrace();
				}
			}
		}
	}

	private class ClientTask extends AsyncTask<String, Void, String> //Capable of returning a string type
	{
		@Override
		protected String doInBackground(String... msgs)
		{
			String[] pieces = msgs[0].split(";");
			String ack;

			if (pieces[0].equals(IC)) //send insertion msg to Coordinator and its 2 successors
			{
				String[] p = {pieces[3], pieces[4], pieces[5]};
				List <String> ports = Arrays.asList(p);
				String msgToserver = IC+";"+pieces[1]+";"+pieces[2];
				ack = send_to_server(ports, msgToserver);
				return ack;
			}

			else if (pieces[0].equals(DA) || pieces[0].equals(QA)) //delete all messages or Query @ across all Avd's
			{
				List <String> ports = new ArrayList<String>(REMOTE_PORTS);
				ack = send_to_server(ports, msgs[0]);
				return ack;
			}

			else if (pieces[0].equals(D)) //delete particular key
			{
				String[] p = {pieces[2], pieces[3], pieces[4]};
				List <String> ports = Arrays.asList(p);
				String msgToserver = D+";"+pieces[1]; //D;key
				ack = send_to_server(ports, msgToserver);
				return ack;
			}

			else if (pieces[0].equals(Q)) //Query particular key
			{
				String[] p = {pieces[2], pieces[3], pieces[4]};
				List <String> ports = Arrays.asList(p);
				String msgToserver = Q+";"+pieces[1]; //Q;key
				ack = send_to_server(ports, msgToserver);
				return ack;
			}

			else if (pieces[0].equals(P)) //Ping next node
			{
				String[] p = {pieces[1]};
				List <String> ports = Arrays.asList(p);
				ack = send_to_server(ports, msgs[0]);
				return ack;
			}

			else //Synchronize keys
			{
				String[] p = {pieces[1], pieces[2], pieces[3]};
				List <String> ports = Arrays.asList(p);
				ack = send_to_server(ports, msgs[0]);
				return ack;
			}
		}

		private String send_to_server(List<String> ports, String msgToserver)
		{
			String response = "", msgfromserver; //Will store the Votes/responses from Server
			for (String port: ports)
			{
				try
				{
					Socket client = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port) * 2);
					client.setSoTimeout(500);
					DataInputStream in = new DataInputStream(client.getInputStream());
					DataOutputStream out = new DataOutputStream(client.getOutputStream());
					out.writeUTF(msgToserver);
					Log.d(TAG, "Client: "+ePort+" Sent Message: "+msgToserver+" to Node: "+port);
					out.flush();
					msgfromserver = in.readUTF();
					Log.d(TAG, "Client: "+ePort+" Received Response: "+msgfromserver+" from Node: "+port);
					response += msgfromserver;
					response += "#";
					out.close();
					in.close();
					client.close();
				}
				catch (SocketTimeoutException e)
				{
					Log.e(TAG, "Client: " + ePort + " Socket Timeout Exception Occurred");
					Log.e(TAG, "Client: " + ePort + " Node: "+port+" has failed");
					e.printStackTrace();
				}

				catch (SocketException e)
				{
					Log.e(TAG, "Client: " + ePort + " Socket Exception Occurred");
					Log.e(TAG, "Client: " + ePort + " Node: "+port+" has failed");
					e.printStackTrace();
				}

				catch (IOException e)
				{
					Log.e(TAG, "Client: " + ePort + " IOException Occurred");
				}
				catch (Exception e)
				{
					Log.e(TAG, "Client: " + ePort + " IOException Occurred");
					e.printStackTrace();
				}
			}
			Log.d(TAG, "Client: "+ePort+" Combined Response: "+response);
			return (response);
		}
	}
}