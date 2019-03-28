package com.chunkserver;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
//import java.util.Arrays;

import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.FileWriter;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.net.InetAddress;

import java.nio.ByteBuffer;

import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "csci485/";	//or C:\\newfile.txt
	public static long counter;
	
	private static ObjectOutputStream oos;
	private static ObjectInputStream ois;
	private static ServerSocket ss;
	private static Socket s;
	private static int port;
	private static String ip;
	final static String configFilePath = "config/";//TODO: change to final static and make a path
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
		port = 0;
		
		File configFileFolder = new File(configFilePath);
		configFileFolder.mkdirs();
		
		File configFile = new File(configFilePath, "configFile");
		try {
			configFile.createNewFile();
		} catch(IOException ioe) {
			System.out.println("configFile ioe: " + ioe.getMessage());
		}
				
		File dir = new File(filePath);
		dir.mkdirs();
		File[] fs = dir.listFiles();

		//determine state of counter based on number of files already in place
		if(fs.length == 0){
			counter = 0;
		} else{
			long[] cntrs = new long[fs.length];
			for (int j=0; j < cntrs.length; j++)
				cntrs[j] = Long.valueOf( fs[j].getName() ); 
			
			Arrays.sort(cntrs);
			counter = cntrs[cntrs.length - 1];
		}
	}
	
	public static void main(String[] args) {
		startChunkServer();
	}
	
	
	public static void startChunkServer() {
		ChunkServer cs = new ChunkServer();
		try {
			ss = new ServerSocket(port);
			port = ss.getLocalPort(); //get port the server bound to
			//TODO write port to config file
			
			System.out.println("ChunkServer bound to port: " + port);
			
			//get ip address
			try {
	            ip = InetAddress.getLocalHost().toString();	            
	        } catch (UnknownHostException e) {
	        	System.out.println("ChunkServer constructor: " + e.getMessage());
	        }
			
			//Write port to config file
			FileWriter fw = new FileWriter(configFilePath + "configFile");
			PrintWriter pw = new PrintWriter(fw);
			pw.println(ip + " : " + Integer.toString(port));
			pw.close();
		} catch(IOException ioe) {
			System.out.println("ioe in startChunkServer binding to port: " + ioe.getMessage());
			return;
		}
		
		System.out.println("Now listening for client connections");
		//continually listen on this port for new connections from clients
		while(true) {
			try {
				//when new client connects, create new socket to communicate through
				s = ss.accept();
				System.out.println("Connection from: " + s.getInetAddress());
				
				oos = new ObjectOutputStream(s.getOutputStream());
				ois = new ObjectInputStream(s.getInputStream());
				
				//continue reading requests until it is closed
				while(!s.isClosed()) {
					//read payload size
					int payloadsize = getPayloadInt(ois);
					if(payloadsize == -1) break;
					
					//read in command identifier and switch based on that
					int command = getPayloadInt(ois);
					
					if(command == InitializeChunk) {
						System.out.println("Received command to initialize chunk");
						//response
						String chunkHandle = cs.initializeChunk(); //allocate chunk here
						byte[] payload = chunkHandle.getBytes();
						//write chunk handle size
						oos.writeInt(4 + payload.length);
						//write chunkHandle
						oos.write(payload);
						oos.flush();
					}
					else if(command == PutChunk) {
						System.out.println("Received command to put chunk");
						int chunkHandleLength = getPayloadInt(ois);
						byte[] chunkHandle = getPayload(ois, chunkHandleLength);
						String handle = new String(chunkHandle);
						
						int payLoadLength = getPayloadInt(ois);
						byte[] payload = getPayload(ois, payLoadLength);
						
						int offset = getPayloadInt(ois);
												
						//send response
						boolean result = cs.putChunk(handle, payload, offset);
						int resultInt = result ? 1 : 0;
						oos.writeInt(4);
						//0 is false, 1 is true
						oos.writeInt(resultInt);
						oos.flush();						
					}
					else if(command == GetChunk) {
						//TODO
						System.out.println("Received command to get chunk");
						
						int offset = getPayloadInt(ois);
						int numberOfBytes = getPayloadInt(ois);
						String handle = new String(getPayload(ois, payloadsize-16));
						
						byte[] chunk = new byte[numberOfBytes];
						chunk = cs.getChunk(handle, offset, numberOfBytes);
						
						oos.write(chunk);
						oos.flush();
						
						
					}
					else {
						System.out.println("Could not parse command");
					}
				}
			} catch(IOException ioe) {
				System.out.println(ioe.getMessage());
			}
		}
	}
	
	
	
	public static int getPayloadInt(ObjectInputStream ois) {
		byte[] payload = new byte[4];
		int result = -2;
		try {
			result = ois.read(payload, 0, 4);
		} catch(IOException ioe) {
			System.out.println("ioe in getPayloadInt on server: " + ioe.getMessage());
		}

		if(result == -1) return result;
		return (ByteBuffer.wrap(payload)).getInt();
		
	}
	
	
	public static byte[] getPayload(ObjectInputStream ois, int payloadSize) {
		byte[] payload = new byte[payloadSize];
		byte[] temp = new byte[payloadSize];
		int totalRead = 0;
		
		while(totalRead != payloadSize) {
			int currRead = -1;
			try {
				//read bytes from stream into byte array and add byte by byte to final
				//payload byte array
				currRead = ois.read(temp, 0, (payloadSize - totalRead));
				for(int i=0; i < currRead; i++) {
					payload[totalRead + i] = temp[i];
				}
			} catch(IOException ioe) {
				System.out.println("ChunkServer getPayload ioe: " + ioe.getMessage());
				try {
					s.close();
					System.out.println("closed client socket connection");
				} catch(IOException ioe2) {
					System.out.println("ioe in closing client socket connection: " + ioe2.getMessage());
				}
				return null;
			}
			if(currRead == -1) {
				System.out.println("error in reading payload");
				return null;
			} else {
				totalRead += currRead;
			}
		}
		return payload;
	}
	
	
	
	
	
	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String initializeChunk() {
		counter++;
		String chunkHandle = String.valueOf(counter);

		File newChunk = new File(filePath, chunkHandle);
		
		try {
			newChunk.createNewFile();
		} catch(IOException ioe) {
			System.out.println("initializeChunk ioe: " + ioe.getMessage());
		}
		
		return chunkHandle;
	}
	
	/**
	 * Write the byte array to the chunk at the offset
	 * The byte array size should be no greater than 4KB
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		try {
			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.write(payload, 0, payload.length);
			raf.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	/**
	 * read the chunk at the specific offset
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		try {
			//If the file for the chunk does not exist the return null
			boolean exists = (new File(filePath + ChunkHandle)).exists();
			if (exists == false) return null;
			
			//File for the chunk exists then go ahead and read it
			byte[] data = new byte[NumberOfBytes];
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.read(data, 0, NumberOfBytes);
			raf.close();
			return data;
		} catch (IOException ex){
			ex.printStackTrace();
			return null;
		}
	}
}
