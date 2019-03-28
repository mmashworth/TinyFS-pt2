package com.client;

import com.chunkserver.ChunkServer;
import com.interfaces.ClientInterface;

import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.io.FileReader;
import java.io.File;
import java.io.BufferedReader;


import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

/**
 * implementation of interfaces at the client side
 * @author Shahram Ghandeharizadeh
 *
 */
public class Client implements ClientInterface {
	//public static ChunkServer cs = new ChunkServer();
	
	private static ObjectOutputStream oos;
	private static ObjectInputStream ois;
	private static Socket s;
	private String ip;
	private int port;
	final static String configFilePath = "config/";
	
	/**
	 * Initialize the client
	 */
	public Client(){
		if (s != null) return;
		try {
			FileReader fr = new FileReader(new File(configFilePath + "configFile"));
			
			BufferedReader bufferedReader = new BufferedReader(fr);
			String line;
			String lastEntry = "";
			while ((line = bufferedReader.readLine()) != null) {
				lastEntry = line;
			}
			fr.close();
			port = Integer.parseInt(lastEntry.substring(lastEntry.indexOf(':')+2));
			//ip = lastEntry.substring(0, lastEntry.indexOf(':'));
			//ip = ip.substring(ip.indexOf("/")+1);
			ip = "127.0.0.1";

			s = new Socket(ip, port);
			
			oos = new ObjectOutputStream(s.getOutputStream());
			oos.flush();

			ois = new ObjectInputStream(s.getInputStream());

		} catch(IOException ioe) {
			System.out.println("ioe in Client constructor: " + ioe.getMessage());
		}
	}
	
	public void close() {
		try {
			oos.close();
			ois.close();
			s.close();
		} catch(IOException ioe) {
			System.out.println("ioe in closing: " + ioe.getMessage());
		}
	}
	
	
	
	/**
	 * Create a chunk at the chunk server from the client side.
	 */
	public String initializeChunk() {
		
		//write payload size
		try {
			oos.writeInt(4 + 4);
			oos.writeInt(1);
			oos.flush();
			
			int resultSize = getPayloadInt(ois);
			String handle = new String(  getPayload(ois, resultSize-4)  );
			return handle;
		} catch(IOException ioe) {
			System.out.println("client ioe in initialize chunk: " + ioe.getMessage());
			return null;
		}
	}
	
	
	
	/**
	 * Write a chunk at the chunk server from the client side.
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset) {
		if(offset + payload.length > ChunkServer.ChunkSize){
			System.out.println("The chunk write should be within the range of the file, invalide chunk write!");
			return false;
		}
		
		try {
			byte[] handle = ChunkHandle.getBytes();
			//write payload size
			oos.writeInt(4 + 4 + 4 + handle.length + 4 + payload.length);
			//write command identifier
			oos.writeInt(ChunkServer.PutChunk);
			//write chunk handle size
			oos.writeInt(handle.length);
			//write chunk handle
			oos.write(handle);
			//write payload size
			oos.writeInt(payload.length);
			//write payload
			oos.write(payload);
			//write offset
			oos.writeInt(offset);
			oos.flush();
			
			//parse response
			
			//working on putChunk now////////////
			int resultSize = getPayloadInt(ois);
			int result = getPayloadInt(ois);
			if(result == 0) return false;
			else return true;
			
		} catch(IOException ioe) {
			System.out.println("ioe in Client's putChunk: " + ioe.getMessage());
			return false;
		}
		
		
		

		
	}
	
	/**
	 * Read a chunk at the chunk server from the client side.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		if(NumberOfBytes + offset > ChunkServer.ChunkSize){
			System.out.println("The chunk read should be within the range of the file, invalid chunk read!");
			return null;
		}
		
		try {
			byte[] chunkHandleBytes = ChunkHandle.getBytes();
			//write payload size
			oos.writeInt(4 + 4 + 4 + 4 + chunkHandleBytes.length);
			//write command identifier
			oos.writeInt(ChunkServer.GetChunk);
			//write offset
			oos.writeInt(offset);
			//write number of bytes to read
			oos.writeInt(NumberOfBytes);
			//write chunk handle
			oos.write(chunkHandleBytes);
			oos.flush();
			
			//parse response
			return getPayload(ois, NumberOfBytes);
		} catch(IOException ioe) {
			System.out.println("ioe in Client's getChunk: " + ioe.getMessage());
			return null;
		}
		
	}

	

	
	
	
	
	
	
	
	
	
	
	public int getPayloadInt(ObjectInputStream ois) {
		byte[] payload = new byte[4];
		int result = -2;
		try {
			result = ois.read(payload, 0, 4);
		} catch(IOException ioe) {
			System.out.println("ioe in getPayloadInt on client: " + ioe.getMessage());
		}

		if(result == -1) return result;
		else {
			return (ByteBuffer.wrap(payload)).getInt();
		}
	}
	
	
	public byte[] getPayload(ObjectInputStream ois, int payloadSize) {
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
				System.out.println("Client getPayload ioe: " + ioe.getMessage());
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

}
