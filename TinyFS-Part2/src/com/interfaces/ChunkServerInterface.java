package com.interfaces;

/**
 * Interfaces of the CSCI 485 TinyFS ChunkServer
 * @author Shahram Ghandeharizaden
 *
 */
public interface ChunkServerInterface {

	public static final int ChunkSize = 4 * 1024; //4 KB chunk sizes
	
	public static final int InitializeChunk = 1;
	public static final int PutChunk = 2;
	public static final int GetChunk = 3;
	
	/**
	 * Return the chunkhandle for a newly created chunk.
	 */
	public String initializeChunk();
	
	/**
	 * Write the byte array payload to the ChunkHandle at the specified offset.
	 */
	public boolean putChunk(String ChunkHandle, byte[] payload, int offset);
	
	/**
	 * Read the specified NumberOfBytes from the target chunk starting at the specified offset.
	 * Return the retrieved number of bytes as a byte array.
	 */
	public byte[] getChunk(String ChunkHandle, int offset, int NumberOfBytes);	

}
