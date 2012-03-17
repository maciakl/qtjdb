/*
 * Created on Nov 21, 2004
 */

/**
 * Shared Lib for Common Components.
 *
 * This class contains variables and methods which are shared by both the server and the
 * client app.
 *
 * @author Lukasz Grzegorz Maciak
 *
 */
public class QTJDBLib
{
	// ### CONTROL SIGNALS ###
	
	// these signals are shared between the server, the client and any other components
	// which might need to use them. They are byte constants which are to be sent via
	// io streams between the machines.

	// client controll signals
	public static final byte SEND_USERNAME = 1; // not used in 0.1
	public static final byte SEND_PASSWORD = 2; // not used in 0.1
	
	public static final byte GET_NEXT = 3;	// get the next record (if sent as frirs it will request record 0)
	public static final byte GET_ID = 4;	// get record by id (server responds GO_AHEAD and waits for 4 byte int)
	public static final byte GET_META = 5;	// requests metadata from the file (record 0)
	public static final byte GET_LEN = 6;	// requests file length
	
	public static final byte UPDATE = 10;	// triggers consecutive deletion and insertion
	public static final byte DELETE = 20; 	// responds with GO_AHEAD and waits for 4 byte int
	public static final byte INSERT = 30;	// responds with GO_AHEAD and waits for 4 byte int, sends ACK and waits for the record
	
	public static final byte END = 100;	// signals the server that the transaction is over
	public static final byte DIE = 101;	// sends kill signal to the server (server will attempt to shut down upon receiving)
	
	// resposnes from the server
	public static final byte NOT_FOUND = 0;	// in response to GET_NEXT or GET_ID if such record is not found (client will stop querying)
	public static final byte GO_AHEAD = 99;	// auto response to GET_ID - server now waits for 4 byte stream representing integer
	public static final byte ACK = 120; // acknowledge succesful operation to the client - mainly used to respond to UPDATE and DELETE
	public static final byte PK_VIOLATED = 122; // in respons to INSERT if PK of record violates PK constraint on the db
	
	
	/**
	*	Translates an integer into a 4 byte array 
	*	this is dine so that it is possible to send integer
	*	values as bytestreams without wrapping or making them
	*	byte character strings.
	*
	*	@param i - integer to be converted into byte array
	*	@returns byte array representing i
	*/
	public static byte[] int2bytes(int i)
	{
		// store int in 4 bytes
		byte[] l = new byte[4];
		
		// converting int into 4 consecutive byte chunks
		l[0] = (byte)((i >>> 24) & 0xFF);
		l[1] = (byte)((i >>> 16) & 0xFF);
		l[2] = (byte)((i >>> 8) & 0xFF);
		l[3] = (byte)((i) &  0xFF);
		
		return l;
	}
	
	
	/**
	*
	*	Decodes the 4 byte array into an integer.
	*
	*	@param b - 4 byte array representing an integer
	*	@return integer represented by b
	*/
	public static int bytes2int(byte[] b)
	{
		int l = ((b[0] &  0xFF) << 24) | ((b[1] &  0xFF) << 16) | ((b[2] & 255) << 8) | ((b[3]) & 255);
		
		return l;
	}
	
}
