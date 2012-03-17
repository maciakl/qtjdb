/*
 * Created on Nov 21, 2004
 */

/**
 * @author Lukasz Grzegorz Maciak
 *
 */
import java.io.*;
import java.net.*;
import java.util.*;


public class QTJDBServer extends Thread 
{

    protected ServerSocket socket = null;
    protected DatagramPacket packet;
    private int lport;
    private String datafile;
    
    private Properties config;
    
    private QTJDBFile database;
    //private QTJDBIndex index;
    
    private boolean session = false;

    public QTJDBServer() throws IOException 
	{
    	this("QTJDBServer");
    }

    public QTJDBServer(String name) throws IOException 
	{
        super(name);
        
        
        System.out.println("Quick & Tiny Database Server 0.1 (c) 2004 Lukasz Grzegorz Maciak");
        System.out.println("Created at Montclair State University\n");
        System.out.println("Using Flat Data Files for Limited DB functionality");
        System.out.println("Please configure user accounts by editing .qtjdb file");
        System.out.println("Reading .qtjdb... Configuring the Server...");
        
        config = new Properties();
        config.load(new FileInputStream(".qtjdb"));
       
	
        lport = Integer.parseInt(config.getProperty("port"));
	
	System.out.println("\n\tGet port number: \t\t [" + lport + "]");
	
        datafile = config.getProperty("datafile");
        
	System.out.println("\tGet datafile reference: \t [" + datafile + "]\n");
	       
        //System.out.println("Preparing for data file access...");
        //database = new QTJDBFile(datafile);
        //index = database.constructIndex();
        //database.close();
              
        socket = new ServerSocket(lport);
                
        System.out.println("Starting the Database Server...");
	System.out.println("QTJDB Server started on " + new Date());
        System.out.println("Now listening for client connections on port " + lport);
	
        if(config.getProperty("log_into_file").equals("1"))
        {
        	System.out.println("Redirecting output to qtjdb.log");
			PrintStream out = new PrintStream(new FileOutputStream("qtjdb.log", true));
			System.setOut(out);
			System.setErr(out);   	
			System.out.println("QTJDB Server Started on: " + new Date());
        }
	System.out.println("\n\tChecking for misc settings... \t [done]\n");
	
	System.out.println("Now listening for client connections on port " + lport);
	System.out.println("Waiting for connections...");
                
    }

    public synchronized void  run() 
    {
    	while(true)
    	{
	    	try 
		{	        	            
	            new QTJDBTransaction(socket.accept());
	            	            	            	      
		}
	    	catch(SocketException sockete)
		{
	    		if(sockete.getMessage().equals("Connection reset"))
	    				System.out.println("Conection with server ended prematurely!");
		}
	        catch (IOException e) 
		{
	        	e.printStackTrace();
		}
    	}
    }
    
    private void kill() throws IOException
    {
    	System.out.println("\nQTJDB Server is shutting down now...");
    	System.out.println("Sending a temination message to the client");
    	
    	System.out.println("Stopping to listen on port " + lport);
    	socket.close();
    	System.out.println("Shutting down systems services...");
    	System.out.println("Tank you for using QTJDB.");
	System.out.println("\nQTJDB Sever killed on " + new Date());
    	System.exit(0);
    	
    }  
    
    
    public static void main(String[] args) throws IOException 
	{
        new QTJDBServer().start();
    }
    
    
    /**
     * QTJDBTransaction Class
     * 
     * This class deals with actual connection to the client.
     * 
     */
    public class QTJDBTransaction
	{
		private Socket socket;
		private InputStream in;
		private OutputStream out;
		
		private QTJDBFile cdatabase;
		
		private boolean connected;
		
		// set to true to kill server after connection closes
		private boolean kill = false;
		
		public QTJDBTransaction(Socket socket) throws IOException
		{
			System.out.println("Starting new transaction...");
			
			this.socket = socket;
			
			System.out.println("Opening Input Stream Connection...");
			in = socket.getInputStream();
			
			System.out.println("Opening Output Stream Connection...");
			out = socket.getOutputStream();
			
			System.out.println("Checking client cridentials...");
			InetAddress address = socket.getInetAddress();
			int port = socket.getLocalPort();
				
			System.out.println("Establising connection to client at" + address +":" + port);
	
			connected = true;
			
			System.out.println("Creating Client Connection to the datafile.");
			
			cdatabase = new QTJDBFile(datafile);
			
			System.out.println("Waiting for input...");
			
			while(connected)
			{
				byte[] buf = new byte[1];
				
				in.read(buf);
				
				evaluateRequest(buf);
			}
			
			System.out.println("Connection with " + address + ":" + port + " terminating.");
			
			System.out.println("Closing Input Stream...");
			in.close();
			
			System.out.println("Closing Output Stream...");
			out.close();
			
			System.out.println("Closing the Socket...");
			socket.close();
			
			System.out.println("Closing the connection to the datafile...");
			cdatabase.close();
			
			System.out.println("Transaction terminated.");
					
			if(kill)
				kill();
		}
			
		
		private synchronized void evaluateRequest(byte[] buf) throws IOException
	    	{
	    	       
			if(buf[0] == QTJDBLib.GET_NEXT)
				sendRow();
			
			if(buf[0] == QTJDBLib.GET_ID)
				sendRowAt();
				
			if(buf[0] == QTJDBLib.END)
				killTransaction();
			
			if(buf[0] == QTJDBLib.DIE)
				killServer();
				
			if(buf[0] == QTJDBLib.GET_META)
				sendMeta();
				
			if(buf[0] == QTJDBLib.GET_LEN)
				sendLen();
				
			if(buf[0] == QTJDBLib.DELETE)
				removeRowAt();
				
			if(buf[0] == QTJDBLib.UPDATE)
				update();
				
			if(buf[0] == QTJDBLib.INSERT)
				write();
	    	}
		
		private synchronized void removeRowAt() throws IOException
		{
			out.write(new byte[]{QTJDBLib.GO_AHEAD});
			
			byte[] b = new byte[4];
			 
			 in.read(b);
			
			int id = QTJDBLib.bytes2int(b);
			
			cdatabase.deleteRecordAt((int)id);
			
			out.write(new byte[]{QTJDBLib.ACK});
		}
		
		/**
		*
		*	Sends a 4 byte array containing an integer, with the
		*	most significant byte stored at byteray[0].
		*
		*/
		private synchronized void sendLen() throws IOException
		{
			int len = cdatabase.getLength();
			
			// store int in 4 bytes
			byte[] l = QTJDBLib.int2bytes(len);
			
			out.write(l);
			
			System.out.println("Sending File Length to " + socket.getInetAddress() + " [FL]");
			
			//out.write(Integer.toString(len).getBytes());
		}
		
		
		private synchronized void sendMeta() throws IOException
		{
			byte[] buf = cdatabase.readMetadata();
						
			System.out.println("Sending Metadata to " + socket.getInetAddress() + " [META]");
			
			
			if(buf != null)
				out.write(buf);
			else
				sendNotFound();
		}
		
		private synchronized void sendRow() throws IOException
	    	{
			
			byte [] buf = cdatabase.read();
			
			System.out.print("Sending next data record to " + socket.getInetAddress() + " ");
			if(buf != null)
			{
				out.write(buf);
				System.out.println("[.]");
			}
			else
			{
				sendNotFound();
				System.out.println("[X] EOF");
			}
			
			//System.out.println("[DONE]");
			
	    	}
		
		private synchronized void sendRowAt() throws IOException
		{
		
			out.write(new byte[]{QTJDBLib.GO_AHEAD});
			
			byte[] b = new byte[4];
			 
			 in.read(b);
			
			int id = QTJDBLib.bytes2int(b);
						
			//for(int i = 0; i <= buf.length-1; i++) System.out.println(buf[i]);
			
			byte[] result = cdatabase.readRecordAt((int)id);
			
			//out.write(result);
			
			if(result != null)
			{
				System.out.println("Sending data record with id " +id+ " to " + socket.getInetAddress() + " [" + id + "]");
				out.write(result);
			}
			else
			{
				sendNotFound();
				System.out.println("Sending NOT_FOUND signal to " + socket.getInetAddress() + " [X]");
			}
			
			
		}
		
		private synchronized void update() throws IOException
		{
			System.out.println("Preparing tu update a record...");
			
			System.out.println("Deleting the old value in the datafile...");
			
			out.write(new byte[]{QTJDBLib.GO_AHEAD});
			
			byte[] b = new byte[4];
			 
			 in.read(b);
			
			int id = QTJDBLib.bytes2int(b);
			
			System.out.println("id = " + id);
			
			cdatabase.deleteRecordAt((int)id);
			
			out.write(new byte[]{QTJDBLib.ACK});
			
			System.out.println("Writing the new record into the datafile...");
			writeRecord();
		}
		
		
		private synchronized void write() throws IOException
		{
			out.write(new byte[]{QTJDBLib.GO_AHEAD});
			
			writeRecord();
		}
		
		private synchronized void writeRecord() throws IOException
		{
			System.out.println("Preparing to write a record...");
			
			System.out.println("Establishing record length...");
			byte[] buf = new byte[4];
			in.read(buf);
			 
			int length = QTJDBLib.bytes2int(buf);
			
			byte[] record = new byte[length];
			
			System.out.println("Receiving record data...");
			in.read(record);
			
			try
			{
				cdatabase.insertRecord(record);
				out.write(new byte[]{QTJDBLib.ACK});
			}
			catch(NumberFormatException nfex)
			{
				System.out.println("Primary key constraint violated... Aborting...");
				out.write(new byte[]{QTJDBLib.PK_VIOLATED});
				
			}
			
			
		}
		
		private synchronized void sendNotFound() throws IOException
		{
			out.write(new byte[]{QTJDBLib.NOT_FOUND});
		}
		
		private synchronized void killTransaction() throws IOException
		{
			out.write(("Connection terminated").getBytes());
			connected = false;
		}
		
		private synchronized void killServer() throws IOException
		{
			byte [] buf = ("The QTJDB Server is shuting down - connection terminated").getBytes();
			out.write(buf);
			
			connected = false;
			kill = true;
		}

	}
    
}