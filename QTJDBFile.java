import java.io.*;
import java.util.*;

/*
 * Created on Nov 21, 2004
 */

/**
 * @author Lukasz Grzegorz Maciak
 *
 */
public class QTJDBFile extends File 
{

	// index file names
	public final String PRIMARY_INDEX = ".index";	
	public final String DELETED_INDEX = ".deleted";
	
	RandomAccessFile db;
	QTJDBIndex index;
	QTJDBIndex deleted;	// holds information about deleted entries
				// deleted[i][0] - address of deleted record
				// deleted[i][1] - size of deleted record
	
	public QTJDBFile(String name) throws IOException
	{
		super(name);
		
		System.out.println("Initializing Random Access File Stream to the data file");
		
		System.out.println("Opening I/O stream to: " + name);
		db = new RandomAccessFile(this, "rw");
		
		try
		{
			System.out.print("\n\tAttempting to read the index file...");
			index = new QTJDBIndex(PRIMARY_INDEX);
			deleted = new QTJDBIndex(DELETED_INDEX);
			System.out.println(" \t\t[done]");
		}
		catch(IOException ioex)
		{
			System.out.println(" \t\t[failed]");
			constructIndex();
		}
		
		System.out.println("\nSucesfully opened and configured datafile.");
						
	}
	
	private long[][] expandArray(long [][] index)
	{
		long[][] index2 = new long[index.length*2][2];
				
		for(int z=0; z <= index.length-1; z++)
		{
			index2[z][0] = index[z][0];
			index2[z][1] = index[z][1];
		}
		
		index = index2;
		index2 = null;
		
		return index;
	}
	
	private long[][] optimizeArray(long[][] index2, int i)
	{
	
		System.out.print("\tOprimizing index length in memory...");
			
		long[][] index3 = new long[i][2];
			
		for(int v=0; v <= i-1; v++)
		{
			index3[v][0] = index2[v][0];
			index3[v][1] = index2[v][1];
		}
			
		index2 = index3;
		index3 = null;
			
		System.out.println(" \t\t[done]");
		
		return index2;
	
	}
	
	public void constructIndex() throws IOException
	{
		System.out.print("\n\tConstructing index on the primary key...");
		
		long [][] index  = new long[100][2];
		long [][] deleted = new long[50][2];
		
		db.seek((long)0);
		
		// grab metadata and trhow it away
		readRecord();
		
		byte[] record = new byte[1];
		long pointer;
		
		int i = 0;
		int d = 0;
		
		while(record != null)
		{
		
			// expand array if it is to small
			if(i>= index.length)
				index = expandArray(index);
				
			if(d>= deleted.length)
				deleted = expandArray(deleted);
			
			
			pointer = db.getFilePointer();
			record = readRecord();
			
			if(record == null)
				break;
			
			
			// deleted rows are indicated by a 0 in the first byte so we skip them
			if(record[0] != ((byte)0))
			{
				index[i][0] = Long.parseLong( (new String(record)).substring(0, 2));
				index[i][1] = pointer;
			
				i++;
			}
			
			if(record[0] == ((byte)0))
			{
				// save starting position
				index[d][0] = pointer;
				index[d][1] = db.getFilePointer() - pointer; // save the size
				
				d++;
			}
			
		}
		
		System.out.println(" \t[done]");
		
		if(d <= 0)
		{
			d =1;
			
			// because there might be no deleted
			// records we initialize deleted with
			// some null values
			
			deleted[0][0] = (long)0;
			deleted[0][1] = (long)0;
		}
		
		// trim the index if it is to long
		if(i<index.length)
			index = optimizeArray(index, i);
			
		if(d<deleted.length)
			deleted = optimizeArray(deleted, d);
		
		db.seek((long)0);
		
		this.index = new QTJDBIndex(index, PRIMARY_INDEX);
		this.deleted = new QTJDBIndex(deleted, DELETED_INDEX);
	}
	
	public int getLength()
	{
		return index.len();
	}
	
	public byte[] readMetadata() throws IOException
	{
		db.seek((long) 0);
		byte [] metadata = readRecord();
		
		return metadata;
	}
	
	public byte[] read() throws IOException
	{
		byte[] buf = readRecord();
		
		if(buf == null)
			return null;
		
		if(buf[0] == ((byte)0))
			buf = read();
		
		/*
		while(buf[0] == ((byte)126))
		{
			System.out.println("buf=" + buf[0]);
		
			buf = readRecord();
		}
		*/
		
		return buf;
		
		
	}
	
	public byte[] readRecord() throws IOException
	{
		// records are delimited by newline characters
		byte current = 0;
		byte[] record = new byte[100];
		
		int i = 0;
						
		while(current != (byte)10)
		{			
			// if the array is to small, make it twice as big
			if(i >= record.length)
			{
				byte[] record2 = new byte[record.length*2];
				System.arraycopy(record, 0, record2, 0, record.length);
				record = record2;
			}
			
			try
			{
				current = db.readByte();
				record[i] = current;
				//System.out.println(current);
			}
			catch(EOFException e)
			{
				return null;
			}
			
			i++;
		}
		
		return record;
	}
	
	public byte[] readRecordAt(int id) throws IOException
	{
		
			long address = index.find(id);
			
			if(address != -1)
			{
				db.seek(address);
				return readRecord();
			}
			else	
				return null;
	}
	
	public void deleteRecordAt(int id) throws IOException
	{
		long address = index.find(id);
		
		if(address != -1)
		{
			System.out.println("Marking record for deleteion...");
			
			db.seek(address);
			db.write(0);
			
			index.remove(id);
			
			db.seek(address);
			
			readRecord();
			
			long end = db.getFilePointer();
			
			deleted.add(address, end-address);
		}
	}
	
	public void writeRecord(byte[] buf) throws IOException
	{
			db.write(buf);
	}
	
	
	public void insertRecord(byte[] buf) throws IOException
	{
		long address = deleted.findGreater((long)buf.length);
		
		StringTokenizer a = new StringTokenizer(new String(buf), "\t");
		
		long id = Integer.parseInt(a.nextToken());
				
		if(address != (long) -1)
			db.seek(address);
		else
			db.seek(db.length());
			
		if(index.find(id) != -1)
			throw new NumberFormatException("Specified index already exists!");
		
		index.add(id, db.getFilePointer());
			
		writeRecord(buf);
	}
	
	public void close() throws IOException
	{
		System.out.println("Writing out index file...");
		index.writeIndex();
		deleted.writeIndex();
		
		System.out.println("Connection to the data file closed.");
		db.close();
	}
	
	public static void main (String[] args)
	{
		try
		{
			QTJDBFile a = new QTJDBFile("qtjdb.dat");
			
			byte[] o = a.readRecord();
			byte[] z = a.readRecordAt(7);
			
			if(o != null)
			{
				System.out.print(new String(o));
				System.out.print(new String(z));
			}
			
			
			
			
		}catch(Exception e){e.printStackTrace();}
		
	}

}
