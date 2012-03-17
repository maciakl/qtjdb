/*
 * Created on Nov 22, 2004
 */

 import java.io.*;
 
/**
 * @author Lukasz Grzegorz Maciak
 *
 */
public class QTJDBIndex
{

	private String name;
	
	// the index
	long[][] index; // => [i][0] = ID
			// => [i][1] = ADDRESS
			
	RandomAccessFile f;
	
	
	public QTJDBIndex(long[][] index, String name)
	{
		this.name = name;
		this.index = index;
	}
	
	
	public QTJDBIndex(String name) throws IOException
	{
		this.name = name;
		readIndex(name);
	}
	
	
	
	private void expandByOne()
	{
		long[][] index2 = new long[index.length+1][2];
				
		for(int z=0; z <= index.length-1; z++)
		{
			index2[z][0] = index[z][0];
			index2[z][1] = index[z][1];
		}
		
		index = index2;
		index2 = null;
		
	}
	
	public void add(long first, long second)
	{
		expandByOne();
		
		index[index.length-1][0] = first;
		index[index.length-1][1] = second;
	}
	
	
	public void set(int i, long id, long address)
	{
		index[i][0] = id;
		index[i][1] = address;
	}
	
	public long find(long id)
	{
		long address = 0;
		boolean success = false;
		
		int i = indexOf(id);
		
			if(i != -1)
			{
				address = index[i][1];
				success = true;
				
				return address;
			}
			else
				return -1;		
	}
	
	/**
	*	Finds the index[i][1] which is greater or equal to value
	*	and returns index[i][0]
	*
	*	@param		value long value to be compared with index[i][1]
	*	@returns	index[i][0] or -1 if such a value is not found
	*/
	public long findGreater(long value)
	{
		for(int x=0; x <= index.length-1; x++)
		{
			if(index[x][1] == value)
				return index[x][0];
		}
		
		return (long) -1;
	}
	
	
	public void remove(int id)
	{
		int dex = indexOf(id);
		
		System.out.println("Updating the Index...");
		
		for(int i = dex+1; i <=index.length-1; i++)
		{
			index[i-1][0] = index[i][0];
			index[i-1][1] = index[i][1];
		}
		
		trimIndex(index.length-1);
	}
	
	public void disp()
	{
		for(int j=0; j<= index.length-1; j++)
			System.out.print(index[j][0] + ":" + index[j][1] + " ");
		
		System.out.println();
		
	}
	
	public int len()
	{
		return index.length;
	}
	
	public void readIndex(String name) throws IOException
	{
	
		index = new long[100][2];
		
		f = new RandomAccessFile(name, "rw");
		
		if(f.readByte() == ((byte)0))
			throw new IOException("File " + name + " is not Valid - recreate Index");
		
		int i = 0;
		
		
		while(true)
		{
		
			// expand array if it is to small
			if(i >= index.length)
			{
				long[][] index2 = new long[index.length*2][2];
				
				
				
				for(int z=0; z <= index.length-1; z++)
				{
					index2[z][0] = index[z][0];
					index2[z][1] = index[z][1];
				}
				
				index = index2;
			}
			
			try
			{
				index[i][0] = f.readLong();
				index[i][1] = f.readLong();
			}
			catch(EOFException eof)
			{
				break;
			}
			
			i++;
		}
		
		
		// trim the index if it is to long
		if(i<index.length)
			trimIndex(i);
		
		// invalidate the index file. Since index is now in memory
		// the file cannot be valid untill it is written to again
		f.write((byte)0);
		
		f.close();
		
	}
	
	/**
	*	Trim the index size down to i
	*
	*/
	private void trimIndex(int i)
	{
		//System.out.println("Optimizing index size in memory...");
		
		long[][] index3 = new long[i][2];
					
		for(int v=0; v <= i-1; v++)
		{
			index3[v][0] = index[v][0];
			index3[v][1] = index[v][1];
		}
		
		index = index3;
		
		// garbage collection
		index3 = null;
	}
	
	
	
	public void writeIndex()
	{
		try
		{
			new File(name).delete();
			
			f = new RandomAccessFile(name, "rw");
		
			// skip the validation byte
			f.writeByte((byte)0);
		
			System.out.print("Writing " + name + "...");
			
			for(int i=0; i<= index.length-1; i++)
			{
				System.out.print(".");
				f.writeLong(index[i][0]);
				f.writeLong(index[i][1]);
			}
			
			f.seek(0);
			
			// write the validation byte - this means that 
			//file is updated and current
			f.write((byte)1);
			
			f.close();
			
			System.out.println("[ok]");
		}
		catch(IOException wrtex)
		{
			System.out.println("Could not write index completely... Aborting anyway.");
		}
		
	}
	
	
	public int indexOf(long id)
	{
		for(int i=0; i<= index.length-1; i++)
		{
			if(index[i][0] == (long) id)
				return i;
		}
		
		return -1;
	}


}
