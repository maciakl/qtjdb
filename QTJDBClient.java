/*
 * Created on Nov 21, 2004
 */


import java.io.*;
import java.net.*;
import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.awt.event.WindowListener;

import javax.swing.*;
import javax.swing.event.*;
import javax.swing.table.*;

/**
 *
 * This is the client class which connects to the server. It provides a GUI interface for
 * the database operations.
 *
 * @author Lukasz Grzegorz Maciak
 *
 */
public class QTJDBClient extends JFrame
{
	private Socket socket;
	
	private InputStream in;
	private OutputStream out;
	
	private JButton showAll, delete, close, insert;
	private JScrollPane pane;
	private JTable table;
	private JLabel errorDisplay;
	
	private JTextField find;
	
	private boolean UPDATES = true;
	
	//private QTJDBClientTransaction tr;

	
	public QTJDBClient(InetAddress address, int port) throws IOException
	{
		super("QTJDB Client");
		
		WindowListener wl = new WindowAdapter() 
		{
			public void windowClosing(WindowEvent e) {System.exit(0);}
		};
		
		this.addWindowListener(wl);
		
		ButtonListener bl = new ButtonListener();
		
		JPanel buttons = new JPanel();
		
		showAll = new JButton("Show All");
		delete = new JButton("Delete");
		close = new JButton("Kill Server");
		insert = new JButton("Insert");
		
		find = new JTextField(3);
		
		showAll.addActionListener(bl);
		find.addActionListener(bl);
		delete.addActionListener(bl);
		insert.addActionListener(bl);
		close.addActionListener(bl);
		
		buttons.add(showAll);
		buttons.add(insert);
		buttons.add(delete);
		buttons.add(new JLabel("Find Record At:"));
		buttons.add(find);
		buttons.add(close);
		
		table = new JTable(1,1);
		pane = new JScrollPane();
		pane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		pane.setViewportView(table);
		
		table.getModel().addTableModelListener(new updateListener());
		
		getContentPane().setLayout(new BoxLayout(getContentPane() ,BoxLayout.Y_AXIS));
		getContentPane().add(buttons);
		getContentPane().add(pane);
		
		pack();
		
		
		setVisible(true);

	}
	
	/**
	* 
	* Setup a table - after each operation we need to re-setup the table and this
	* method reaplies all the properties and redisplays it.
	*
	*/
	private void setupTable(Object[][] a, Object[] b)
	{
		table = new JTable(new DefaultTableModel(a, b));
		pane.setViewportView(table);		
		table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
		table.getModel().addTableModelListener(new updateListener());
	}
	
	private void connectionError()
	{
		JOptionPane.showMessageDialog(this, "Connection Refused - check if the DB Server is running.", "Connection Error",JOptionPane.ERROR_MESSAGE);
	}
	
	private void ioError(String msg)
	{
		JOptionPane.showMessageDialog(this, "I/O Error: " + msg, "I/O Error",JOptionPane.ERROR_MESSAGE);
	}
	
	private void invalidParamError(String msg)
	{
		JOptionPane.showMessageDialog(this, "Invalid Parameter - please input different number. \n" + msg, "Invalid Parameter",JOptionPane.ERROR_MESSAGE);
	}
	
	private void showInsertMessage()
	{
		JOptionPane.showMessageDialog(this, "Please scroll down to see the new row.\nAfter inputing new data, hit Insert again.", "Inserting a New Row",JOptionPane.INFORMATION_MESSAGE);
	}
	
	private void showMessage(String message)
	{
		JOptionPane.showMessageDialog(this, message, "QTJDB Message",JOptionPane.INFORMATION_MESSAGE);
	}
	
	
	public static void main(String[] args) throws IOException
	{

		if (args.length <= 1) 
		{
			System.out.println("Usage: java QTJDBClient <hostname> <port>");
			return;
		}
	
		
			QTJDBClient a = new QTJDBClient(InetAddress.getByName(args[0]), Integer.parseInt(args[1]));
		

		
	 }
	
	/**
	*
	* The tranaction class which handles all the communication with the server
	* 
	*/
	public class QTJDBClientTransaction 
	{
	
		public QTJDBClientTransaction() throws IOException
		{
			socket = new Socket("localhost", 1337);
        
        		out = socket.getOutputStream();
			in = socket.getInputStream();
		}
		
		/**
		* 	Sends a request and waits for reply
		* 
		* 	@param buf - request buffer
		*/
		private byte[] sendRequest(byte[] buf) throws IOException
		{
			out.write(buf);
			
			byte[] buf2 = new byte[100];
		
			in.read(buf2);
		
			//System.out.println(new String(buf2));
		
			return buf2;	
		
		}
		
		/**
		*	 Sends signal to the server without waiting for
		* 	reply.
		* 
		* 	@param buf message to ot be sent to the server
		*/
		private void send(byte[] buf) throws IOException
		{
			out.write(buf);
		}
		
		/**
		*	Sends the rermination signal to the server
		*	and closes the sockets to free up the connection.
		*/
		private void close() throws IOException
		{
			send(new byte[]{QTJDBLib.END});
			in.close();
			out.close();
			socket.close();
			
		}
		
		private void sendKillServer() throws IOException
		{
			byte[] buf = sendRequest(new byte[]{QTJDBLib.DIE});
			
			//this is ugly but I dont feel like going through the
			// byte array - I know the message fom the server is
			// that size
			showMessage(new String(buf).substring(0, 56));
			
		}
		
		/**
		* 	Gets all of the rows from the database by repetedly sending the
		* 	GET_NEXT signal to the Server. Stops after recieving NOT_FOUND
		* 	response.
		*
		*	@returns Object array containing all the rows and columns returned
		*			by the server. 
		*/
		private Object[][] getAllRows() throws IOException
		{
		
			int records = getNumberOfRecords();
			
			Object[] meta = getMetadata();
		
			Object[][] result = new Object[records][meta.length]; 
			
			
			byte buf[] = new byte[]{1};
			
			
			
			int counter = 0;
			
			while(buf[0]!=QTJDBLib.NOT_FOUND)
			{
				
				buf = sendRequest(new byte[]{QTJDBLib.GET_NEXT});
				
				if(buf[0]==QTJDBLib.NOT_FOUND)
					break;
				
				
				//System.out.println(new String(buf));
				
				StringTokenizer record = new StringTokenizer(new String(buf), "\t\n", true);
				
				int i = 0;
				
				while(record.hasMoreTokens())
				{
					String temp = record.nextToken();
					
					if(temp.equals("\n"))
						break;
					
					if(!temp.equals("\t"))
					{
						result[counter][i] = temp;
						i++;
					}
				}
				
				counter++;
			}
			
			return result;
		}
		
		
		
		private Object[][] getRecordAt(int id) throws IOException
		{
			Object[] meta = getMetadata();
			
			Object[][] result = new Object[1][meta.length];
			
			byte[] buf = sendRequest(new byte[]{QTJDBLib.GET_ID});
			
			if(buf[0]== QTJDBLib.GO_AHEAD)
			{
				buf = sendRequest(QTJDBLib.int2bytes(id));
				
				if(buf[0]==QTJDBLib.NOT_FOUND)
				{
					invalidParamError("ID not found in Database Index.");
				}
				else
				{
			
					StringTokenizer record = new StringTokenizer(new String(buf), "\t\n", true);
					int i = 0;
					
					while(record.hasMoreTokens())
					{
						String temp = record.nextToken();
					
						if(temp.equals("\n"))
							break;
									
						if(!temp.equals("\t"))
						{
							result[0][i] = temp;
							i++;
						}
					}
				}
			}
				
			
			return result;
			
		}
		
		private void deleteRecordAt(int id) throws IOException
		{
			byte[] buf = sendRequest(new byte[]{QTJDBLib.DELETE});
			
			if(buf[0]== QTJDBLib.GO_AHEAD)
			{
				buf = sendRequest(QTJDBLib.int2bytes(id));
				
				if(buf[0]== QTJDBLib.ACK)
					return;
				else
					throw new IOException("Unable to delete row.");
			}
		
		}
		
		
		/**
		*	Gets the column names for all the rows in a table
		*
		*	@returns Object array with names of all the columns in the db
		*/
		private Object[] getMetadata() throws IOException
		{
			Object[] columns = new Object[10];
		
			byte[] buf = sendRequest(new byte[]{QTJDBLib.GET_META});
			
			StringTokenizer cols = new StringTokenizer(new String(buf), "\t\n", true);
			
			int counter = 0;
			
			while(cols.hasMoreTokens())
			{
				if(counter >= columns.length)
				{
					Object[] columns2 = new Object[2*columns.length];
					System.arraycopy(columns, 0, columns2, 0, columns.length);
					columns = columns2;
					columns2 = null;				
				}
				
				String t = cols.nextToken();
				
				if(t.equals("\n"))
					break;
					
				if(!t.equals("\t"))
				{
					columns[counter] = t;
					counter++;
				}
			}
			
			// trim the array if there ar blank cells
			
			
			if(columns.length >= counter)
			{
				Object[] c = new Object[counter];
				System.arraycopy(columns, 0, c, 0, counter);
				columns = c;
				c = null;
			}
			
			
			return columns;
			
		}
		
		private int getNumberOfRecords() throws IOException
		{
			byte[] b = sendRequest(new byte[]{QTJDBLib.GET_LEN});
			
			// reconstruct the int from a byte array (int stored as 4 bytes)
			int len = QTJDBLib.bytes2int(b);
			
			//System.out.println(len);
			
			return len;
		}
		
		private void sendRecordUpdate(int id, byte[] record) throws IOException
		{
			// request a buffer with id
			byte[] buf = sendRequest(new byte[]{QTJDBLib.UPDATE});
			
			if(buf[0]== QTJDBLib.GO_AHEAD)
			{
				buf = sendRequest(QTJDBLib.int2bytes(id));
				
				if(buf[0]!=QTJDBLib.ACK)
				{
					invalidParamError("ID not found in Database Index.");
				}
				else
				{	
					send(QTJDBLib.int2bytes(record.length));
					buf = sendRequest(record);
					
					if(buf[0]== QTJDBLib.ACK)
						return;
					else
						throw new IOException("Unable to update row.");
				}
			}
			else
				throw new IOException("Unable to update row: \n Unexpected Response from the Server.");
			
		
		}
		
		private void sendRecordInsertion(byte[] record) throws IOException
		{
			byte[] buf = sendRequest(new byte[]{QTJDBLib.INSERT});
			
			if(buf[0]== QTJDBLib.GO_AHEAD)
			{
				send(QTJDBLib.int2bytes(record.length));	
				buf = sendRequest(record);
				
				if(buf[0]== QTJDBLib.ACK)
					return;
				else
					throw new IOException("Primary Key Constraint Violated. Please try another key.");
			}
			else
				throw new IOException("Unable to update row: \n Unexpected Response from the Server.");
			
		}
		
		
		
	}
	
	private class ButtonListener implements ActionListener
	{
		public void actionPerformed(ActionEvent e)
		{
			try
			{
				
				if(e.getSource() == showAll)
				{
					QTJDBClientTransaction tr = new QTJDBClientTransaction();	
				
					setupTable(tr.getAllRows(), tr.getMetadata());
					
					// terminate the transoaction
					tr.close();
				}
				
				if(e.getSource() == find)
				{
					QTJDBClientTransaction tr = new QTJDBClientTransaction();
				
					if(find.getText().equals("") || find.getText() == null)
						throw new NumberFormatException("Empty String is not a Valid Number.");
					try
					{
						setupTable( tr.getRecordAt(Integer.parseInt(find.getText())),tr.getMetadata());
					}
					catch(Exception exc)
					{
						invalidParamError(exc.getMessage());
					}
					finally
					{
						tr.close();
					}
				}
				
				if(e.getSource() == delete)
				{
					QTJDBClientTransaction tr = new QTJDBClientTransaction();
				
					int row = table.getSelectedRow();
					
					if(row == -1)
						throw new NumberFormatException("Please select row to be deleted.");
					
					int id = Integer.parseInt((String)table.getValueAt(row, 0));
					
					tr.deleteRecordAt(id);
					
					setupTable(tr.getAllRows(), tr.getMetadata());
					
					tr.close();
					
				}
				
				if(e.getSource() == close)
				{
					QTJDBClientTransaction tr = new QTJDBClientTransaction();
					
					tr.sendKillServer();
					tr.close();
					
				}
				
				if(e.getSource() == insert)
				{
					if(UPDATES)
					{	
						UPDATES = false;
					
						// get the table model
						//DefaultTableModel tm = (DefaultTableModel) table.getModel();
					
						// create array of objects with appropriate number of slots
						Object [] obj = new Object[table.getModel().getColumnCount()];
					
						// fill the array with empty strings
						// this is better than empty objects which
						// will produce funny hash values
						for(int i=0; i<=table.getModel().getColumnCount()-1; i++)
						{
							obj[i] = new String("");
						}
					
						// add the row to the table and redraw
						((DefaultTableModel)table.getModel()).addRow(obj);
						pane.setViewportView(table);
						
						delete.setEnabled(false);
						showAll.setEnabled(false);
						find.setEnabled(false);
						insert.setForeground(Color.red);
						
						showInsertMessage();
						
						//table.getSelectionModel().setSelectionInterval(table.getRowCount(), 0);
						table.scrollRectToVisible(table.getCellRect(table.getRowCount(), 0, false));
						
					}
					else
					{
						UPDATES = true;
						
						QTJDBClientTransaction tr = new QTJDBClientTransaction();
						
						int row = table.getRowCount()-1;
			
						TableModel model = (TableModel)table.getModel();
						
						int cols = model.getColumnCount();
						
						// hold record as a string
						String record = "";
						
						for(int i =0; i<= cols-1; i++)
						{
							record += (String) model.getValueAt(row, i);
							
							// add delimiters
							if(i == cols-1)
								record += "\n";
							else
								record += "\t";
						}
						
						byte[] buf = record.getBytes();
						
						System.out.println(record);
						try
						{
							tr.sendRecordInsertion(buf);
							
							delete.setEnabled(true);
							showAll.setEnabled(true);
							find.setEnabled(true);
							insert.setForeground(Color.black);
						}
						catch(IOException eee)
						{
							ioError(eee.getMessage());
							setupTable(tr.getAllRows(), tr.getMetadata());
							delete.setEnabled(true);
							showAll.setEnabled(true);
							find.setEnabled(true);
							insert.setForeground(Color.black);
						}
						
						tr.close();
					}
				}
			}
			catch(NumberFormatException nfe)
			{
				invalidParamError(nfe.getMessage());
			}
			catch(ConnectException ce)
			{
				connectionError();
			}
			catch(SocketException se)
			{
				connectionError();
			}
			catch(IOException eee)
			{
				ioError(eee.getMessage());
			}
			
			
		}
	}
	
	private class updateListener implements TableModelListener
	{
		public void tableChanged(TableModelEvent tbme)
		{
			if(!UPDATES)
				return;
				
			int row = tbme.getFirstRow();
			
			TableModel model = (TableModel)tbme.getSource();
			
			int cols = model.getColumnCount();
			
			int id = Integer.parseInt(((String)model.getValueAt(row, 0)));
			
			// hold record as a string
			String record = "";
			
			for(int i =0; i<= cols-1; i++)
			{
				record += (String) model.getValueAt(row, i);
				
				// add delimiters
				if(i == cols-1)
					record += "\n";
				else
					record += "\t";
			}
			
			byte[] buf = record.getBytes();
			
			System.out.println(record);
			
			try
			{
				QTJDBClientTransaction tr = new QTJDBClientTransaction();
			
				tr.sendRecordUpdate(id, buf);
				
				setupTable(tr.getAllRows(), tr.getMetadata());
				
				tr.close();
			}
			catch(NumberFormatException nfe)
			{
				invalidParamError(nfe.getMessage());	
			}
			catch(ConnectException ce)
			{
				connectionError();
			}
			catch(SocketException se)
			{
				connectionError();
			}
			catch(IOException eee)
			{
				ioError(eee.getMessage());
			}
			
            	}
	}
   
}