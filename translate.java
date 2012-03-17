public class translate
{
	public static void main(String[] args)
	{
		//byte[] p = new String("~#~").getBytes();
		
		//for(int i=0; i<=p.length-1; i++)
			//System.out.println(p[i]);
		/*	
		byte[] a = new byte[10];
		
		a[0] = (byte)(126);
		a[1] = (byte)(35);
		a[2] = (byte)(126);
		
		if(new String(a).equals("~#~"))
			System.out.println("IRULEYOU!");
		*/	
		
		int i=0;
		String a = "#";
		
		while(true)
		{
			if(i>=10)
				a = "-";
			
			System.out.print(a+"\b");
		}
	}
} 
