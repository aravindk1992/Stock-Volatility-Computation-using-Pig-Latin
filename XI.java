package myUDFS;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

import java.util.Collections;
import java.util.HashMap;
import java.util.ArrayList;

 
 public class XI extends EvalFunc<String>{
 HashMap<String,ArrayList<String>> map = new HashMap<String,ArrayList<String>>();
 ArrayList<String> list = new ArrayList<String>();
 String FileName;
 String Year;
 String Month;
 String Balance;
 String Day;
 	
     public String exec(Tuple input) throws IOException {
         if (input == null || input.size() == 0)
             return null;
         try{
        	 	DataBag Sample = (DataBag)input.get(0);
        	 	int max=0; 
        	 	int min=32;
        	 	double closing_max= 0.0;
        	 	double closing_min = 0.0;
        	 	for(Tuple t : Sample){
        	 		FileName= String.valueOf(t.get(0));
        	 		Year = String.valueOf(t.get(1));
        	 		Month = String.valueOf(t.get(2));
        	 		Day = String.valueOf(t.get(3));
        	 		Balance = String.valueOf(t.get(4));
        	 		
        	 		if(max<Integer.parseInt(Day)){
        	 			max= Integer.parseInt(Day);
        	 			closing_max= Double.parseDouble(Balance);
        	 		}
        	 		if(min>Integer.parseInt(Day)){
        	 			min = Integer.parseInt(Day);
        	 			closing_min = Double.parseDouble(Balance);
        	 		}
        	 		   	 		
        	 	 }
        	   double Value = (closing_max - closing_min)/closing_min;
               
               return FileName.split("\\.")[0]+"\t"+String.valueOf(Value); 
        	 	
         }catch(Exception e){
             throw WrappedIOException.wrap("Caught exception processing input row ", e);
         }
     }
 }

