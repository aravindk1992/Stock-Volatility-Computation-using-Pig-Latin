package volatilityCompute;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;


public class VOLATILITY extends EvalFunc<String>{
	
	String FileName;
	
	
	public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
        	int count=0;
        	double xis= 0.0;
        	double timepass=0.0;
	 	 	double Volatility=0.0;
	 	 	double xbar=0.0;
	 	 	ArrayList<Double> list = new ArrayList<Double>();
    	 	DataBag Sample = (DataBag)input.get(0);
    	 	 for(Tuple t : Sample){
    	 		 FileName = String.valueOf(t.get(0));
    	 		 String Xi = String.valueOf(t.get(1));
    	 		 list.add(Double.parseDouble(Xi));
    	 		 //count++;
    	 	 }
    	 	 	
	    	 	for(int i=0;i<list.size();i++){
					xis+= list.get(i); //sum(xi-xbar)
				}
	    	 	count= list.size();
	    	 	xbar= xis/count;
    	 	 	for(int i=0;i<count;i++){
					timepass+= Math.pow(list.get(i)-xbar,2); //sum(xi-xbar)
				}
    	 	 	
    	 	 	Volatility = Math.pow((timepass/(count-1)),0.5);
    	 	 	
    	 	 	if(Volatility>0.0){
    	 	 		return FileName+"\t"+ String.valueOf(Volatility);
    	 	 	}
    	 	 	else{
    	 	 		return null;
    	 	 	}
    	 	 	        }
        catch(Exception e){
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
	
	}

}
