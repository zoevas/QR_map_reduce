/**
 * Cloud computing Direct TSQR decomposition
 * Decomposition.java
 * Purpose: Performs  QR Decomposition for tall and skinny matrices. 
 * The QR decomposition of A is done in three steps using two map 
 * functions and one reduce function. The algorithm is in paper 
 * "Direct QR factorizations for tall-and-skinny matrices in
 * MapReduce architectures"
 * @author Vasileiou Zoi, Kiamilis Nikolaos
 * @version 1.0 1/22/14
 */

package org.myorg;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapred.lib.NLineInputFormat;

import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.OutputStreamWriter;
import java.io.InputStreamReader;
import java.io.IOException;

//Library for print messages inside mappers and reducers
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//Library which performs th QR decomposition is the Jama library http://math.nist.gov/javanumerics/jama/
import Jama.Matrix;
import Jama.QRDecomposition;


public class Decomposition {

	private static final Log LOG = LogFactory.getLog(Decomposition.class.getName());
	public static final int N = 2;	
	public static final int ROWS = 8 * N;
	public static final int LOCAL_ROWS = ROWS / 4;
	
	

	public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
	/**
	  * This method corresponds to the first step of the algorithm TSQR.The input for each map task 
	  * is a submatrix A with LOCAL_ROWS rows and N columns. Each map task collects data as a local matrix A, 
          * computes a single QR decomposition. Each map tasks emits its corresponding  Qi to the file 
	  * path/hadoop-0.8.13/my_input2 with a file name Qi where i is the index at which the submatrix Qi starts. 
	  * Finally, this folder will have all the Qi submatrices to seperate files Qi and the Q matrix which is 
	  * output from the reduce task. Also each map task emits its R matrix to one single reduce task.
	*/
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
		double[][] A = new double[LOCAL_ROWS][N];
		double[][] Q = new double[LOCAL_ROWS][N];
		double[][] R = new double[N][N];

		String line = value.toString();
	        String[] values = line.split(",");

		//values[0] contains the row index at which each submatrix starts. The values have elements of the matrix from index 1 and after.  
		int valueIndx = 1;
		for(int i = 0; i < LOCAL_ROWS; i++){
			for(int j = 0; j < N; j++) {
				A[i][j] = Double.parseDouble(values[valueIndx]);
                                valueIndx++;
			}
		}

		//QR Decomposition
		//Q array	
		Matrix A_M = new Matrix(A);
		QRDecomposition QR = new QRDecomposition(A_M);
		Matrix Q_M = QR.getQ();
		Q = Q_M.getArray();

		String Q_S = new String();

		//Write the Q submatrix in a string.		
		for (int i=0; i < LOCAL_ROWS; i++){
			for (int j = 0;j < N;j++) {
				if(i == LOCAL_ROWS -1 && j == N -1 )
					Q_S += Q[i][j]; 
				else
					Q_S += Q[i][j] + ","; 
			} 
		}


		//Writes Q_S to a file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path("/home/zoe/hadoop-0.18.3/my_input2/Q" + values[0]);  
		
		BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(path)));
		br.write(Q_S);
		br.close();

		//R array		
		String RValues = new String((Integer.parseInt(values[0])/2) +",");
		Matrix R_M = QR.getR();
		R = R_M.getArray();
		
		for (int i=0; i < N; i++){
			for (int j = 0;j < N;j++) {
				if(i == N -1 && j == N -1 )
					RValues+= R[i][j];
				else
					RValues+= R[i][j] + ",";
			} 
		}
		
		//each map task emits the R submatrix to one single reduce task
		Text outputKey =  new Text();
		outputKey.set("1");
		Text outputValue = new Text(RValues);
		output.collect(outputKey, outputValue);        
		
		}
	}







	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
	/**
 	  * This map method inputs the file Q_input_for_mapper2. Each line of this file contains two submatrices. One
	  * Qi matrix from the first step, and Qi matrix from the second step. One line is assigned to each map task.
	  * So each map task performs a multiplication of these two matrices and writes the result to a file.
	  * All the Qi matrices produced compose the final Q. 
   	  *
        */
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

		LOG.info("mapper2\n");
		
		String line = value.toString();        	
		String[] values = line.split(",");

		
		double[][] QFirstStep = new double[LOCAL_ROWS][N];
		double[][] QSecondStep = new double[N][N];
		double[][] QFinal = new double[LOCAL_ROWS][N];

		int valueIndx = 1;
				

		for(int i = 0; i < N; i++){
			for(int j = 0; j < N; j++){
				QSecondStep[i][j] = Double.parseDouble(values[valueIndx]);
				valueIndx++;		
			}
		}

		for(int i = 0; i < LOCAL_ROWS; i++){
			for(int j = 0; j < N; j++){
				QFirstStep[i][j] = Double.parseDouble(values[valueIndx]);
				valueIndx++;		
			}
		}
				

		String QFinalStr = new String("");
				
		double sum = 0.0;
		for(int i = 0; i < LOCAL_ROWS; i++){
			for(int j = 0; j < N; j++){
				for(int k = 0; k < N; k++){
					sum +=	QFirstStep[i][k] * QSecondStep[k][j];					
				}
				QFinal[i][j] = sum;
				
				if(j ==N-1)
				  QFinalStr += sum + "\n";
				else
				  QFinalStr += sum + ",";
			
				sum = 0.0;
			}
		}
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		Path QFinalOutputPath = new Path("/home/zoe/hadoop-0.18.3/finalQ/Q"+ values[0]);

		BufferedWriter outputBr = new BufferedWriter(new OutputStreamWriter(fs.create(QFinalOutputPath,true)));

		outputBr.write(QFinalStr);
		outputBr.close();

		}
	}





	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		/**
		* This reduce method collects the R matrix(stored in A) from the mapper1. It performs a QR factorization
		* of R matrix. The new R matrix which is produced is the final R matrix which is emitted as output.
		* The new Q matrix is written in the folder ~/hadoop-0.8.13/my_input_2/. One single reduce task runs this
		* reduce method.
		*/
		public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter reporter) 			throws 	IOException {
			
			
			String[] values; 
			String line;
			//The A matrix contains the R matrix from the first step			
			double[][] A = new double[4 * N][N];	

			//Collection of R as a matrix
			while(value.hasNext()) {
				line = value.next().toString();
				values = line.split(",");
				
				int value_indx = 1;
				int row_start = Integer.parseInt(values[0]);

				for(int i = row_start; i < row_start + N; i++){
					for(int j = 0; j < N; j++) {
						A[i][j] = Double.parseDouble(values[value_indx]);
						value_indx++;
					}
				}

			}



			//QR decomposition of R matrix of the first step
			Matrix A_M = new Matrix(A);
			QRDecomposition QR = new QRDecomposition(A_M);

			//Get the final R array Matrix
			Matrix R_M = QR.getR();
			double[][] R = new double[N][N];	
			R = R_M.getArray();
			//The final R matrix is emitted in a file at the folder my_output
			String RFinalS;
			for (int i = 0; i < N; i++){
				RFinalS = " ";
				for (int j = 0;j < N;j++) {
					RFinalS += R[i][j] + " "; 
				} 
				RFinalS +="\n";
				output.collect(new Text(i +" "), new Text(RFinalS));
			}

			//Get the Q matrix
			Matrix Q_M = QR.getQ();
			double[][] Q = new double[4 * N][N];
			Q = Q_M.getArray();

			//Format the Q matrix to a string 
			String Q_S = new String("");

			for (int i=0; i < 4 * N; i++){
				for (int j = 0;j < N;j++) {
					if( ((i+1) % N == 0) && j == N -1 )
						Q_S += Q[i][j]; 
					else
						Q_S += Q[i][j] + ","; 
				} 

				if ((i+1) % N == 0){
					Q_S += "\n";
				}
			}
			

			//Write the Q matrix as a string to a file so as to go as input to the next Map
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			Path path = new Path("/home/zoe/hadoop-0.18.3/my_input2/Q_from_reduce");  
		
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
			br.write(Q_S);
			br.close();
		}
    	}







	/**
	* The function mergeQFIles merges the files Q_from_reduce from the second step and the Qi files from the first map.
	* It creates the file Q_input_for_mapper2 whose each line contains one submatrix Qi from the Q_from_reduce, and one
	* submatrix Qi from a file Qi from the first step. Namely, each line contains Q1, Q1,   Q2, Q2,   Q3, Q3 and Q4, Q4.
	* In this format, each map2 task will perform Qi * Qi.
	*/
	public static void mergeQFiles(){

		try{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
		
			Path QFromReduceInputPath = new Path("/home/zoe/hadoop-0.18.3/my_input2/Q_from_reduce");
			Path outputPath = new Path("/home/zoe/hadoop-0.18.3/my_input2/Q_input_for_mapper2");	

			BufferedReader readQReduceBr = new BufferedReader(new InputStreamReader(fs.open(QFromReduceInputPath)));
			BufferedWriter outputBr = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath,true)));

			for(int row = 0; row < 8 * N; row += LOCAL_ROWS){
				//Write one local matrix Q(output from reducer)	
				//We write at the beggining of each line the row indexof Q so as mapper2 know which Q submatrix will compute.			
				outputBr.write(row +","+ readQReduceBr.readLine() + "," );
				
				//Write one local matrix Q(multiple files Qi outputs from mapper1)
				Path inputPathQi = new Path("/home/zoe/hadoop-0.18.3/my_input2/"+ "Q"+ row);				
				BufferedReader QiInputStream = new BufferedReader(new InputStreamReader(fs.open(inputPathQi)));
				outputBr.write(QiInputStream.readLine() + "\n");	
				QiInputStream.close();	
				//We delete the Qi file from the first Map task				
				fs.delete(inputPathQi, true);
			}
			//We delete the Q_from_reduce file from the reduce task so as only Q_input_for_mapper2 will remain
			fs.delete(QFromReduceInputPath, true);
			outputBr.close();
		}
		catch(IOException ex){
			System.out.println(ex.toString());
		}
	
	}
	






	public static void main(String[] args) throws Exception {
		JobConf conf1 = new JobConf(Decomposition.class);
		conf1.setJobName("Decomposition");

		//conf.setInt("mapred.line.input.format.linespermap", 2);
	
		conf1.setOutputKeyClass(Text.class);
		conf1.setOutputValueClass(Text.class);

		conf1.setMapperClass(Map1.class);
		//conf1.setCombinerClass(Reduce.class);
		conf1.setReducerClass(Reduce.class);

		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);
  
		//If the output folder exists delete it
		FileSystem.get(conf1).delete(new Path(args[1]), true);

		FileInputFormat.setInputPaths(conf1, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf1, new Path(args[1]));
		JobClient.runJob(conf1);


		mergeQFiles();


		//Run second mapper to calculate the final Q
		JobConf conf2 = new JobConf(Decomposition.class);
		
		conf2.setOutputKeyClass(Text.class);
		conf2.setOutputValueClass(Text.class);

		conf2.setMapperClass(Map2.class);
		  
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		
		//If the output folder exists delete it
		FileSystem.get(conf1).delete(new Path("/home/zoe/hadoop-0.18.3/my_output2/"), true);

		FileInputFormat.setInputPaths(conf2, new Path("/home/zoe/hadoop-0.18.3/my_input2/"));
		FileOutputFormat.setOutputPath(conf2, new Path("/home/zoe/hadoop-0.18.3/my_output2/"));
		JobClient.runJob(conf2);


	}
}
