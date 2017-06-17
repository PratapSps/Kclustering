package kmeans.sps;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.FileSystemAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._Content;
import org.mortbay.log.Log;

import com.amazonaws.services.s3.transfer.Upload;

/**
 *
 * @author Survya Pratap Singh- UID:U00803205
 * Cloud computing project
 * K-clusting
 * Some part of the code has been taken from code provided by professor in class
 * Refernces:
 * http://www.macalester.edu/~shoop/sc13/hadoop/html/hadoop/wc-detail.html
 * http://kickstarthadoop.blogspot.com/2011/04/word-count-hadoop-map-reduce-example.html
 * https://www.tutorialspoint.com/map_reduce/map_reduce_combiners.htm
 */

public class KmeansImpl {
   
    public static ArrayList<Integer> LABELS=new ArrayList<Integer>(); //stores label from centroid file.
    public static ArrayList<double[]> CENTROIDS=new ArrayList<double[]>();//store centroids from centroid file
    public static ArrayList<double[]> UPLOADDATA=new ArrayList<double[]>();//store data form data file
    public static int ROWDATA=0;//no for data rows
    public static int COLDATA=0;// no of columns row
    public static ArrayList<double[]>UpdateCentroid=new ArrayList<double[]>(); //store updated centroid
    public static final String centroidFilePath="/user/pratap/center"; // path for centroid file
    public static String oldaggData; //used for finding convergance based on label change.
    public static String newaggData; //used for finding conbvergance based on label change


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
       
        Configuration config=new Configuration();
        //config.addResource(new Path("/usr/local/hadoop/hadoop-2.7.2/etc/hadoop/core-site.xml"));
        //config.addResource(new Path("/usr/local/hadoop/hadoop-2.7.2/etc/hadoop/hdfs-site.xml"));
        config.set("centroidfile",centroidFilePath);
        config.set("inputfile", args[0].toString());
        Job mrJob = Job.getInstance(config);
        mrJob.setJobName("KmeansImpl");
        mrJob.setJarByClass(KmeansImpl.class);
       
        mrJob.setMapperClass(Mapper_Label.class);
        mrJob.setCombinerClass(MapAggregater_combiner.class);
        mrJob.setReducerClass(Reducer_Centroids.class);
       
       
        Path outputF=new Path(args[1]);
        FileSystem filesystem=FileSystem.get(config);
        if(filesystem.exists(outputF)){
            filesystem.delete(outputF,true);
        }
       
        mrJob.setOutputKeyClass(IntWritable.class);
        mrJob.setOutputValueClass(Text.class);
       
        mrJob.setInputFormatClass(TextInputFormat.class);
        mrJob.setOutputFormatClass(TextOutputFormat.class);
       
        FileInputFormat.addInputPath(mrJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(mrJob,outputF);
        boolean checkConvergance=true;
        int itera=0;
       
        while(checkConvergance){
        mrJob.waitForCompletion(true);
       
        //updating centroid file with updated centroids.
        BufferedWriter writebuffer=new BufferedWriter(new OutputStreamWriter(filesystem.create(new Path(centroidFilePath),true)));
        StringBuilder ss=new StringBuilder();
        for(int i=0;i<UpdateCentroid.size();i++){
            String label=LABELS.get(i).toString();
            String Centroid=Arrays.toString(UpdateCentroid.get(i)).replaceAll("\\[", "").replaceAll("\\]", "");
            ss.append(label+"\t"+Centroid+"\n");
            if(i==UpdateCentroid.size()){
                ss.append(label+"\t"+Centroid);
            }
       
        }
       
        String line=ss.toString();
        writebuffer.write(line);
        writebuffer.close();
       
        //calling findconvergance method to check convergance based on number of iteration and threshold.
        /* if(findConvergance(0.001)){
                checkConvergance=false;
                System.out.printf("Convergance achieved in round %d .",itera);
            }
         
        if(itera==10){
            checkConvergance=false;
            System.out.printf("Reached maximum round of %d.",itera);
        }*/
       
       
     
       
        //code for finding convergance method based on label change.
            if(itera==0){
                oldaggData=newaggData;
                //newaggData="";
                checkConvergance=true;
               
            }
            if(itera>0){
                if(newaggData.equals(oldaggData)){
               
                    System.out.printf("No change in labels, thus Convergance is achieved.\n");
                    System.out.println("Centroid are:");
                    for(int i=0;i<UpdateCentroid.size();i++){
                        System.out.println(i+" :"+Arrays.toString(UpdateCentroid.get(i)));
                    }
                    checkConvergance=false;
                   
                   
                }
                else{
                    //oldaggData="";
                    oldaggData=newaggData;
                    checkConvergance=true;
                   
                }
               
               
            }
            itera++;
        }
       
    }//main method closed
   
   
    //method for finding convergance based on  threshold.
   
    public static boolean findConvergance(double convThreshold){
        double maxv = 0;
        for(int j=0;j<CENTROIDS.size();j++){
            double [] oldCen=CENTROIDS.get(j);
            double [] newCen=UpdateCentroid.get(j);
            double d= dist(oldCen,newCen);
            if (maxv<d)
                maxv = d;
        }

        if (maxv <convThreshold)
          return true;
        else
          return false;
       
       
    }
   
   
   
   
//mapper class
   
public static class Mapper_Label extends  Mapper<LongWritable, Text, IntWritable, Text>{

//setup method to read the centroid file and will identify the number of centroids and labels from the file.
   
    @SuppressWarnings("unchecked")
    public void setup(Context context)throws IOException, InterruptedException {
       
        Configuration conf=context.getConfiguration();
        try{
       
        FileSystem filesystem=FileSystem.get(conf);
        String readCentroid=null;
        BufferedReader readBuffer=new BufferedReader(new InputStreamReader(filesystem.open(new Path(conf.get("centroidfile")))));
        while((readCentroid=readBuffer.readLine())!=null){
            String [] splitData=readCentroid.split("\t");
            LABELS.add(Integer.parseInt(splitData[0]));
            String []cData=splitData[1].split(",");
            double []cinner=new double[cData.length];
            for(int i=0;i<cData.length;i++){
                cinner[i]=Double.parseDouble(cData[i]);
            }
            CENTROIDS.add(cinner);
           
            }
        readBuffer.close();
        BufferedReader readinput=new BufferedReader(new InputStreamReader(filesystem.open(new Path(conf.get("inputfile")))));
        String inputbuffer=null;
        int counter=0;
        while((inputbuffer=readinput.readLine())!=null){
            counter++;
           
            }
        ROWDATA=counter;
        readinput.close();
       
       
       
        }
        catch(Exception ex){
            Log.info("something is not right...fix the code.");
        }
       
    }
   
//map method will fetch the data from file and will compute distance for each data form given number of centroids . It will assign label to data based on distance computation.   
   
    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
       
        String line = value.toString();
        String []data=line.split("\t");
        String []realData=data[1].split(",");
        COLDATA=realData.length;
        double[] inner=new double[COLDATA];
        for(int i=0;i<realData.length;i++){
            inner[i]=Double.parseDouble(realData[i]);
        }
        UPLOADDATA.add(inner);
       
        if(UPLOADDATA.size()==ROWDATA){
           
           
            for(int i=0;i<UPLOADDATA.size();i++){
                double []rawData=UPLOADDATA.get(i);
                int label=closest(rawData);
                IntWritable Hlabel=new IntWritable(label);
                String rawformat=Arrays.toString(rawData);
                String finalData=rawformat.replaceAll("\\[","").replaceAll("\\]", "");
                //System.out.println(finalData);
                Text Hdata=new Text();
                Hdata.set(finalData);
       
                context.write(Hlabel,Hdata);   
               
            }
           
        }
       
    }
   
}

//find the closest centroid for the record kdata (taken from code provided by professor in class.)
private static int closest(double [] Kdata){

 double mindist = dist(Kdata, CENTROIDS.get(0));
 int label =LABELS.get(0);
 for (int i=1; i<LABELS.size();i++){
   double t = dist(Kdata, CENTROIDS.get(i));
   if (mindist>t){
     mindist = t;
     label = LABELS.get(i);
   }
 }
 return label;
}


//compute Euclidean distance between two vectors data and centroid(taken from code provided by professor in class.)
private static  double dist(double [] data, double [] centroid){
 double sum=0;
 for (int i=0; i<COLDATA;i++){
   double d = data[i]-centroid[i];
   sum += d*d;
 }
 return Math.sqrt(sum);
}


//combiner class , which will get key and value data from mapper and will save values in string for computing convergance based on labels.

public static class MapAggregater_combiner extends Reducer<IntWritable,Text,IntWritable,Text>{
    StringBuilder ss=new StringBuilder();
    int count=0;
   
    public void reduce(IntWritable label,Iterable<Text> Kdata,Context context) throws IOException, InterruptedException {
        for(Text val:Kdata){
            context.write(label, val);
            ss.append(val);
           
           
        }
        count++;
        
        if(count==LABELS.size()){
            newaggData=ss.toString();
           
        }
       
    }
   
   
   
}


//reducer class

public static class Reducer_Centroids extends Reducer<IntWritable, Text , IntWritable, Text>{
   
   
//reducer method to compute updated centroids based on the data with same label id or cluster id.   
    public void reduce(IntWritable label,Iterable<Text> Kdata,Context context) throws IOException, InterruptedException {
        int dataround=0;
        ArrayList<double[]> temp=new ArrayList<double[]>();
        for(Text val:Kdata){
            context.write(label, val);
            String ss=val.toString();
            String []dd=(ss.split(","));
            double []tempArray=new double[dd.length];
            for(int i=0;i<dd.length;i++){
                tempArray[i]=Double.parseDouble(dd[i]);
            }
            temp.add(tempArray);
            dataround++;
           
        }
        double []finalCentroid=new double[COLDATA];
        double [] sum=new double[COLDATA];
        for(int i=0;i<sum.length;i++){
            sum[i]=0.0;
            finalCentroid[i]=0.0;
        }
       
        for(int i=0;i<temp.size();i++){
            double []kk=temp.get(i);
            for(int j=0;j<kk.length;j++){
                sum[j]=sum[j]+kk[j];
            }
           
           
        }
       
        for(int i=0;i<sum.length;i++){
            double dd=sum[i]/dataround;
            finalCentroid[i]=dd;
        }
        UpdateCentroid.add(finalCentroid);
       
       

    }
   
}

//Method to update centroid file with updated centroids.

public static void UpdateCentoirdFile() throws IOException{
    Path cenPath=new Path(centroidFilePath);
    FileSystem filesystem=FileSystem.get(new Configuration());
    BufferedWriter writebuffer=new BufferedWriter(new OutputStreamWriter(filesystem.create(cenPath,true)));
    StringBuilder ss=new StringBuilder();
    for(int i=0;i<UpdateCentroid.size();i++){
        String label=LABELS.get(i).toString();
        String Centroid=Arrays.toString(UpdateCentroid.get(i)).replaceAll("\\[", "").replaceAll("\\]", "");
        ss.append(label+"\t"+Centroid+"\n");
        if(i==UpdateCentroid.size()){
            ss.append(label+"\t"+Centroid);
        }
   
    }
   
   
   
}


}