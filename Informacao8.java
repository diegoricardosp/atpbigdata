/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pucpr.atp;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author diego.plinta
 */
public class Informacao8 {
    
    public static class MapperInformacao8 extends Mapper<Object, Text, Text, LongWritable>{
   
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            try {
                            
                String linha = valor.toString();
                String[] campos = linha.split(";");
                if (campos.length == 10){
                    String info = campos[1] + "-" + campos[3];

                    Text chaveMap = new Text(info);
                    LongWritable valorMap = new LongWritable(Long.parseLong(campos[6]));

                    context.write(chaveMap, valorMap);
                }
            } catch (IOException | InterruptedException | NumberFormatException err){
               System.out.println(err);
            }
        }        
    }
    
    public static class ReducerInformacao8 extends Reducer<Text, LongWritable, Text, LongWritable>{
        
        @Override
        public void reduce(Text chave, Iterable<LongWritable>valores, Context context) throws IOException, InterruptedException{
            long soma = 0;
            
            for(LongWritable val:valores){
                soma += val.get();
            }
        
        context.write(chave, new LongWritable(soma));
        }
    }
            
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "/home2/ead2022/SEM1/diego.plinta/Documents/ATP/base_inteira.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/diego.plinta/Documents/ATP/informacao8";
        
        if(args.length == 2){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Informacao 8");
        
        job.setJarByClass(Informacao8.class);
        job.setMapperClass(MapperInformacao8.class);
        job.setReducerClass(ReducerInformacao8.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
               
        job.waitForCompletion(true);
    }
    
}
