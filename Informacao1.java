/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pucpr.atp;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class Informacao1 {
    
    public static class MapperInformacao1 extends Mapper<Object, Text, Text, IntWritable>{
       
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException{
            String linha = valor.toString();
            String[] campos = linha.split(";");
            if (campos.length == 10){
                String info = campos[0];
                int qtd = 1;
                
                Text chaveMap = new Text(info);
                IntWritable valorMap = new IntWritable(1);
                
                context.write(chaveMap, valorMap);
            }
            
        }        
    }
    
    public static class ReducerInformacao1 extends Reducer<Text, IntWritable, Text, IntWritable>{
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException{
            int soma = 0;
            for(IntWritable val: valores){
                soma += val.get();
            }
        IntWritable valorSaida = new IntWritable(soma);
        context.write(chave, valorSaida);
        }
    }
            
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
        
        String arquivoEntrada = "/home2/ead2022/SEM1/diego.plinta/Documents/ATP/base_inteira.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/diego.plinta/Documents/ATP/informacao1";
        
        if(args.length == 2){
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Informacao 1");
        
        job.setJarByClass(Informacao1.class);
        job.setMapperClass(MapperInformacao1.class);
        job.setReducerClass(ReducerInformacao1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
               
        job.waitForCompletion(true);
    }
    
}
