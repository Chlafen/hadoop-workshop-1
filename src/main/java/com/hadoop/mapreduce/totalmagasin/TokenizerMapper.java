package com.hadoop.mapreduce.totalmagasin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable> {

  private final static IntWritable one = new IntWritable(1);
  private Text shopName = new Text();

  public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
    // [date] [temps] [magasin] [produit] [cout] [paiement]

    StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
    if (itr.countTokens() > 3) {
      itr.nextToken();
      itr.nextToken();
      String magasin = itr.nextToken();
      shopName.set(magasin);
      context.write(shopName, one);
    } else {
      context.write(new Text(""), new IntWritable(1));
    }
  }
}
