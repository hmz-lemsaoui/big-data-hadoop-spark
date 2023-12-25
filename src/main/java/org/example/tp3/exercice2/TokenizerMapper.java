package org.example.tp3.exercice2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

// fonction Map
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String a=itr.nextToken();
            if(a.length()>=10) {
                word.set("long");
            }else if (a.length()>=5){
                word.set("moyen");
            }else {
                word.set("court");
            }
            context.write(word, one);
        }
    }

}
