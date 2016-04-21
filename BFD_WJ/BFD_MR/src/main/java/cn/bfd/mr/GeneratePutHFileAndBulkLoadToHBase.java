package cn.bfd.mr;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.hbase.HBaseConfiguration;
        import org.apache.hadoop.hbase.KeyValue;
        import org.apache.hadoop.hbase.client.HTable;
        import org.apache.hadoop.hbase.client.Put;
        import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
        import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
        import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.LongWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        import org.apache.hadoop.util.GenericOptionsParser;

        import java.io.FileInputStream;
        import java.io.IOException;

@SuppressWarnings("deprecation")

public class GeneratePutHFileAndBulkLoadToHBase {

    public static class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable>{
        private Text wordText = new Text();
        private IntWritable one  = new IntWritable(1);
        protected void map(LongWritable key,Text value,Context context)throws IOException, InterruptedException{
            String line = value.toString();
            String[] wordArray = line.split(" ");
            for (String word : wordArray){
                wordText.set(word);
                context.write(wordText, one);
            }
        }
    }

    public static class WordCountReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        private  IntWritable result = new IntWritable();
        protected void reduce(Text key,Iterable<IntWritable>valueList,Context context) throws IOException,InterruptedException{
            int sum = 0;
            for (IntWritable value: valueList){
                sum += value.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }

    public static class convertResultToHfileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>{
        protected void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
            String wordCountStr = value.toString();
            String[] wordCountArray = wordCountStr.split("\t");
            String word = wordCountArray[0];

            byte[] rowKey = word.getBytes();
            ImmutableBytesWritable rowkeImmutableBytesWritable = new ImmutableBytesWritable(rowKey);
            byte[] family = "cf".getBytes();
            byte[] qualifier = "count".getBytes();
            byte[] hbaseValue = wordCountStr.getBytes();

            Put put = new Put(rowKey);
            put.add(family,qualifier,hbaseValue);
            KeyValue writeKeyValue = new KeyValue(rowKey,family,qualifier,hbaseValue);
            context.write(rowkeImmutableBytesWritable, writeKeyValue);
        }
    }


    @SuppressWarnings("deprecation")
    public static void main(String args[]) throws Exception{
        Configuration conf = new Configuration();
//         conf.addResource(new FileInputStream("conf/core-site.xml"));
//         conf.addResource(new FileInputStream("conf/hdfs-site.xml"));
//         conf.addResource(new FileInputStream("conf/yarn-site.xml"));
//         conf.addResource(new FileInputStream("conf/mapred-site.xml"));
        String[] dfsArgs = new GenericOptionsParser(conf,args).getRemainingArgs();

        Job job = new Job(conf,"bfdTest");
        job.setJarByClass(GeneratePutHFileAndBulkLoadToHBase.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("hdfs://bfdhadoop26/user/lap/TestHFileInput.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://bfdhadoop26/user/lap/output"));

        int jobComplete = job.waitForCompletion(true)?0:1;

        Job convertResultToHfile = new Job(conf,"bfdBulkload");
        convertResultToHfile.setJarByClass(convertResultToHfileMapper.class);
        convertResultToHfile.setMapperClass(convertResultToHfileMapper.class);
        convertResultToHfile.setOutputKeyClass(ImmutableBytesWritable.class);
        convertResultToHfile.setOutputValueClass(Put.class);

        FileInputFormat.addInputPath(convertResultToHfile, new Path("hdfs://bfdhadoop26/user/lap/output"));
        FileOutputFormat.setOutputPath(convertResultToHfile, new Path("hdfs://bfdhadoop26/user/lap/output2"));

        Configuration hbaseConfiguration=HBaseConfiguration.create();
        hbaseConfiguration.addResource(new FileInputStream("conf/hbase-site.xml"));
        HTable wordcountTable = new HTable(hbaseConfiguration, "word_count");
        HFileOutputFormat.configureIncrementalLoad(convertResultToHfile, wordcountTable);


        int convertWrodCountJob = convertResultToHfile.waitForCompletion(true)?1:0;
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConfiguration);
        loader.doBulkLoad(new Path("hdfs://bfdhadoop26/user/lap/output2"), wordcountTable);
        System.exit(convertWrodCountJob);
    }

}
