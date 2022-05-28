from pyspark.sql import SparkSession
import argparse
spark = SparkSession\
    .builder\
    .appName("5349-a2")\
    .getOrCreate()
sc=spark.sparkContext

parser = argparse.ArgumentParser()
parser.add_argument("--output", help="the output path",
                        default='ass2_out')
args = parser.parse_args()


input_path='s3://comp5349-2022/test.json'
# input_path='s3://comp5349-2022/train_separate_questions.json'
# input_path='s3://comp5349-2022/CUADv1.json'

output_path = args.output

stage_1=spark.read.json(input_path)

from pyspark.sql.functions import *
from pyspark.sql.types import *
def flat_df(df):
  # flat the data of the given dataframe
  return df.select(explode('data').alias('data'))\
        .select('data.title','data.paragraphs').toDF('title','paragraphs')\
        .select('title',explode('paragraphs').alias('paragraphs'))\
        .select('title','paragraphs.*').toDF('title','context','qas')\
        .withColumn("context_id", monotonically_increasing_id())\
        .select('title','context','context_id',explode('qas').alias('qas'))\
        .select('title','context','context_id','qas.*').toDF('title','context','context_id','answers','id','is_impossible','question')\
        .select('title','context','context_id',explode_outer('answers').alias('answer'),'id','is_impossible','question')\
        .select('title','context','context_id','answer.*','id','is_impossible','question')\
        .toDF('title','context','context_id','answer_start','text','id','is_impossible','question')\
        .na.fill(value=0)\
        .withColumn('answer_end',when(col('is_impossible')==False, col('answer_start')+length(col('text'))).otherwise(0))\
        .withColumn('answer_start',when(col('is_impossible')==False, col('answer_start')).otherwise(0))\
        .select('context','context_id','question','answer_start','answer_end','is_impossible',concat_ws('_',col('id'),col('answer_start')).alias('id'))\
        .toDF('context','context_id','question','answer_start','answer_end','is_impossible','id')

df_flat=flat_df(stage_1)
df_flat.show(5)

def func_1(x):
  l=int(x['s'])
  res=[]
  if l<4096:
    res.append((0,l))
  else:
    for i in range(l//2048 -1):
      res.append((i*2048,i*2048+4096))
    res.append((l//2048*2048,l))
  return [res]
def split_df(df):
  a=df.select('*',length('context').alias('s'))
  b=a.select('s').rdd.map(lambda x:func_1(x)).toDF(['source_'])
  c=a.join(b).select('*',transform('source_',lambda x:col('context')[x._1:x._2]).alias('source')).withColumn('source',arrays_zip('source_','source'))
  c=c.select('source','question','answer_start','answer_end','is_impossible','id','context_id') 
  c=c.select(explode('source').alias('source'),'question','answer_start','answer_end','is_impossible','id','context_id')
  c=c.select('*',when(c.answer_start>c.source.source_._2,0).when(c.answer_end<c.source.source_._1,0).otherwise(col('answer_start')).alias('start'))
  c=c.select('*',when(c.answer_start>c.source.source_._2,0).when(c.answer_end<c.source.source_._1,0).otherwise(col('answer_end')).alias('end'))
  return c.select(['start','end','is_impossible','id','context_id','source.source','question'])
stage_2=split_df(df_flat)
# stage_2.show(truncate=False)
stage_2.show()
from pyspark.sql.window import Window

def balance_df(df):
  # a positive + possible negative
  a=df.filter('is_impossible=False').orderBy(rand()).dropDuplicates(['start','id']).orderBy('id')
  # w1 = Window.partitionBy("context_id") 
  # positive numbers in contract
  # b=a.filter('start!=0').withColumn('n',count('is_impossible').over(w1))
  w2 = Window.partitionBy("context_id").orderBy(rand())
  b=df.filter('is_impossible=True').withColumn('row',row_number().over(w2)).filter('row<10').drop('row')
  return a.union(b).orderBy('id').select('source','question','start','end')

stage_3=balance_df(stage_2)
stage_3.show()
stage_3.write.csv(output_path)
spark.stop()