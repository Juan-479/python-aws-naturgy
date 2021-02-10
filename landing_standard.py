# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 10:16:34 2021

@author: jtintore
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col,when,count
from awsglue.job import Job
import pandas as pd

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark = SparkSession.builder.appName('pyspark - create empty dataframe').getOrCreate()

#List of the landing tables.

table_list=["lnd_atr_ans_kpi","lnd_atr_q","lnd_neg_zeus_elec","lnd_neg_zeus_gas","lnd_sspp_ned","lnd_sspp_ufd"]
new_records_list=[]

#Final table Schema
schema = StructType([StructField('tipo_reg',LongType(),True),StructField('id',StringType(),True),StructField('nombre',StringType(),True),
                StructField('fecha',StringType(),True),StructField('valor',StringType(),True),StructField('valor_ant',StringType(),True),
                StructField('medida1',StringType(),True),StructField('medida2',StringType(),True),StructField('medida3',StringType(),True),
                StructField('aplicación_proceso',StringType(),True),StructField('alegación',StringType(),True),
                StructField('calibración',LongType(),True),StructField('descripción',StringType(),True) 
                    ])
                
df_f = spark.createDataFrame(sc.emptyRDD(),schema)

i=0

for n in table_list:
    i+=1
   
    datasource = glueContext.create_dynamic_frame.from_catalog(database = "input_db", table_name = n, transformation_ctx = "datasource%s"%(n))
    #datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "input_db", table_name = n)
    
    df0=datasource.toDF()
    
    if df0.count()!=0:
        new_records_list.append(n)
        print("New records from table %s: %s" %(n,df0.count()))

        if len(new_records_list)>0:
            
            j=0
        
            for m in new_records_list:
                
                j+=1
                
                datasource2 = glueContext.create_dynamic_frame.from_catalog(database = "input_db", table_name = m, transformation_ctx = "datasource%s"%(m))
                
                print(j)
                
                if j==1:
                    
                    df_concat=datasource2.toDF()
                    print("Tabla %s añadida al DF" %(m))
                    
                elif j==2 and df_concat.count()!=0:
                
                    df1=datasource2.toDF()
                    
                    if len(df_concat.columns)==len(df1.columns):
                        df_concat = df_concat.union(df1)
                        print("Tabla %s concatenada al DF" %(m))
                       
                    else:
                        print("Número de columnas diferentes. La tabla %s tiene  %s y la tabla %s tiene  %s" %(new_records_list[0],len(df_concat.columns),m,len(df1.columns))) 
                            
                        
                elif j>2 and df_concat.count()!=0:
                    df2=datasource2.toDF()
                    
                    if len(df_concat.columns)==len(df2.columns):
                        df_concat = df_concat.union(df2)
                        print("Tabla %s concatenada al DF" %(m))
                        
                    else:
                        print("Número de columnas diferentes. El DF tiene  %s y la tabla %s tiene  %s" %(len(df_concat.columns),m,len(df2.columns))) 
                        
                else:
                    print("DF vacío")
        else:
            print("No records in new list")
               
        
        count=df_concat.count()
        print(count)
        
        
        #df_new.printSchema()

        
        if count!=0 and len(df_f.columns)==len(df_concat.columns):
            df_concat_drop=df_concat.distinct()
            #df_concat_drop_2=df_concat_drop.dropDuplicates(['id','fecha','aplicación_proceso'])
            
            df_new = df_concat_drop.withColumn("calibración", df_concat_drop["calibración"].cast(StringType()))
        
            datatarget1 = DynamicFrame.fromDF(df_new.repartition(1), glueContext, "datatarget1")
            datasink4 = glueContext.write_dynamic_frame.from_options(frame = datatarget1, connection_type = "s3", connection_options = {"path": "s3://bkt-transformed-data/standard/" }, format = "parquet", transformation_ctx = "datasink4")
            print("ok")
        else:
            print("Data Frame Empty")
    
    else:
        print("Table %s doesn't have new records" %(n))

print("Finished-----------")

job.commit()