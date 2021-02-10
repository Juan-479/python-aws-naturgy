# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 10:15:25 2021

@author: jtintore
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


n=["standard","sp_special01_indicadores_final"]

datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "output_db", table_name = "standard", transformation_ctx = "datasource_standard")
datasource2 = glueContext.create_dynamic_frame.from_catalog(database = "output_db", table_name = "sp_special01_indicadores_final", transformation_ctx = "datasource_final")


df1_0=datasource1.toDF()
df1_1 = df1_0.distinct()
df1=df1_1.dropDuplicates(['id','fecha','aplicación_proceso'])
df2_0=datasource2.toDF()
#df2_1= df2_0.select("*").toPandas()
#[float(x.replace(',','.')) for x in df_bd['comas_puntos']
df2 = df2_0.withColumn('valor', translate(col('valor'), '.', ','))
# ordenamos las columnas
df2.select("tipo_reg","id","nombre","fecha","valor","valor_ant","medida1","medida2","medida3","aplicacion_proceso","alegacion","calibracion","descripcion")

if df1.count()!=0:
    print("New records from table %s: %s" %(n[0],df1.count()))
    
    if df2.count()!=0:
        print("New records from table %s: %s" %(n[1],df2.count()))
        final=df1.union(df2)
        
        distinctDF = final.distinct()

        datatarget1 = DynamicFrame.fromDF(distinctDF.repartition(1), glueContext, "datatarget1")
        
        datasink4 = glueContext.write_dynamic_frame.from_options(frame = datatarget1, connection_type = "s3", connection_options = {"path": "s3://bkt-transformed-data/primary_output"}, format = "parquet", transformation_ctx = "datasink4")
    
    else:
       print("Table %s doesn't have new records" %(n[1])) 
        
else:
    print("Table %s doesn't have new records" %(n[0]))


print('Finale')

job.commit()