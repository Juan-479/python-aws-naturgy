# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 10:17:21 2021

@author: jtintore
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


n=["primary_output","lnd_alegaciones"]

datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "input_db", table_name = "lnd_alegaciones", transformation_ctx = "datasource_alegacion")
datasource2 = glueContext.create_dynamic_frame.from_catalog(database = "output_db", table_name = "primary_output", transformation_ctx = "datasource_primary")


df_standard_0=datasource2.toDF()
df_standard=df_standard_0.distinct()

df_alegacion_0=datasource1.toDF()
df_alegacion = df_alegacion_0.distinct()

if df_standard.count()!=0:
    print("New records from table %s: %s" %(n[0],df_standard.count()))
    
    if df_alegacion.count()!=0:
        print("New records from table %s: %s" %(n[1],df_alegacion.count()))
        
        # vamos a añadir una columna constante a cada df 
        # a df_alegacion sumamos si_alegacion, mejor 0 
        # a df_standard sumamos no_alegacion, mejor 1
        
        df2_alegacion = df_alegacion.withColumn("filtro", lit('0'))
        # ordenamos las columnas para que no se desordene la tabla
        df2_alegacion.select("tipo_reg","id","nombre","fecha","valor","valor_ant","medida1","medida2","medida3","aplicación_proceso","alegación","calibración","descripción","filtro")
        
        df2_standard = df_standard.withColumn("filtro", lit('1'))
        df2_standard.select("tipo_reg","id","nombre","fecha","valor","valor_ant","medida1","medida2","medida3","aplicación_proceso","alegación","calibración","descripción","filtro")
        
        df_alegacion.show()
        
        
        join_df=df2_alegacion.union(df2_standard)
        
        # ordenamos por la columna filtro, de menor a mayor asi alegaciones estaara primero en el df
        
        join_df2= join_df.orderBy('filtro')
        
        # ahora eliminamos los duplicados y nos quedamos con las alegaciones 
        
        new_df= join_df2.dropDuplicates(['id','fecha','aplicación_proceso'])
        
        #ahora eliminamos columna 'filtro'
        final_df=new_df.drop('filtro')    

        datatarget1 = DynamicFrame.fromDF(df_standard.repartition(1), glueContext, "datatarget1")
        
        datasink4 = glueContext.write_dynamic_frame.from_options(frame = datatarget1, connection_type = "s3", connection_options = {"path": "s3://bkt-transformed-data/output"}, format = "parquet", transformation_ctx = "datasink4")
    
    else:
       print("Table %s doesn't have new records" %(n[1])) 
        
else:
    print("Table %s doesn't have new records" %(n[0]))


print('Finale')

job.commit()