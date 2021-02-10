# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 10:14:03 2021

@author: jtintore
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
import pandas as pd
from datetime import datetime
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "input_db", table_name = "lnd_descripcion_ticket", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []


datasource_descrip = glueContext.create_dynamic_frame.from_catalog(database = "input_db", table_name = "lnd_descripcion_ticket", transformation_ctx = "datasource_descrip")
datasource_estado = glueContext.create_dynamic_frame.from_catalog(database = "output_db", table_name = "sp_special01_estadosticket",transformation_ctx = "datasource_estado")
df=datasource_descrip.toDF()
df_estadoticket=datasource_estado.toDF()

if df.count()!=0:
    print("New records from table %s: %s" %("lnd_descripcion_ticket",df.count()))
    if df_estadoticket.count()!=0:
        print("New records from table %s: %s" %("sp_special01_estadosticket",df_estadoticket.count()))

        # eliminar de df (descripcion ticet) la columna date
        df_df = df.select("*").toPandas()
        print (df_df.columns)
        del df_df['date']
        
        df=spark.createDataFrame(df_df)
        
        
        df_union=df.join(df_estadoticket, "id_ticket")#, how='outer')
        #df_union.show()
        #ahora lo pasamos a Pandas y operamos con el 
        pdf = df_union.select("*").toPandas()
        pdf.fillna(0.0, inplace=True)   
        pdf.loc[pdf['volumen_total_casos'] == 0.0, 'volumen_total_casos'] = 1.0
        pdf.loc[pdf['volumen_total_casos'] == -1.0, 'volumen_total_casos'] = 1.0
        pdf.loc[pdf['total_casos_ok'] == -1.0, 'total_casos_ok'] = 1.0
        #ahora creamos lista vacia donde vamos a almacenar total_casos_ok
        #print(pdf['volumen_total_casos'])
        total_ok=[]
        for i in pdf.loc[:,'volumen_total_casos']:#volumen_total_casos
            total_ok.append(i)
        #print (total_ok)
        #ahora almacenamos los datos de ok_ko
        lista_okko=[]
        for i in pdf.loc[:,'ok_ko']:
            lista_okko.append(i)
        #print(lista_okko)
        #print(len(lista_okko))
         
        sum_casos_ok=[]
        for i in range(len(lista_okko)):
            if lista_okko[i]=='OK':
                sum_casos_ok.append(str(total_ok[i]))
            else:
                sum_casos_ok.append('0.0')
                
        
        print(len(sum_casos_ok))
        new_items = [x if x!='nan' else '0.0' for x in sum_casos_ok]
        
        pdf.insert(14, 'sum_casos_ok', new_items)  
        
        #pdf.sort_values('date',ascending=False, inplace=True)
        #pdf.drop_duplicates('id_ticket', keep="first", inplace=True)
        #pdf.sort_index(inplace=True) 
        
        ###########################################################
        nuevas_fechas=[]
        for i in pdf.loc[:,'date']:
            d = datetime.strptime(i, '%Y%m%d')
            resultado= d.replace(day=1)
            resultado_2= resultado.strftime("%d/%m/%Y")
            nuevas_fechas.append(resultado_2)
        
        del pdf['date']
        pdf['date'] = nuevas_fechas
        #############################################################
        
        
        #pdf['sum_casos_ok'].replace('nan','0.0')
        #pdf.fillna(0, inplace=True)
        #print (new_items) #(sum_casos_ok)      
        df_2=spark.createDataFrame(pdf)
        df_3=df_2.distinct()
        df_4=df_3.dropDuplicates(['id_ticket','proyecto','flujo','date'])
        
        df_new = df_4.withColumn("user_name", df_4["user_name"].cast(StringType()))
        
        #df_estadoticket.show()
        datatarget1 = DynamicFrame.fromDF(df_new.repartition(1), glueContext, "datatarget1")
        datasink4 = glueContext.write_dynamic_frame.from_options(frame = datatarget1, connection_type = "s3", connection_options = {"path": "s3://bkt-transformed-data/special01_listatickets"}, format = "parquet", transformation_ctx = "datasink4")
    else:
         print("Table %s doesn't have new records" %("sp_special01_estadosticket"))
else:
    print("Table %s doesn't have new records" %("lnd_descripcion_ticket"))
    
print('FINISH----------')
job.commit()