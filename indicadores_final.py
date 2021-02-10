# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 10:14:29 2021

@author: jtintore
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import col,when,count
from awsglue.job import Job
import pandas as pd

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

sql_context=SQLContext(sc)
spark = SparkSession.builder.appName('pyspark - create empty dataframe').getOrCreate()

datasource2 = glueContext.create_dynamic_frame.from_catalog(database = "input_db", table_name = "special01_categories")# transformation_ctx = "datasource2")
datasource_lista = glueContext.create_dynamic_frame.from_catalog(database = "output_db", table_name = "special01_listatickets", transformation_ctx = "datasource_lista")

#pasamos a data frame los datasources
df=datasource_lista.toDF() ###df de listatickets
df_2=datasource2.toDF()###df de categories


if df.count()!=0:
    print("New records from table %s: %s" %("special01_listatickets",df.count()))

    df.fillna(0)#llenamos todos los Null con 0.0 para poder operar con ellos 
    
    
    
    
    #cambiamos a todo minusculas la columna que nos va a servir para unir las tablas en el join final
    minusculas= df_2.select("*").toPandas()
    print(minusculas.columns)
    del minusculas['col8']
    minusculas.columns=['id_indicador','tipo_indicador','nombre_indicador','proyecto','id_grupo','flujo','tipo_solicitud','fichero','id_pro_flu_sol']
    # parches para solucionar discrepancias entre tablas
    minusculas.replace({"Alta de obras en plan SGT y GODA": "Alta de obras de plan SGT y GODA", "Alta solicitudes": "Altas Solicitudes",
    "Propuestas de Inversión PIs PGAs":"Propuestas de inversión PIs PGAs","Órdenes de trabajo OTs":"Ordenes de trabajo OTs",
    "Ordenes de Entrega y Certificaciones":"Ordenes Entrega y Certificaciones"},inplace=True)
    
    #ahora uos pequeños cambios en id_pro_flu_sol
    minusculas.replace({"SSPP Gestión económica_Órdenes de trabajo OTs_Todas":"SSPP Gestión económica_Ordenes de trabajo OTs_Todas",
    "SSPP Operaciones_SGT_Alta de obras en plan SGT y GODA":"SSPP Operaciones_SGT_Alta de obras de plan SGT y GODA",
    "SSPP Operaciones_SGM_Alta solicitudes":"SSPP Operaciones_SGM_Altas Solicitudes",
    "SSPP Operaciones_SGT_Adjudicación de trabajos en SGT ":"SSPP Operaciones_SGT_Adjudicación de trabajos en SGT",
    "SSPP Operaciones_SGT_Modificación trabajos en SGT ":"SSPP Operaciones_SGT_Modificación trabajos en SGT",
    "SSPP Operaciones_SGM_Anulación y Modificación Solicitudes ":"SSPP Operaciones_SGM_Anulación y Modificación Solicitudes",
    "SSPP Operaciones_SGM_Solicitud de Verificación de Reformados ":"SSPP Operaciones_SGM_Solicitud de Verificación de Reformados",
    "SSPP Recursos Medios y Servicios_Vehículos_Todas ":"SSPP Recursos Medios y Servicios_Vehículos_Todas",
    "SSPP Compras_Ordenes de Entrega y Certificaciones_Todas":"SSPP Compras_Ordenes Entrega y Certificaciones_Todas"},inplace=True)
    
    
    minusculas["id_pro_flu_sol"] = minusculas["id_pro_flu_sol"].str.lower()
    
    df_2=spark.createDataFrame(minusculas)
    
    
    
    df.registerTempTable("indicadores")
    df_2.registerTempTable("indicadores2")
    
    ################################################################
    ######################    Q    #################################
    #primero vamos a hacer un primer filtrado de categories por tipo_indicador empezando por Q
    df_q=df_2.filter(df_2.tipo_indicador == "Q") #filtrado por tipo_indicador Q
    
    #df_q.show()
    
    #ahora sacamos los valores de la columna de flujo segun su solicitud
    df_q_todas=df_q.filter(df_q.tipo_solicitud == "Todas") #aqui estan los flujos que tienen la solicitud como todas
    df_q_no=df_q.filter(df_q.tipo_solicitud != "Todas") #aqui estan los flujos que tienen una solicitud distinta a todas
    
    
    
    flujo_1 = df_q_todas.select('flujo').toPandas()
    flujo_1.drop_duplicates()
    flujo_2=df_q_no.select('flujo').toPandas()
    flujo_2.drop_duplicates()
    
    flujo_1s=[]
    flujo_2s=[]
    #sacamos los nombres de los flujo en dos listas las que tienen solicitud= a todas( flujo_1s) y las que no (flujo_2s)
    for i in flujo_1.iloc[:,0]:
        flujo_1s.append(i)
    flujo_1s=tuple(flujo_1s)
    for i in flujo_2.iloc[:,0]:
        flujo_2s.append(i)
    flujo_2s=tuple(flujo_2s)
    
    #print(flujo_1s)
    print(flujo_2s)
    flujo_q=[flujo_1s,flujo_2s]
    ################################################################
    ######################   ANS   #################################
    #primero vamos a hacer un primer filtrado de categories por tipo_indicador empezando por Q
    df_ans=df_2.filter(df_2.tipo_indicador == "ANS") #filtrado por tipo_indicador ANS
    
    #df_q.show()
    
    #ahora sacamos los valores de la columna de flujo segun su solicitud
    df_ans_todas=df_ans.filter(df_q.tipo_solicitud == "Todas") #aqui estan los flujos que tienen la solicitud como todas
    df_ans_no=df_ans.filter(df_q.tipo_solicitud != "Todas") #aqui estan los flujos que tienen una solicitud distinta a todas
    
    flujo_3 = df_ans_todas.select('flujo').toPandas()
    flujo_4=df_ans_no.select('flujo').toPandas()
    
    flujo_3s=[]
    flujo_4s=[]
    #sacamos los nombres de los flujo en dos listas las que tienen solicitud= a todas( flujo_1s) y las que no (flujo_2s)
    for i in flujo_3.iloc[:,0]:
        flujo_3s.append(i)
    flujo_3s=tuple(flujo_3s)
    for i in flujo_4.iloc[:,0]:
        flujo_4s.append(i)
    flujo_4s=tuple(flujo_4s)
    
    #print(flujo_1s)
    flujo_ans=[flujo_3s,flujo_4s]
    ################################################################
    ######################   KPI   #################################
    #primero vamos a hacer un primer filtrado de categories por tipo_indicador empezando por KPI
    df_kpi=df_2.filter(df_2.tipo_indicador == "KPI") #filtrado por tipo_indicador KPI
    
    #df_q.show()
    
    #ahora sacamos los valores de la columna de flujo segun su solicitud
    df_kpi_todas=df_kpi.filter(df_kpi.tipo_solicitud == "Todas") #aqui estan los flujos que tienen la solicitud como todas
    df_kpi_no=df_kpi.filter(df_kpi.tipo_solicitud != "Todas") #aqui estan los flujos que tienen una solicitud distinta a todas
    
    flujo_5 = df_kpi_todas.select('flujo').toPandas()
    flujo_6=df_kpi_no.select('flujo').toPandas()
    
    flujo_5s=[]
    flujo_6s=[]
    #sacamos los nombres de los flujo en dos listas las que tienen solicitud= a todas( flujo_1s) y las que no (flujo_2s)
    for i in flujo_5.iloc[:,0]:
        flujo_5s.append(i)
    flujo_5s=tuple(flujo_5s)
    for i in flujo_6.iloc[:,0]:
        flujo_6s.append(i)
    flujo_6s=tuple(flujo_6s)
    
    #print(flujo_1s)
    flujo_kpi=[flujo_5s,flujo_6s]
    ################################################################
    ########### Filtro SQL y JOIN con categories : Q ###############
    a=0
    for flujo in flujo_q:
        a+=1
        #vamos a hacer dos querys apra diferenciar por tipo de solicitud
         ####{0} as Id,####esto produce unindice segun de que iteracion sea
         ###a nosotros no nos interesa es soslo meramente informativo
        query = """SELECT
            {0} as Id,
            date as fecha,
            proyecto,
            flujo,
            'Todas' as tipo_solicitud,
            sum(volumen_total_casos) as volumen_total_casos,
            sum(diferencia_tiempo_festivos) as total_tiempo,
            sum(sum_casos_ok) as sum_casos_ok,
            (sum(sum_casos_ok))/(sum(volumen_total_casos)) as ratio_ok,
            (sum(diferencia_tiempo_festivos))/(sum(volumen_total_casos)) as ratio_tiempop
            FROM indicadores
            WHERE flujo in {1}
            GROUP BY proyecto, flujo, date
            """.format(a,flujo)
        
        #este query va para los que se tiene que diferenciar por solicitud
        query2 = """SELECT
            {0} as Id,
            date as fecha,
            proyecto,
            flujo,
            tipo_solicitud as tipo_solicitud,
            sum(volumen_total_casos) as volumen_total_casos,
            sum(diferencia_tiempo_festivos) as total_tiempo,
            sum(sum_casos_ok) as sum_casos_ok,
            (sum(sum_casos_ok))/(sum(volumen_total_casos)) as ratio_ok,
            (sum(diferencia_tiempo_festivos))/(sum(volumen_total_casos)) as ratio_tiempop
            FROM indicadores
            WHERE flujo in {1}
            GROUP BY proyecto, flujo, tipo_solicitud, date
            """.format(a,flujo)
            
        if a==1:
            results = sql_context.sql(query)
        else:
            results2 = sql_context.sql(query2)
            results=results.union(results2)
    
    #vamos a crear una columna mas en results que sera la concatenacion de proyeto_flujo_solicitud y todo enminuculas
    minusculas_2 = results.select("*").toPandas()
    minusculas_2["id_pro_flu_sol"]=minusculas_2['proyecto']+'_'+minusculas_2['flujo']+'_'+minusculas_2['tipo_solicitud']
    minusculas_2["id_pro_flu_sol"] = minusculas_2["id_pro_flu_sol"].str.lower()
    minusculas_2['proyecto']=minusculas_2['proyecto'].str.lower()
    results=spark.createDataFrame(minusculas_2)
    
    #diferencia_tiempo_festivos
    minusculas= df_q.select("*").toPandas()
    minusculas["proyecto"] = minusculas["proyecto"].str.lower()
    df_q=spark.createDataFrame(minusculas)
    
    
    #ahora vamos a intentar un join directamente con q a ver que pasa   le he quitado esto (,'proyecto')
    new_df_q = results.join(df_q, on=['id_pro_flu_sol'], how='inner')
    
    #new_df.show()
    ##################################################################
    ########### Filtro SQL y JOIN con categories : ANS ###############
    a=0
    for flujo in flujo_ans:
        a+=1
        #vamos a hacer dos querys apra diferenciar por tipo de solicitud
         ####{0} as Id,####esto produce unindice segun de que iteracion sea
         ###a nosotros no nos interesa es soslo meramente informativo
        query = """SELECT
            {0} as Id,
            date as fecha,
            proyecto,
            flujo,
            'Todas' as tipo_solicitud,
            sum(volumen_total_casos) as volumen_total_casos,
            sum(diferencia_tiempo_festivos) as total_tiempo,
            sum(sum_casos_ok) as sum_casos_ok,
            (sum(sum_casos_ok))/(sum(volumen_total_casos)) as ratio_ok,
            (sum(diferencia_tiempo_festivos))/(sum(volumen_total_casos)) as ratio_tiempop
            FROM indicadores
            WHERE flujo in {1}
            GROUP BY proyecto, flujo, date
            """.format(a,flujo)
        
        #este query va para los que se tiene que diferenciar por solicitud
        query2 = """SELECT
            {0} as Id,
            date as fecha,
            proyecto,
            flujo,
            tipo_solicitud as tipo_solicitud,
            sum(volumen_total_casos) as volumen_total_casos,
            sum(diferencia_tiempo_festivos) as total_tiempo,
            sum(sum_casos_ok) as sum_casos_ok,
            (sum(sum_casos_ok))/(sum(volumen_total_casos)) as ratio_ok,
            (sum(diferencia_tiempo_festivos))/(sum(volumen_total_casos)) as ratio_tiempop
            FROM indicadores
            WHERE flujo in {1}
            GROUP BY proyecto, flujo, tipo_solicitud, date
            """.format(a,flujo)
            
        if a==1:
            results = sql_context.sql(query)
        else:
            results2 = sql_context.sql(query2)
            results=results.union(results2)
    
    #vamos a crear una columna mas en results que sera la concatenacion de proyeto_flujo_solicitud y todo enminuculas
    minusculas_2 = results.select("*").toPandas()
    minusculas_2["id_pro_flu_sol"]=minusculas_2['proyecto']+'_'+minusculas_2['flujo']+'_'+minusculas_2['tipo_solicitud']
    minusculas_2["id_pro_flu_sol"] = minusculas_2["id_pro_flu_sol"].str.lower()
    minusculas_2['proyecto']=minusculas_2['proyecto'].str.lower()
    results=spark.createDataFrame(minusculas_2)
    
    minusculas= df_ans.select("*").toPandas()
    minusculas["proyecto"] = minusculas["proyecto"].str.lower()
    df_ans=spark.createDataFrame(minusculas)
    #ahora vamos a intentar un join directamente con q a ver que pasa
    new_df_ans = results.join(df_ans, on=['id_pro_flu_sol'], how='inner')
    
    ##################################################################
    ########### Filtro SQL y JOIN con categories : KPI ###############
    a=0
    for flujo in flujo_kpi:
        a+=1
        #vamos a hacer dos querys apra diferenciar por tipo de solicitud
         ####{0} as Id,####esto produce unindice segun de que iteracion sea
         ###a nosotros no nos interesa es soslo meramente informativo
        query = """SELECT
            {0} as Id,
            date as fecha,
            proyecto,
            flujo,
            'Todas' as tipo_solicitud,
            sum(volumen_total_casos) as volumen_total_casos,
            sum(diferencia_tiempo_festivos) as total_tiempo,
            sum(sum_casos_ok) as sum_casos_ok,
            (sum(sum_casos_ok))/(sum(volumen_total_casos)) as ratio_ok,
            (sum(diferencia_tiempo_festivos))/(sum(volumen_total_casos)) as ratio_tiempop
            FROM indicadores
            WHERE flujo in {1}
            GROUP BY proyecto, flujo, date
            """.format(a,flujo)
        
        #este query va para los que se tiene que diferenciar por solicitud
        query2 = """SELECT
            {0} as Id,
            date as fecha,
            proyecto,
            flujo,
            tipo_solicitud as tipo_solicitud,
            sum(volumen_total_casos) as volumen_total_casos,
            sum(diferencia_tiempo_festivos) as total_tiempo,
            sum(sum_casos_ok) as sum_casos_ok,
            (sum(sum_casos_ok))/(sum(volumen_total_casos)) as ratio_ok,
            (sum(diferencia_tiempo_festivos))/(sum(volumen_total_casos)) as ratio_tiempop
            FROM indicadores
            WHERE flujo in {1}
            GROUP BY proyecto, flujo, tipo_solicitud, date
            """.format(a,flujo)
            
        if a==1:
            results = sql_context.sql(query)
        else:
            results2 = sql_context.sql(query2)
            results=results.union(results2)
    
    #vamos a crear una columna mas en results que sera la concatenacion de proyeto_flujo_solicitud y todo enminuculas
    minusculas_2 = results.select("*").toPandas()
    minusculas_2["id_pro_flu_sol"]= minusculas_2['proyecto']+'_'+minusculas_2['flujo']+'_'+minusculas_2['tipo_solicitud']
    minusculas_2["id_pro_flu_sol"] = minusculas_2["id_pro_flu_sol"].str.lower()
    minusculas_2['proyecto']=minusculas_2['proyecto'].str.lower()
    results=spark.createDataFrame(minusculas_2)
    
    minusculas= df_kpi.select("*").toPandas()
    minusculas["proyecto"] = minusculas["proyecto"].str.lower()
    df_kpi=spark.createDataFrame(minusculas)
    
    
    
    
    #ahora vamos a intentar un join directamente con q a ver que pasa
    new_df_kpi = results.join(df_kpi, on=['id_pro_flu_sol'], how='inner')
    
    ####################################################################
    # ahora unimos os diferentes df con union() unon debajo del otro
    
    new_df = new_df_q.union(new_df_ans) #primera union solo falta KPI
    
    new_df = new_df.union(new_df_kpi)
    
    ##################################################################
    ############333  Vamos a darle el ultimo formatio#################
    
    new_df.registerTempTable("indicadoresmas")
    
    
    tipo1=('Q','QQQQ','QQQQQQQ') #solo necesitamos Q las otras son auxiliares sino  no funciona la query
    tipo2=('ANS','ANSSS','ANSS')
    tipo3=('KPI','KPO','KPL')
    
    indicador=[tipo1,tipo2,tipo3]
    #new_df.fillna(0)
    a=0
    for tipo_indicador in indicador:
        a+=1 #mostrara el indice de cada indicador Q=1,ANS=2, KPI=3
        b=0 #si se quiere que en tipo_reg aparezcan 0 en vez de su indice simplemente en .format(a,tipo_indicador)
            #cambiar a por b 
        query = """SELECT
            {0} as tipo_reg,
            id_indicador as id,
            nombre_indicador as nombre,
            fecha,
            sum(volumen_total_casos) as valor,
            '' as valor_ant,
            '' as medida1,
            '' as medida2,
            '' as medida3,
           id_grupo as aplicacion_proceso,
            '' as alegacion,
            '' as calibracion,
            '' as descripcion
            FROM indicadoresmas
            WHERE tipo_indicador in {1}
            GROUP BY id_grupo,id_indicador, nombre_indicador, tipo_indicador, fecha
            """.format(b,tipo_indicador)
        
        #este query va para los que se tiene que diferenciar por solicitud
        query2 = """SELECT
            {0} as tipo_reg,
            id_indicador as id,
            nombre_indicador as nombre,
            fecha,
            (sum(total_tiempo)/sum(volumen_total_casos)) as valor,
            '' as valor_ant,
            '' as medida1,
            '' as medida2,
            '' as medida3,
            id_grupo as aplicacion_proceso,
            '' as alegacion,
            '' as calibracion,
            '' as descripcion
            FROM indicadoresmas
            WHERE tipo_indicador in {1}
            GROUP BY id_grupo,id_indicador, nombre_indicador,tipo_indicador, fecha
            """.format(b,tipo_indicador)
        query2_aux = """SELECT
            {0} as tipo_reg,
            id_indicador as id,
            sum(volumen_total_casos) as nombre,
            sum(total_tiempo) as tiempo
            FROM indicadoresmas
            WHERE tipo_indicador in {1}
            GROUP BY id_indicador
            """.format(b,tipo_indicador)
            
        query3 = """SELECT
            {0} as tipo_reg,
            id_indicador as id,
            nombre_indicador as nombre,
            fecha,
            (sum(sum_casos_ok)/sum(volumen_total_casos)) as valor,
            '' as valor_ant,
            '' as medida1,
            '' as medida2,
            '' as medida3,
            id_grupo as aplicacion_proceso,
            '' as alegacion,
            '' as calibracion,
            '' as descripcion
            FROM indicadoresmas
            WHERE tipo_indicador in {1}
            GROUP BY id_grupo,id_indicador, nombre_indicador, tipo_indicador,fecha
            """.format(b,tipo_indicador)
        
        query3_aux = """SELECT
            {0} as tipo_reg,
            id_indicador as id,
            sum(volumen_total_casos) as nombre,
            sum(sum_casos_ok) as sum_casos_ok
            FROM indicadoresmas
            WHERE tipo_indicador in {1}
            GROUP BY id_indicador
            """.format(b,tipo_indicador)    
        
        if a==1:
            results_q = sql_context.sql(query)
            
            
        elif a==2:
            results_ans = sql_context.sql(query2)
            results_q=results_q.union(results_ans)
            
            results_2aux=sql_context.sql(query2_aux)
            #results_2aux.show()
        else:
            results_kpi = sql_context.sql(query3)
            results_total=results_q.union(results_kpi)
            results_3aux=sql_context.sql(query3_aux)
            #results_3aux.show()
    
    
    
    datatarget1 = DynamicFrame.fromDF(results_total.repartition(1), glueContext, "datatarget1")
    datasink4 = glueContext.write_dynamic_frame.from_options(frame = datatarget1, connection_type = "s3", connection_options = {"path": "s3://bkt-transformed-data/special01_indicadores_final"}, format = "parquet", transformation_ctx = "datasink4")

else:
    print("Table %s doesn't have new records" %("special01_listatickets"))
    
print('FINISH----------')

job.commit()