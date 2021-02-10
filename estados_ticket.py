# -*- coding: utf-8 -*-
"""
Created on Wed Feb 10 10:12:18 2021

@author: jtintore
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import pandas as pd
from datetime import datetime, timedelta
import numpy as np

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

datasource_historico = glueContext.create_dynamic_frame.from_catalog(database = "input_db", table_name = "lnd_historico_ticket", transformation_ctx = "datasource_historico")
datasource_festivo = glueContext.create_dynamic_frame.from_catalog(database = "input_db", table_name = "special01_festivos")# transformation_ctx = "datasource_festivo")


df=datasource_historico.toDF()
###################################################################################3
##### para lugo calcuar los dias festivos entre fechas #############################
df_festivo=datasource_festivo.toDF()
df_1= df_festivo.select("*").toPandas()
## sacamos los dias  feestivos del csv y los guardamos en un ARRAY, NO en lista.

festivos=[]
for i in df_1.loc[:,'date']:
    fecha=datetime.strptime(i,'%d/%m/%Y')
    fecha_2=fecha.replace(year=2020)
    festivos.append(fecha_2)

#convertimos en ARRAy y a formato 'datetime64[D]', ya que nos lo pide la funcion busy_count
festivos_array=np.array(festivos,'datetime64[D]')

def festivo(begin, end):
    # en el caso de que begin y end esten en otro formato habra que cambiarlo 
    # en estadocasos se utiizaron estos formatos: '%d/%m/%Y %H:%M:%S +00:00' y '%m/%d/%Y %I:%M:%S %p +00:00'
    
    inicio=np.array(begin,'datetime64[D]')
    final=np.array((end + timedelta(days=1)) ,'datetime64[D]') # le sumamos uno dia ya que la end date no la inlcuye en el recuento
    dias_laborables= np.busday_count(inicio,final, holidays=festivos_array)

    dias_enteros=(end-begin + timedelta(days=1)).days
    dias_fest=dias_enteros-dias_laborables
    if dias_fest>= 0:
        result= dias_fest
    else:
        result= 0
    return result
######################################################################################
# Convert the Spark DataFrame back to a Pandas DataFrame using Arrow


if df.count()!=0:
    print("New records from table %s: %s" %("lnd_historico_ticket",df.count()))
    result_pdf = df.select("*").toPandas()
    
    #print(df.count())
    #print(df.show(10))
    #print (result_pdf)
    
    valores_estado=[] #hemos guardado los datos de ESTADO
    for i in result_pdf.iloc[:,1]:
        valores_estado.append(i)
        #print(valores_estado)
    valores_fecha=[]
    fechas_resta=[]
    for i in result_pdf.iloc[:,2]:
        valores_fecha.append(i)
        fechas_resta.append(i)
        #print(valores_fecha)
    print (len(valores_fecha))
    #vamos a eliminar el primer valor de estado y de fecha ya que no es un dato
    valores_estado.pop(0)
    valores_fecha.pop(0)
    
    ####ahora cada vez que ponga crear tiquet que lo cambie por Nan
    
    estado_final=[]
    fecha_final=[]
    for i in range(len(valores_estado)):
        if valores_estado[i] == 'Crear ticket' or valores_estado[i]=='Crear Ticket' :
            #valores_final.append(valores_estado[i])
            estado_final.append('Nan')
            fecha_final.append('Nan')
        else:
            estado_final.append(valores_estado[i])
            fecha_final.append(valores_fecha[i])
    print (len(estado_final))
    estado_final.append('Nan') 
    fecha_final.append('Nan')
    result_pdf.insert(4,'Estadofinal', estado_final)
    result_pdf.insert(5,'FechaFinal', fecha_final)
    
    #print (result_pdf)
    
    
    #prueba10=prueba2-prueba6
    #print(prueba10)
    '''convertimos las fechas a datos para operar con ellas'''
    data_fecha=[] #aqui iran los datos de la fecha de entrada
    #fechas_resta.pop(0)
    data_fecha_fiesta=[]
    i_modificados=[]
    
    for i in fechas_resta:
        try:
            fecha=datetime.strptime(i,'%d/%m/%Y %H:%M:%S +00:00' )
            data_fecha.append(fecha)
        except:
            fecha_2=datetime.strptime(i,'%m/%d/%Y %I:%M:%S %p +00:00' )
            data_fecha.append(fecha_2)
    
    
    
    for i in data_fecha:
        resultado=i.replace(hour=00,minute=00,second=00)
        data_fecha_fiesta.append(resultado)
    
    
    
    
    #####
    data_fecha_final=[]#aqui iran los datos de fecha de salida
    #fecha_final.pop(0)
    data_fecha_final_fiesta=[]
    
    for i in fecha_final:
        try:
            fecha_3=datetime.strptime(i,'%d/%m/%Y %H:%M:%S +00:00' )
            data_fecha_final.append(fecha_3)
            
        except:
            try:
                fecha_4=datetime.strptime(i,'%m/%d/%Y %I:%M:%S %p +00:00' )
                data_fecha_final.append(fecha_4)
            except:
                data_fecha_final.append('Nan')
    
    for i in data_fecha_final:
        try:
            resultado=i.replace(hour=00,minute=00,second=00)
            data_fecha_final_fiesta.append(resultado)
        except:
            data_fecha_final_fiesta.append('Nan')
    
    '''hay que restar ahora las fechas'''
    fechas_ultimas=[]#aqui iran las restas
    fechas_auxiliar=[]#fechas auxiliares sin formateo en string
    for i in range(len(data_fecha)):
        try:
            operacion= data_fecha_final[i]-data_fecha[i]
            #operacion3=operacion*0.66
            operacion2=str(operacion.total_seconds())
            fechas_auxiliar.append(operacion.total_seconds())
            fechas_ultimas.append(operacion2)
        except:
            fechas_ultimas.append('Nan')
            fechas_auxiliar.append(0.0)
    
    dias_festivos=[]
    
    
    for i in range(len(data_fecha_final)):
        try:
            valor=festivo(data_fecha_fiesta[i],data_fecha_final_fiesta[i])
            #valor2=valor*0.33
            dias_festivos.append(float(valor))
        except:
            dias_festivos.append(float(0))
        
    
    #print(len(fechas_auxiliar)) 
    #rint (len(dias_festivos))
    #fechas_ultimas.pop(-1)
    #fechas_ultimas.insert(0,'Tiempo')
    #fechas_ultimas.append('Nan')
    #fechas_auxiliar.append('Nan')
    result_pdf.insert(6,'Diferencia_tiempo', fechas_auxiliar)
    
    
    result_pdf.insert(7,'dias_festivos', dias_festivos)
    
    
    #eliminamo la columna date que contiene la fecha de la particion
    result_pdf.rename(columns={'date':'date_particion'},inplace=True)
    #del result_pdf['date']
    #ahora vamos a crear otra columna date con a ultima fecha de finalizacion en este formato 20201230 (30/12/2020)
    #creamos lista vacia 
    date_datos=[]
    for i in data_fecha_final:
        try:
            valor=datetime.strftime(i, '%Y%m%d')
            date_datos.append(valor)
        except:
            date_datos.append(str(0))
            
    result_pdf['date']= date_datos
    
    copia_df=result_pdf.copy(deep=True)
    copia_df = copia_df.drop_duplicates()
    copia_df_2= copia_df.groupby(['id_ticket','date_particion']).agg({'date':['max']}).reset_index()
    copia_df_2.columns=['id_ticket','date_particion','date']
    
   
    
    for i in result_pdf.iloc[:,1]:
        a=1#constante auxiliar no sirve para nada
        if i =='En Validación' or i=='En gestión' or i=='En validación' or i=='En Gestión' or i=='Validación':
            a=a+1 #solo queremos que nos lleven al else
        else:
            result_pdf=result_pdf.drop(result_pdf[result_pdf['estado']==i].index)
    
    print(result_pdf.columns)
    ############################################## 
    ########## esta sentencia quizas habria que eliminarla o comentarla para que no influya en el codigo
    #result_pdf = result_pdf.drop_duplicates(['id_ticket','estado','fecha','Estadofinal','FechaFinal','Diferencia_tiempo']) # esta sentencia no acaba de fundionar vamos a pasarlo a spark y luego a pandas otra vez
    ##############################################33
    
    
    #spark_1=spark.createDataFrame(result_pdf)
    #spark_duplicados = spark_1.distinct()
    #result_pdf= spark_duplicados.select("*").toPandas()
    
     
    #creamos lista vacia donde almacenaremos los id_ticket
    ##########################################################################################
    ############ Aqui vamos a hacer una segunda tabla que la contendremos en otro bucket #####
    
    otro_df=result_pdf.copy(deep=True)
    
    otro_df['diferencia_tiempo_dias']= (otro_df['Diferencia_tiempo']/86400)
    
    otro_df['diferencia_tiempo_festivos']= (otro_df['Diferencia_tiempo']/86400) - otro_df['dias_festivos']
    
    #ahora los sacamos y los convertimos en strings
    otra_dias_festivos=[]
    for i in otro_df.loc[:,'dias_festivos']:
        otra_dias_festivos.append(str(i))
    
    del otro_df['dias_festivos']
    otro_df['dias_festivos'] = otra_dias_festivos
    
    
    otra_tiempo_dias=[]
    for i in otro_df.loc[:,'diferencia_tiempo_dias']:
        otra_tiempo_dias.append("{0:.2f}".format(i))
    
    del otro_df['diferencia_tiempo_dias']
    otro_df['diferencia_tiempo_dias'] = otra_tiempo_dias
    
    otra_festivos=[]
    for i in otro_df.loc[:,'diferencia_tiempo_festivos']:
        if i > 0.0:
            otra_festivos.append("{0:.2f}".format(i))
        else:
            otra_festivos.append("{0:.2f}".format(0.0))
    
    del otro_df['diferencia_tiempo_festivos']
    otro_df['diferencia_tiempo_festivos'] = otra_festivos
    
    del otro_df['Diferencia_tiempo']
    
    
    ##########################################################################################, 'date':['max']
    ##########################################################################################
    result_pdf4= result_pdf.groupby(['id_ticket','date_particion']).agg({'Diferencia_tiempo':['sum'],'dias_festivos':['sum']}).reset_index()
    result_pdf4.columns=['id_ticket','date_particion','Diferencia_tiempo','dias_festivos']
    
    result_pdf4=pd.merge(result_pdf4, copia_df_2, on=['id_ticket','date_particion'])
    result_pdf40=result_pdf4.sort_values('date_particion')
    result_pdf3 = result_pdf40.drop_duplicates(['id_ticket'], keep='first')
    
    del result_pdf3['date_particion']
    
    result_pdf2= result_pdf3.groupby(['id_ticket','date']).agg({'Diferencia_tiempo':['sum'],'dias_festivos':['sum']}).reset_index()
    result_pdf2.columns=['id_ticket','date','Diferencia_tiempo','dias_festivos']
    
    
    result_pdf2= result_pdf2.sort_values('id_ticket')
    print(result_pdf2)
    #result_pdf2['date']=super_date
    
    #creamos listas vacias donde van a ir los tiempos en dias y la de KO o OK
    lista_tiempos=[] # solo para comprobar el formato despues
    lista_dias=[] #aqui van la diferencia de tiempo en dias que es el numero / 86400 
    okko=[] #lista que vamos a utilizar para ir comprobando y guardando lo OK o KO
    
    #sacamos los valores del data frame y los metemos en lita_dias con el formato ya dado, dos decimales
    for i in result_pdf2.loc[:,'Diferencia_tiempo']: 
        lista_tiempos.append(i)
        lista_dias.append(str(i/86400))
    
    result_pdf2['diferencia_tiempo_festivos']= (result_pdf2['Diferencia_tiempo']/86400) - result_pdf2['dias_festivos']
    
    otra_festivos_2=[]
    for i in result_pdf2.loc[:,'diferencia_tiempo_festivos']:
        if i > 0.0:
            otra_festivos_2.append((i))
        else:
            otra_festivos_2.append((0.0))
    
    del result_pdf2['diferencia_tiempo_festivos']
    result_pdf2['diferencia_tiempo_festivos'] = otra_festivos_2
    
    dif_t_festivos=[]
    dias_fest=[]
    
    #comprobamos la lista_dias, si es mayor que 5.00 entonces sera un K.O.// si es menor que 5.00 sera un O.K. 
    for i in result_pdf2.loc[:,'diferencia_tiempo_festivos']:# lista_dias:
        
        dif_t_festivos.append(str(i))
        if i >= 5.00:
            okko.append('KO')
        else:
            okko.append('OK')
    
    
    del result_pdf2['diferencia_tiempo_festivos']
    result_pdf2['diferencia_tiempo_festivos'] = dif_t_festivos
    
    
    result_pdf2['Diferencia_tiempo_dias']= lista_dias
    
    result_pdf2['OK_KO']= okko
    print (result_pdf2.columns)
    result_pdf2.columns=['id_ticket','date','Diferencia_tiempo','dias_festivos','diferencia_tiempo_festivos','Diferencia_tiempo_dias','ok_ko']
    print (result_pdf2.columns)
    
    ######################################################################################3
    result_pdf2['dias_festivos']=result_pdf2['dias_festivos'].astype(str)
    
    #######################################################################################
    
    df_2=spark.createDataFrame(result_pdf2)
    otro_df_spark=spark.createDataFrame(otro_df)
    df_2.show()
    
    datatarget2=DynamicFrame.fromDF(otro_df_spark.repartition(1), glueContext, "datatarget2")
    datatarget1 = DynamicFrame.fromDF(df_2.repartition(1), glueContext, "datatarget1")
    datasink4 = glueContext.write_dynamic_frame.from_options(frame = datatarget2, connection_type = "s3", connection_options = {"path": "s3://bkt-transformed-data/special01_estadosticket_auxiliar"}, format = "parquet", transformation_ctx = "datasink4")
    datasink4 = glueContext.write_dynamic_frame.from_options(frame = datatarget1, connection_type = "s3", connection_options = {"path": "s3://bkt-transformed-data/special01_estadosticket"}, format = "parquet", transformation_ctx = "datasink4")

else:
        
    print("Table %s doesn't have new records" %("lnd_historico_ticket"))

print("Finished-----------")


job.commit()