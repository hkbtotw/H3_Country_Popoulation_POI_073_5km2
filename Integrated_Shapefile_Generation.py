from h3 import h3
from Database_Population import *
from datetime import datetime, date, timedelta
from geopandas import GeoDataFrame
from shapely.geometry import Polygon, mapping
import pyproj    #to convert coordinate system
from csv_join_tambon import Reverse_GeoCoding
from Credential import *
import numpy as np
import os
import ast
import pandas as pd
import pickle
import glob
from sys import exit
import warnings
from tqdm import *

warnings.filterwarnings('ignore')

#enable tqdm with pandas, progress_apply
tqdm.pandas()

start_datetime = datetime.now()
print (start_datetime,'execute')
todayStr=date.today().strftime('%Y-%m-%d')
nowStr=datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print("TodayStr's date:", todayStr,' -- ',type(todayStr))
print("nowStr's date:", nowStr,' -- ',type(nowStr))



def ConvertGeometryCoordinate(n):

    latList=[]
    lonList=[]
    #print(' n  : ',n, ' ---- ',type(n))
    n2=ast.literal_eval(str(n))
    #print(list(n2['coordinates'][0]), ' ----- ',type(list(n2['coordinates'][0])) )#,list(zip(*n.exterior.coords.xy)))

    for m in list(n2['coordinates'][0]):
        coor=list(m)
        #print(' --- ',coor)
        latList.append(coor[0])
        lonList.append(coor[1])

    
    #Change coordinate system from  epsg:32647 is  UTM
    UTM47N=pyproj.CRS("EPSG:32647")
    #Change coordinate system to  . epsg:4326 is lat lon
    wgs84=pyproj.CRS("EPSG:4326")

    xx, yy=pyproj.transform(wgs84,UTM47N,  latList, lonList )
    #print(xx, ' ---- ',yy)
    
    polygon_geom = Polygon(zip(xx,yy))
    #print(' ----  > ',polygon_geom)
    return polygon_geom

def GetFileNameList(file_path):
    fileList=glob.glob(file_path+"*.data")
    #print(' ----- ',len(fileList))
    filenameList=[]
    for n in fileList:
        file_dummy=n.split('\\')
        filenameList.append(file_dummy[ len(file_dummy)-1 ])

    del file_dummy, fileList    
    return filenameList

def GetH3hex(lat,lng,h3_level):
    return h3.geo_to_h3(lat, lng, h3_level)

def AssignPopulationToHex(idIn, dfagg):
    dfDummy=dfagg[dfagg['hex_id']==idIn].copy().reset_index(drop=True)
    dfDummy.set_index('hex_id', inplace=True)
    #print(' ==> ',dfDummy.head(10))
    if(len(dfDummy)>1):
        print(' ==> ',dfDummy)
    
    totalsum=dfDummy.sum()
    #print(' sum : ',totalsum['population'], ' ===  ',type(totalsum['population']))
    del dfDummy
    return totalsum['population']

def Write_H3_Grid_Province(df_input, conn1):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
   
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[H3_Grid_Province]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province]  """
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province](	

      [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[geometry]
      ,[p_name_t]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,?,?
  
    )""", 
      row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['population']
      ,row['geometry']
      ,row['p_name_t']
      ,row['DBCreatedAt']
        )
    conn1.commit()

    cursor.close()
    #conn1.close()
    print('------------Complete WriteDB-------------')

def Write_H3_Kepler_Grid_Province(df_input,conn2):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[H3_Grid_Province]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province]  """
    cursor=conn2.cursor()
    cursor.execute(sql)
    conn2.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province](	

      [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[geometry]
      ,[p_name_t]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,?,?
  
    )""", 
      row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['population']
      ,row['geometry']
      ,row['p_name_t']
      ,row['DBCreatedAt']
        )
    conn2.commit()

    cursor.close()

    print('------------Complete WriteDB-------------')

def Write_H3_Grid_Province_PAT(df_input, conn1):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
   
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[H3_Grid_Province]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province_PAT]  """
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province_PAT](	

     [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[population_youth]
      ,[population_elder]
	    ,[population_under_five]
	    ,[population_515_2560]
	    ,[population_men]
	    ,[population_women]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[prov_idn]
      ,[amphoe_idn]
      ,[tambon_idn]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,
    ?,?,?,?,?,
    ?,?,?,?,
    ?,?,?,?,?
  
    )""", 
      row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['population']
      ,row['population_youth']
      ,row['population_elder']
	    ,row['population_under_five']
	    ,row['population_515_2560']
	    ,row['population_men']
	    ,row['population_women']
      ,row['geometry']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['t_name_t']
      ,row['s_region']
      ,row['prov_idn']
      ,row['amphoe_idn']
      ,row['tambon_idn']
      ,row['DBCreatedAt']
        )
    conn1.commit()

    cursor.close()
    #conn1.close()
    print('------------Complete WriteDB-------------')

def Write_H3_Kepler_Grid_Province_2(df_input,conn2):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[H3_Grid_Province]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province_2]  """
    cursor=conn2.cursor()
    cursor.execute(sql)
    conn2.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv8_Province_2](	

      [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[population_youth]
        ,[population_elder]
	    ,[population_under_five]
	    ,[population_515_2560]
	    ,[population_men]
	    ,[population_women]
      ,[geometry]
      ,[p_name_t]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,?,?,?,
    ?,?,?,?,?
  
    )""", 
      row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['population']
      ,row['population_youth']
              ,row['population_elder']
	    ,row['population_under_five']
	    ,row['population_515_2560']
	    ,row['population_men']
	    ,row['population_women']
      ,row['geometry']
      ,row['p_name_t']
      ,row['DBCreatedAt']
        )
    conn2.commit()

    cursor.close()

    print('------------Complete WriteDB-------------')

def Write_H3_Grid_Lv8_Ext_Province_PAT(df_input, conn1):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
   
    #- View all records from the table
    
    #sql="""delete from FROM [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Ext_Province_PAT] """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Ext_Province_PAT] """
    cursor=conn1.cursor()
    cursor.execute(sql)
    conn1.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Ext_Province_PAT](	

     /****** Script for SelectTopNRows command from SSMS  ******/
       [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[population_youth]
      ,[population_elder]
      ,[population_under_five]
      ,[population_515_2560]
      ,[population_men]
      ,[population_women]
      ,[population_general_5]
      ,[population_youth_5]
      ,[population_elder_5]
      ,[population_under_five_5]
      ,[population_515_2560_5]
      ,[population_men_5]
      ,[population_women_5]
      ,[ext_711_073]
      ,[ext_Retail_073]
      ,[ext_Residential_073]
      ,[ext_Restaurant_073]
      ,[ext_Education_073]
      ,[ext_Hotel_073]
      ,[ext_711_5C]
      ,[ext_Retail_5C]
      ,[ext_Residential_5C]
      ,[ext_Restaurant_5C]
      ,[ext_Education_5C]
      ,[ext_Hotel_5C]
      ,[geometry]
      ,[p_name_t]
      ,[a_name_t]
      ,[t_name_t]
      ,[s_region]
      ,[prov_idn]
      ,[amphoe_idn]
      ,[tambon_idn]
      ,[DBCreatedAt]
  
    
	)     
    values(?,?,?,?,?,?,?,?,?,?,
    ?,?,?,?,?,?,?,?,?,?,
    ?,?,?,?,?,?,?,?,?,?,
    ?,?,?,?,?,?,?,?
  
    )""", 
      row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['population']
      ,row['population_youth']
      ,row['population_elder']
      ,row['population_under_five']
      ,row['population_515_2560']
      ,row['population_men']
      ,row['population_women']
      ,row['population_general_5']
      ,row['population_youth_5']
      ,row['population_elder_5']
      ,row['population_under_five_5']
      ,row['population_515_2560_5']
      ,row['population_men_5']
      ,row['population_women_5']
      ,row['ext_711_073']
      ,row['ext_Retail_073']
      ,row['ext_Residential_073']
      ,row['ext_Restaurant_073']
      ,row['ext_Education_073']
      ,row['ext_Hotel_073']
      ,row['ext_711_5C']
      ,row['ext_Retail_5C']
      ,row['ext_Residential_5C']
      ,row['ext_Restaurant_5C']
      ,row['ext_Education_5C']
      ,row['ext_Hotel_5C']
      ,row['geometry']
      ,row['p_name_t']
      ,row['a_name_t']
      ,row['t_name_t']
      ,row['s_region']
      ,row['prov_idn']
      ,row['amphoe_idn']
      ,row['tambon_idn']
      ,row['DBCreatedAt']
        )
    conn1.commit()

    cursor.close()
    #conn1.close()
    print('------------Complete WriteDB-------------')

def Write_H3_Kepler_Grid_Lv9_Province_2(df_input,conn2):
    print('------------- Start WriteDB -------------')
    #df_input=df_input.replace([np.inf,-np.inf,np.nan],-999)
    df_input=df_input.replace({np.nan:None})
    df_write=df_input
    print(' col : ',df_write.columns)


	## ODBC Driver 17 for SQL Server
    # SQL Server
    
    

    #- View all records from the table
    
    #sql="""delete from [TSR_ADHOC].[dbo].[H3_Grid_Province]  """ 
    sql="""select * from [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv9_Province_2]  """
    cursor=conn2.cursor()
    cursor.execute(sql)
    conn2.commit()

    for index, row in df_write.iterrows():
        cursor.execute("""INSERT INTO [TSR_ADHOC].[dbo].[H3_Kepler_Grid_Lv9_Province_2](	

      [hex_id]
      ,[Latitude]
      ,[Longitude]
      ,[population]
      ,[population_youth]
        ,[population_elder]
	    ,[population_under_five]
	    ,[population_515_2560]
	    ,[population_men]
	    ,[population_women]
      ,[geometry]
      ,[p_name_t]
      ,[DBCreatedAt]
    
	)     
    values(?,?,?,?,?,?,?,?,
    ?,?,?,?,?
  
    )""", 
      row['hex_id']
      ,row['Latitude']
      ,row['Longitude']
      ,row['population']
      ,row['population_youth']
              ,row['population_elder']
	    ,row['population_under_five']
	    ,row['population_515_2560']
	    ,row['population_men']
	    ,row['population_women']
      ,row['geometry']
      ,row['p_name_t']
      ,row['DBCreatedAt']
        )
    conn2.commit()

    cursor.close()

    print('------------Complete WriteDB-------------')

def Read_H3_Grid_Lv8_Province_PAT(province):
    #print('------------- Start ReadDB -------------', province)
    #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
    # ODBC Driver 17 for SQL Server
    conn = connect_tad

    cursor = conn.cursor()

    sql="""
            SELECT  [hex_id]
                     ,[Latitude]
                     ,[Longitude]
                     ,[population]
                     ,[population_youth]
                     ,[population_elder]
                     ,[population_under_five]
                     ,[population_515_2560]
                     ,[population_men]
                     ,[population_women]
                     ,[geometry]
                     ,[p_name_t]
                     ,[a_name_t]
                     ,[t_name_t]
                     ,[s_region]
                     ,[prov_idn]
                     ,[amphoe_idn]
                     ,[tambon_idn]
                     ,[DBCreatedAt]
              FROM [TSR_ADHOC].[dbo].[H3_Grid_Lv8_Province_PAT]
              where p_name_t= N'"""+str(province)+"""'
        """

    dfout=pd.read_sql(sql,conn)    
    #print(len(dfout.columns),' :: ',dfout.columns)
    #print(dfout)    
    del conn, cursor, sql
    #print(' --------- Reading End -------------')


    return dfout

##### Get popolation 5km2 area
def GetPopulation_Around_CenterGrid(dfDummy,hex_id):
       hexagons1=[]
       hexagons1.append(hex_id)
       # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
       # 0 is no neighbor
       # 1 is 1 level around center grid and so on
       kRing = h3.k_ring(hexagons1[0], 1)
       hexagons1=list(set(list(hexagons1+list(kRing))))

       dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
       #print(' --- ',dfHex)
       if(len(dfDummy)>0):
           #print(' merge ')
           dfHex=dfHex.merge(dfDummy, how="left", on=["hex_id"])
           #print(' --- hex : ',dfHex,' :: ',dfHex.columns)
           includeList=['hex_id', 'population', 'population_youth', 'population_elder', 'population_under_five', 'population_515_2560', 'population_men', 'population_women']
           dfHex=dfHex[includeList].copy()
           #print(' --- hex : ',dfHex,' :: ',dfHex.columns)
           dfSum=dfHex.sum()
           #print(' --- Sum : ',dfSum,' :: ',dfSum[1], ' ::  ',type(dfSum[2]))
           pop_general=dfSum[1]
           pop_youth=dfSum[2]
           pop_elder=dfSum[3]
           pop_under_five=dfSum[4]
           pop_515_2560=dfSum[5]
           pop_men=dfSum[6]
           pop_women=dfSum[7]
           population=str(pop_general)+'_'+str(pop_youth)+'_'+str(pop_elder)+"_"+str(pop_under_five)+"_"+str(pop_515_2560)+"_"+str(pop_men)+"_"+str(pop_women)
           del dfSum, pop_515_2560, pop_elder, pop_general, pop_men, pop_women, pop_youth, pop_under_five
           
       else:
           #print(' not merge ')
           population=str(0)+'_'+str(0)+'_'+str(0)+"_"+str(0)+"_"+str(0)+"_"+str(0)+"_"+str(0)
       del dfHex, hexagons1
       return population

def Assign_Population_General_CenterGrid(x):
       return float(x.split("_")[0])
def Assign_Population_Youth_CenterGrid(x):
       return float(x.split("_")[1])
def Assign_Population_Elder_CenterGrid(x):
       return float(x.split("_")[2])
def Assign_Population_underFive_CenterGrid(x):
       return float(x.split("_")[3])
def Assign_Population_515_2560_CenterGrid(x):
       return float(x.split("_")[4])
def Assign_Population_Men_CenterGrid(x):
       return float(x.split("_")[5])
def Assign_Population_Women_CenterGrid(x):
       return float(x.split("_")[6])


# Read external complementary data to present on each grid
def Get_Facebook_Population(province, dfHex, columns_name):
    #######################################################################################################
    # Read facebook population from database on sandbox    
    dfIn=Read_Location_Population(province)    

    if(len(dfIn)>0):        
        print('There are population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        # Find hex_id of population location
        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)        
        
        # compute summation of population on each existing grid in dfDummy
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()
     
        # Allocate compute total population to dfHex
        dfHex[columns_name]=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))
        del dfagg       
   
    else:
        print(' No population in =======> ',province)
        
        # Allocate compute total population to dfHex
        dfHex[columns_name]=0
        print(len(dfHex), ' ---- ',dfHex.head(10))
    ##########################################################################################
    return dfHex

def Get_Facebook_Population_Youth_15_24(province, dfHex, columns_name):
    #######################################################################################################
    # Read facebook population from database on sandbox    
    dfIn=Read_Location_Population_Youth_15_24(province)   

    if(len(dfIn)>0):        
        print('There are population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        # Find hex_id of population location
        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)        
        
        # compute summation of population on each existing grid in dfDummy
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()
     
        # Allocate compute total population to dfHex
        dfHex[columns_name]=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))
        del dfagg       
   
    else:
        print(' No population in =======> ',province)
        
        # Allocate compute total population to dfHex
        dfHex[columns_name]=0
        print(len(dfHex), ' ---- ',dfHex.head(10))
    ##########################################################################################
    return dfHex

def Get_Facebook_Population_elderly_60_plus(province, dfHex, columns_name):
    #######################################################################################################
    # Read facebook population from database on sandbox    
    dfIn=Read_Location_Population_elderly_60_plus(province)   

    if(len(dfIn)>0):        
        print('There are elder population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        # Find hex_id of population location
        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)        
        
        # compute summation of population on each existing grid in dfDummy
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()
     
        # Allocate compute total population to dfHex
        dfHex[columns_name]=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))
        del dfagg       
   
    else:
        print(' No elder population in =======> ',province)
        
        # Allocate compute total population to dfHex
        dfHex[columns_name]=0
        print(len(dfHex), ' ---- ',dfHex.head(10))
    ##########################################################################################
    return dfHex

def Get_Facebook_Population_children_under_five(province, dfHex, columns_name):
    #######################################################################################################
    # Read facebook population from database on sandbox    
    dfIn=Read_Location_Population_children_under_five(province)   

    if(len(dfIn)>0):        
        print('There are under five population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        # Find hex_id of population location
        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)        
        
        # compute summation of population on each existing grid in dfDummy
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()
     
        # Allocate compute total population to dfHex
        dfHex[columns_name]=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))
        del dfagg       
   
    else:
        print(' No under five population in =======> ',province)
        
        # Allocate compute total population to dfHex
        dfHex[columns_name]=0
        print(len(dfHex), ' ---- ',dfHex.head(10))
    ##########################################################################################
    return dfHex

def Get_Facebook_Population_men(province, dfHex, columns_name):
    #######################################################################################################
    # Read facebook population from database on sandbox    
    dfIn=Read_Location_Population_men(province)   

    if(len(dfIn)>0):        
        print('There are men population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        # Find hex_id of population location
        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)        
        
        # compute summation of population on each existing grid in dfDummy
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()
     
        # Allocate compute total population to dfHex
        dfHex[columns_name]=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))
        del dfagg       
   
    else:
        print(' No men population in =======> ',province)
        
        # Allocate compute total population to dfHex
        dfHex[columns_name]=0
        print(len(dfHex), ' ---- ',dfHex.head(10))
    ##########################################################################################
    return dfHex

def Get_Facebook_Population_women(province, dfHex, columns_name):
    #######################################################################################################
    # Read facebook population from database on sandbox    
    dfIn=Read_Location_Population_women(province)   

    if(len(dfIn)>0):        
        print('There are women population in ',province)
        # Read Facebook population ,saved in dfIn, selected only lat lng and population columns stored in dfDummy
        #print(dfIn.columns, '===== ',dfIn.head(5))
        dfDummy=dfIn[['Longitude','Latitude','population']].copy()        
        del dfIn
        #print(dfDummy.columns,' ----- ',dfDummy.head(5))

        # Find hex_id of population location
        dfDummy['hex_id']=dfDummy.apply(lambda x:GetH3hex(x['Latitude'],x['Longitude'],h3_level),axis=1)        
        
        # compute summation of population on each existing grid in dfDummy
        dfagg = dfDummy.groupby(by = "hex_id").sum()
        dfagg.drop(columns=['Longitude','Latitude'],inplace=True)
        dfagg=dfagg.reset_index()
     
        # Allocate compute total population to dfHex
        dfHex[columns_name]=dfHex.apply(lambda x: AssignPopulationToHex(x['hex_id'],dfagg),axis=1)
        print(len(dfHex), ' ---- ',dfHex.head(10))
        del dfagg       
   
    else:
        print(' No women population in =======> ',province)
        
        # Allocate compute total population to dfHex
        dfHex[columns_name]=0
        print(len(dfHex), ' ---- ',dfHex.head(10))
    ##########################################################################################
    return dfHex

## Get external data from sandbox
### Read external data from sandbox
def Read_Ext_711_Prv(prv_input):
        #print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                #print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"th_ext_711\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                #print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"th_ext_711\" """

        dfout = pd.read_sql_query(sql, connection)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                #print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_Retail_Shop_Prv(prv_input):
        #print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                #print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"th_ext_retailshop\" where p_name_t = '"""+str(prv_input)+"""' and type_ in ('Convenience store','CP','Family Mart','Lawson 108','Freshmart','108 Shop') """
        else:
                #print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"th_ext_retailshop\" """

        dfout = pd.read_sql_query(sql, connection)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                #print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_Residential_Prv(prv_input):
        #print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                #print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"th_ext_residential\" where p_name_t = '"""+str(prv_input)+"""'  """
        else:
                #print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"th_ext_residential\" """

        dfout = pd.read_sql_query(sql, connection)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                #print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_Restaurant_Prv(prv_input):
        #print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                #print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"th_ext_restaurant\" where p_name_t = '"""+str(prv_input)+"""' and left(goodfors,4) in ('จานด','เดลิ') """
        else:
                #print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"th_ext_restaurant\" where left(goodfors,4) in ('จานด','เดลิ')  """

        dfout = pd.read_sql_query(sql, connection)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                #print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_Education_Prv(prv_input):
        #print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                #print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"th_ext_education\" where p_name_t = '"""+str(prv_input)+"""' and cate in ('มหาวิทยาลัย') """
        else:
                #print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"th_ext_education\" where cate in ('มหาวิทยาลัย')  """

        dfout = pd.read_sql_query(sql, connection)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                #print("PostgreSQL connection is closed")    

        return dfout
def Read_Ext_Hotel_Prv(prv_input):
        #print('------------- Start ReadDB -------------')
        #dfout = pd.DataFrame(columns=['EmployeeId','UserLat','UserLong','DateTimeStamp'])
        # ODBC Driver 17 for SQL Server
        host=machine_1
        database=server_1
        user=username_1
        password=password_1
        connection = psycopg2.connect(host=host, database=database, user=user, password=password)
        cursor_po = connection.cursor()

        sql=""
        if(len(prv_input)>0):
                #print(' Province ------------------------------------------------- ') 
                sql = """SELECT * FROM public.\"th_ext_hotel\" where p_name_t = '"""+str(prv_input)+"""' """
        else:
                #print(' ALL ****************************************************** ') 
                sql = """SELECT * FROM public.\"th_ext_hotel\"   """

        dfout = pd.read_sql_query(sql, connection)

        #print(len(dfout), ' =======================  ',dfout.head(10))

        if connection:
                cursor_po.close()
                connection.close()
                #print("PostgreSQL connection is closed")    

        return dfout

### Get Store on Store grid
def Get711Store_rev2(df711, hex_id,h3_level):             
       if(len(df711)>0):
              df711['hex_id']=df711.apply(lambda x: GetH3hex(x['lat'],x['lng'],h3_level),axis=1)
              #print(province,' 711 ', df711.head(10))
              # compute summation of population on each existing grid in dfDummy
              dfagg = df711.groupby(by = "hex_id").count()
              dfagg=dfagg['code'].copy().reset_index()
              #print(' dfagg : ',dfagg, ' --- ',dfagg.columns)              
              dfDummy=dfagg[dfagg['hex_id']==hex_id].copy()
              if(len(dfDummy)>0):
                     #print(' dfDummy : ',dfDummy, ' --- ',dfDummy.columns)           
                     sum_711_store=dfDummy['code'].values[0]
              else: 
                     sum_711_store=0
              del df711, dfagg, dfDummy
       else:
              sum_711_store=0       
       return sum_711_store
def GetExtRetailShop_rev2(df711, hex_id,h3_level):      
       if(len(df711)>0):
              df711['hex_id']=df711.apply(lambda x: GetH3hex(x['lat'],x['lng'],h3_level),axis=1)
              #print(province,' 711 ', df711.head(10))
              # compute summation of population on each existing grid in dfDummy
              dfagg = df711.groupby(by = "hex_id").count()
              dfagg=dfagg['code'].copy().reset_index()
              #print(' dfagg : ',dfagg, ' --- ',dfagg.columns)              
              dfDummy=dfagg[dfagg['hex_id']==hex_id].copy()
              if(len(dfDummy)>0):
                     #print(' dfDummy : ',dfDummy, ' --- ',dfDummy.columns)           
                     sum_711_store=dfDummy['code'].values[0]
              else: 
                     sum_711_store=0
              del df711, dfagg, dfDummy
       else:
              sum_711_store=0       
       return sum_711_store
def GetExtResidential_rev2(df711, hex_id,h3_level):    
       if(len(df711)>0):
              df711['hex_id']=df711.apply(lambda x: GetH3hex(x['lat'],x['lng'],h3_level),axis=1)
              #print(province,' 711 ', df711.head(10))
              # compute summation of population on each existing grid in dfDummy
              dfagg = df711.groupby(by = "hex_id").count()
              dfagg=dfagg['code'].copy().reset_index()
              #print(' dfagg : ',dfagg, ' --- ',dfagg.columns)              
              dfDummy=dfagg[dfagg['hex_id']==hex_id].copy()
              if(len(dfDummy)>0):
                     #print(' dfDummy : ',dfDummy, ' --- ',dfDummy.columns)           
                     sum_711_store=dfDummy['code'].values[0]
              else: 
                     sum_711_store=0
              del df711, dfagg, dfDummy
       else:
              sum_711_store=0       
       return sum_711_store
def GetExtRestaurant_rev2(df711, hex_id,h3_level):   
       if(len(df711)>0):
              df711['hex_id']=df711.apply(lambda x: GetH3hex(x['lat'],x['lng'],h3_level),axis=1)
              #print(province,' 711 ', df711.head(10))
              # compute summation of population on each existing grid in dfDummy
              dfagg = df711.groupby(by = "hex_id").count()
              dfagg=dfagg['code'].copy().reset_index()
              #print(' dfagg : ',dfagg, ' --- ',dfagg.columns)              
              dfDummy=dfagg[dfagg['hex_id']==hex_id].copy()
              if(len(dfDummy)>0):
                     #print(' dfDummy : ',dfDummy, ' --- ',dfDummy.columns)           
                     sum_711_store=dfDummy['code'].values[0]
              else: 
                     sum_711_store=0
              del df711, dfagg, dfDummy
       else:
              sum_711_store=0       
       return sum_711_store
def GetExtEducation_rev2(df711, hex_id,h3_level):    
       if(len(df711)>0):
              df711['hex_id']=df711.apply(lambda x: GetH3hex(x['lat'],x['lng'],h3_level),axis=1)
              #print(province,' 711 ', df711.head(10))
              # compute summation of population on each existing grid in dfDummy
              dfagg = df711.groupby(by = "hex_id").count()
              dfagg=dfagg['code'].copy().reset_index()
              #print(' dfagg : ',dfagg, ' --- ',dfagg.columns)              
              dfDummy=dfagg[dfagg['hex_id']==hex_id].copy()
              if(len(dfDummy)>0):
                     #print(' dfDummy : ',dfDummy, ' --- ',dfDummy.columns)           
                     sum_711_store=dfDummy['code'].values[0]
              else: 
                     sum_711_store=0
              del df711, dfagg, dfDummy
       else:
              sum_711_store=0       
       return sum_711_store
def GetExtHotel_rev2(df711, hex_id,h3_level):     
       if(len(df711)>0):
              df711['hex_id']=df711.apply(lambda x: GetH3hex(x['lat'],x['lng'],h3_level),axis=1)
              #print(province,' 711 ', df711.head(10))
              # compute summation of population on each existing grid in dfDummy
              dfagg = df711.groupby(by = "hex_id").count()
              dfagg=dfagg['code'].copy().reset_index()
              #print(' dfagg : ',dfagg, ' --- ',dfagg.columns)              
              dfDummy=dfagg[dfagg['hex_id']==hex_id].copy()
              if(len(dfDummy)>0):
                     #print(' dfDummy : ',dfDummy, ' --- ',dfDummy.columns)           
                     sum_711_store=dfDummy['code'].values[0]
              else: 
                     sum_711_store=0
              del df711, dfagg, dfDummy
       else:
              sum_711_store=0       
       return sum_711_store


### Get #Store on 5km3 around store grid
def Get711Store_Around_CenterGrid(dfShop,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex['Store']=dfHex.apply(lambda x:Get711Store_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.sum()
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum[1]
def GetExtRetailShop_Around_CenterGrid(dfShop,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex['Store']=dfHex.apply(lambda x:GetExtRetailShop_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.sum()
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum[1]
def GetExtResidential_Around_CenterGrid(dfShop,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex['Store']=dfHex.apply(lambda x:GetExtResidential_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.sum()
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum[1]
def GetExtRestaurant_Around_CenterGrid(dfShop,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex['Store']=dfHex.apply(lambda x:GetExtRestaurant_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.sum()
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum[1]
def GetExtEducation_Around_CenterGrid(dfShop,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex['Store']=dfHex.apply(lambda x:GetExtEducation_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.sum()
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum[1]
def GetExtHotel_Around_CenterGrid(dfShop,hex_id, h3_level):
    hexagons1=[]
    hexagons1.append(hex_id)
    # k_ring 2nd argument: 1,2,3,....  is the level of neighbor grids around center grid
    # 0 is no neighbor
    # 1 is 1 level around center grid and so on
    kRing = h3.k_ring(hexagons1[0], 1)
    hexagons1=list(set(list(hexagons1+list(kRing))))

    dfHex=pd.DataFrame(hexagons1, columns=['hex_id'])
    #print(' --- ',dfHex)
    dfHex['Store']=dfHex.apply(lambda x:GetExtHotel_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    #print(' 2 --- ',dfHex)
    dfSum=dfHex.sum()
    #print(' Sum --- ',dfSum[1])   
    del dfHex, hexagons1, kRing
    return dfSum[1]



########################################################################################################
######  Input ----  ####################################################################################
# SQL connection for writing data to database
conn = connect_tad

# level 8 covers approx 1 km2
h3_level=8   

# Select if using the specific provinces
# if_all_provinces=1 => Use all provinces in boundary_data
# if_all_provinces=2 => Incase, previous run not complete, Continue running from what being left off from previous run.
if_all_provinces=2

file_path='C:\\Users\\70018928\\Documents\\Project2021\\Uber_h3\\boundary_data\\'
temp_path='C:\\Users\\70018928\\Documents\\Project2021\\Uber_h3\\temp\\'
write_path='C:\\Users\\70018928\\Documents\\Project2021\\Uber_h3\\shapefile\\'
qgis_path='C:\\Users\\70018928\\Documents\\Project2021\\Uber_h3\\qgis_shapefile\\'

#######################################################################################################

###### Start from Scratch or Continue from previous incomplete runs
if(if_all_provinces==1):
    # Read province boundary data
    print(' --- USE ALL PROVINCES IN BOUNDARY DATA --- ')
    filenameList=GetFileNameList(file_path)
    print(' ---> ',filenameList)
    previousCompleteList=[]
elif(if_all_provinces==2):
    print(' --- Continue from Previous incomplete run  --- ')
    allFilenameList=GetFileNameList(file_path)          
    dfContinue=pd.read_csv(temp_path+'continue.csv')
    completeFlg=dfContinue['completeFlg'].head(1).values[0]
    #print(' continue : ',dfContinue, ' --- flag : ',completeFlg)
    if(completeFlg==0):
        previousCompleteList=list(dfContinue['Province'].unique())
        print(' completeList : ',previousCompleteList)
        continueList = np.setdiff1d( allFilenameList, previousCompleteList)   # find elements in allFilenameList not in dfContinue
        print(' continueList : ',continueList)
        pd.DataFrame(continueList, columns=['Province']).to_csv(temp_path+'incomplete.csv') 
        filenameList=continueList
        del allFilenameList, dfContinue
        del completeFlg, continueList
    else:
        print(' **************************************** ')
        print('  ----  Runs   actually  COMPLETE  ----   ')
        print(' **************************************** ')
        exit(0)
else:
    print(' **************************************** ')
    print('  ----  PROBLEM WITH BOUNDARY DATA ----   ')
    print(' **************************************** ')
    exit(0)

completeList=[]+previousCompleteList

#filenameList=['boundary_กระบี่.data','boundary_ปทุมธานี.data']

for file_name in filenameList:  #[:2]:
    ################# format : file_name='boundary_ชลบุรี.data'

    province=file_name.split('_')[1].split('.')[0]
    print(' - ',province)

    with open(file_path+file_name,'rb') as filehandle:
        testlist=pickle.load(filehandle)

    hexList=[]
    for coor in testlist:  
        geoJson = {"coordinates": [coor], "type": "Polygon"}   
            
        hexagons = list(h3.polyfill(geoJson,h3_level))
        print(' ==> ',len(hexagons))
        hexList.append(hexagons)

    #### Distinct hexagons to find the complete set of hexagons  for MultiPolygon
    totalList=[]
    for n in hexList:
        totalList=totalList+n
        #print(len(n),' ==> ',len(totalList))
    totalList=list(set(totalList))
    print(' ==> ',len(totalList))
    hexagons=totalList

    # Create dataframe with one columns from hexagons (total hex_id of the selected province) , named it dfHex
    dfHex=pd.DataFrame(hexagons, columns=['hex_id'])
    #dfHex.head(10)

    print(len(dfHex),' ------  ',dfHex.head(10))

    dfHex=Get_Facebook_Population(province, dfHex, 'population')
    dfHex=Get_Facebook_Population_Youth_15_24(province, dfHex, 'population_youth')
    dfHex=Get_Facebook_Population_elderly_60_plus(province, dfHex, 'population_elder')
    dfHex=Get_Facebook_Population_children_under_five(province, dfHex, 'population_under_five')
    dfHex['population_515_2560']=dfHex['population']-dfHex['population_youth']-dfHex['population_elder']-dfHex['population_under_five']

    dfHex=Get_Facebook_Population_men(province, dfHex, 'population_men')
    dfHex=Get_Facebook_Population_women(province, dfHex, 'population_women')


    ### Count store numbers on store grid
    print(' 2. Store on grid and 5km2 area ')    
    dfShop=Read_Ext_711_Prv(province) 
    dfHex['ext_711_073']=dfHex.progress_apply(lambda x: Get711Store_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    dfHex['ext_711_5C']=dfHex.progress_apply(lambda x: Get711Store_Around_CenterGrid(dfShop,x['hex_id'], h3_level),axis=1)           
    dfShop=Read_Ext_Retail_Shop_Prv(province) 
    dfHex['ext_Retail_073']=dfHex.progress_apply(lambda x: GetExtRetailShop_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    dfHex['ext_Retail_5C']=dfHex.progress_apply(lambda x: GetExtRetailShop_Around_CenterGrid(dfShop,x['hex_id'], h3_level),axis=1)           
    dfShop=Read_Ext_Residential_Prv(province)   
    dfHex['ext_Residential_073']=dfHex.progress_apply(lambda x: GetExtResidential_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    dfHex['ext_Residential_5C']=dfHex.progress_apply(lambda x: GetExtResidential_Around_CenterGrid(dfShop,x['hex_id'], h3_level),axis=1)
    dfShop=Read_Ext_Restaurant_Prv(province)    
    dfHex['ext_Restaurant_073']=dfHex.progress_apply(lambda x: GetExtRestaurant_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    dfHex['ext_Restaurant_5C']=dfHex.progress_apply(lambda x: GetExtRestaurant_Around_CenterGrid(dfShop,x['hex_id'], h3_level),axis=1)
    dfShop=Read_Ext_Education_Prv(province)   
    dfHex['ext_Education_073']=dfHex.progress_apply(lambda x: GetExtEducation_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    dfHex['ext_Education_5C']=dfHex.progress_apply(lambda x: GetExtEducation_Around_CenterGrid(dfShop,x['hex_id'], h3_level),axis=1)
    dfShop=Read_Ext_Hotel_Prv(province)  
    dfHex['ext_Hotel_073']=dfHex.progress_apply(lambda x: GetExtHotel_rev2(dfShop, x['hex_id'],h3_level),axis=1)
    dfHex['ext_Hotel_5C']=dfHex.progress_apply(lambda x: GetExtHotel_Around_CenterGrid(dfShop,x['hex_id'], h3_level),axis=1)
    #print(' dfHex  : ',dfHex)    

    print(' 3. Population on 5km2 area ')
    dfShop=Read_H3_Grid_Lv8_Province_PAT(province)     
    dfHex['Population_C']=dfHex.progress_apply(lambda x:GetPopulation_Around_CenterGrid(dfShop,x['hex_id']),axis=1)
    dfHex['population_general_5']=dfHex.progress_apply(lambda x: Assign_Population_General_CenterGrid(x['Population_C']),axis=1)
    dfHex['population_youth_5']=dfHex.progress_apply(lambda x: Assign_Population_Youth_CenterGrid(x['Population_C']),axis=1)
    dfHex['population_elder_5']=dfHex.progress_apply(lambda x: Assign_Population_Elder_CenterGrid(x['Population_C']),axis=1)
    dfHex['population_under_five_5']=dfHex.progress_apply(lambda x: Assign_Population_underFive_CenterGrid(x['Population_C']),axis=1)
    dfHex['population_515_2560_5']=dfHex.progress_apply(lambda x: Assign_Population_515_2560_CenterGrid(x['Population_C']),axis=1)
    dfHex['population_men_5']=dfHex.progress_apply(lambda x: Assign_Population_Men_CenterGrid(x['Population_C']),axis=1)
    dfHex['population_women_5']=dfHex.progress_apply(lambda x: Assign_Population_Women_CenterGrid(x['Population_C']),axis=1)
    dfHex.drop(columns=['Population_C'], inplace=True)

    # Save dfHex with 2 ++ columns, hex_id and population and ++ in dfDummy
    dfDummy=dfHex.copy().reset_index(drop=True)

    # Find center point of each hex_id
    dfDummy['lat'] = dfDummy['hex_id'].apply(lambda x: h3.h3_to_geo(x)[0])
    dfDummy['lng'] = dfDummy['hex_id'].apply(lambda x: h3.h3_to_geo(x)[1])


    # Cretae geometry columns (this is in 4326 to use with Kepler.gl)
    dfDummy["geometry"] =  dfDummy.hex_id.apply(lambda x: 
                                                        {    "type" : "Polygon",
                                                                "coordinates": 
                                                                [h3.h3_to_geo_boundary(x)]                                                                
                                                            })

    ## csv file as an input of the ConvertCSV_To_Shapefile_rev2 on local machine
    print(' ===>  Kepler data out')
    dfDummy.rename(columns={'lat':'Latitude','lng':'Longitude'}, inplace=True)
    dfDummy.to_csv(write_path+'\\test_'+province+'_shapefile.csv')  
    dfDummy['p_name_t']=province
    dfDummy['DBCreatedAt']=nowStr 
    dfDummy_2=dfDummy.copy()
    dfDummy_2['geometry']=dfDummy_2['geometry'].astype(str)
    #Write_H3_Kepler_Grid_Lv9_Province_2(dfDummy_2,conn)


    # Convert information in geometry columns to 32647 coordinates to use with QGIS
    print(' ===>  Convert Coordinates 4236 -> 32647 ')
    dfDummy['geom_2']=dfDummy.apply(lambda x: ConvertGeometryCoordinate(x['geometry']),axis=1 )
    dfDummy.drop(columns=['geometry'],inplace=True)
    dfDummy.rename(columns={'geom_2':'geometry'},inplace=True)
    dfDummy['geometry']=dfDummy['geometry'].astype(str)
    

    # Find District and Subdistrict location of each grid
    print(' ===>  Reverse Geocoding')
    dfPAT=Reverse_GeoCoding(dfDummy[['hex_id', 'Latitude', 'Longitude']])
    includeList=['hex_id', 'Latitude', 'Longitude','p_name_t',
       'a_name_t',  't_name_t', 's_region', 'prov_idn', 'amphoe_idn', 'tambon_idn']       
    dfPAT=dfPAT[includeList]    

    mainDf=pd.merge(dfDummy, dfPAT, how="left", on=["hex_id"])
    mainDf.rename(columns={'Latitude_x':'Latitude','Longitude_x':'Longitude','p_name_t_x':'p_name_t'},inplace=True)
    print(mainDf.columns,' ===> ',mainDf, ' ----  ',mainDf.dtypes)    

    # Write data to files and databse
    print(' ===>  QGIS data out ')
    mainDf.to_csv(qgis_path+'test_'+province+'_shapefile_32647_PAT.csv')       
    Write_H3_Grid_Lv8_Ext_Province_PAT(mainDf,conn) 

    # write temp file to check if the run is complete
    completeList.append(file_name)
    dfComplete=pd.DataFrame(completeList, columns=['Province'])
    if(len(completeList)==len(filenameList)):
        dfComplete['completeFlg']=1
    else:
        dfComplete['completeFlg']=0
    dfComplete.to_csv(temp_path+'continue.csv')
    del dfComplete




conn.close()
del dfDummy, dfHex, dfDummy_2, mainDf, dfPAT, dfShop
del includeList, hexagons, totalList, testlist, hexList, filenameList, previousCompleteList
###****************************************************************
end_datetime = datetime.now()
print ('---Start---',start_datetime)
print('---complete---',end_datetime)
DIFFTIME = end_datetime - start_datetime 
DIFFTIMEMIN = DIFFTIME.total_seconds()
print('Time_use : ',round(DIFFTIMEMIN,2), ' Seconds')