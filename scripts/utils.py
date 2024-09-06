import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import seaborn as sns

load_dotenv()

class DataLoader:
  def __init__(self):
      DB_HOST=os.getenv('DB_HOST')
      DB_PORT=os.getenv('DB_PORT')
      DB_NAME=os.getenv('DB_NAME')
      DB_USER=os.getenv('DB_USER')
      DB_PASSWORD=os.getenv('DB_PASSWORD')

      self.conn= create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
  def load_data_using_sqlalchemy(self) -> pd.DataFrame:
      """
      the data is loaded directly from the postgres database to our environment
      based on the given query using SQLAlchemy. This library is also preferred because
      Pandas often throws you a user warning while using psycopg2.
      """
      query='select * from public.xdr_data'
      with self.conn.connect() as connection:
              df=pd.read_sql_query(query,connection)
            
      return df
class DataOverview:
    def __init__(self,data):
        self.data=data
    def bytes_to_megabytes(self):
        """
        This method changes any column in the initial data that has bytes in it to megabytes to
        make an easier analysis.
        """
        for i in self.data.columns:
            if 'Bytes' in i:
                self.data[i]=self.data[i]/10**6
                self.data=self.data.rename(columns={i:i.split('(')[0]+"(Megabytes)"})
        return self.data
    def overview_data(self,apps,identifier):
        """
        This method collects multiple columns(session duration, application data) and provides the overview data 
        grouped by IMSI(SIM card) and sorted using the count of the identifier.
        """
        dr=pd.DataFrame()
        dr['Duration (s)']=self.data['Dur. (ms)']/1000
        dr[identifier]=self.data[identifier]
        for app in apps:
            dr[app+' (Total in MB)']=self.data[app+" DL (Bytes)" ]/10**6+self.data[app+' UL (Bytes)']/10**6
        dr['Total UL (in MB)'], dr['Total DL (in MB)']=self.data['Total UL (Bytes)']/10**6, self.data['Total DL (Bytes)']/10**6
        dr['Total Volume (in MB)']=dr['Total UL (in MB)']+dr['Total DL (in MB)']
        dr['IMSI']=self.data['IMSI']
        dd=dr.groupby(by='IMSI').sum()
        dc=dr.groupby(by='IMSI')[identifier].count()
        dp=pd.merge(dd,dc,on='IMSI')
        dp=dp.drop(identifier+'_x',axis=1)
        dp=dp.sort_values(identifier+'_y',ascending=False)
        return dp
class Subtask:
    def __init__(self,data):
      self.data=data
    def subtasks(self):
      """
      computes the main EDA done on the handset and handset manufacturers.
      """
      print('The top 10 handsets used by the customers are:')
      o=self.data['Handset Type'].value_counts().head(11)
      o.drop('undefined',inplace=True)
      print(o)
      print('The top 3 handset manufacturers are:')
      print(self.data['Handset Manufacturer'].value_counts().head(3))
      d=list(self.data['Handset Manufacturer'].value_counts().head(3).index)
      d=list(self.data['Handset Manufacturer'].value_counts().head(3).index)
      for i in d:
        print(f'The top 5 handsets for {i} are:')
        dr=self.data.loc[self.data['Handset Manufacturer']==i]
        print(dr['Handset Type'].value_counts().head(3))
def plot_corr(data,lis:list):
        l=[]
        for i in lis:
            for j in data.columns:
                if i in j:
                    l.append(j)
        corr=data[l].corr()
        return sns.heatmap(corr,annot=True,cbar=False)

        

          
    
    
       