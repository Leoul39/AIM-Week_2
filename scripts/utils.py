import pandas as pd
import os
import math
from sqlalchemy import create_engine
from dotenv import load_dotenv
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
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
        dp=dp.rename(columns={identifier+'_y':identifier})
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
      for i in d:
        print(f'The top 5 handsets for {i} are:')
        dr=self.data.loc[self.data['Handset Manufacturer']==i]
        print(dr['Handset Type'].value_counts().head(3))
def plot_null(data):
    """
    This function sums up the null values found in each columns and plots their amount in 
    a bar chart
    """
    plt.figure(figsize=(10, 6))  # Adjust the figure size as needed
    data.isnull().sum().plot(kind='bar')
    plt.xlabel('Columns')
    plt.ylabel('Count of Null Values')
    plt.title('Count of Null Values in Each Column')
    plt.show()
def plot_corr(data,lis:list):
    """
    This function uses seaborn's heatmap function to plot the correlation matrix of the application's data 
    usage or any column names provided as list to the list parameter 

    """
    l=[]
    for i in lis:
        for j in data.columns:
            if i in j:
                l.append(j)
    corr=data[l].corr()
    return sns.heatmap(corr,annot=True,cbar=False)
def decile_class(data,decile,result):
    """
    This function reads the data and makes a decile class from the data using the decile parameter column and 
    computes the result per decile class using the column provided by the result parameter. The function also
    returns the top 5 classes
    """
    data['Decile_Class'] = pd.qcut(data[decile], q=5, labels=False)

    # Compute total data (DL+UL) per decile class
    per_decile = pd.DataFrame(data.groupby('Decile_Class')[result].sum())
    per_decile['Total Volume (in GB)']=per_decile[result]/10**3
    return per_decile
def pca(data):
    pca=PCA(n_components=2)
    d=pca.fit_transform(data)
    return plt.scatter(d[:,0],d[:,1],c=data['Duration (s)'],cmap='coolwarm')

 



        
        

          
    
    
       