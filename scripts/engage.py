import pandas as pd
from sklearn.decomposition import PCA
import seaborn as sns
import math
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
class Engagement:
    def __init__(self,data):
        self.data=data
    def engage_data(self)-> pd.DataFrame:
        dr=self.data[['Bearer Id','MSISDN/Number','Dur. (ms)','Total UL (Bytes)', 'Total DL (Bytes)']]
        dr['Total Data']=self.data['Total UL (Bytes)']+self.data['Total DL (Bytes)']
        dr['Dur. (s)']=dr['Dur. (ms)']/1000
        dr.drop('Dur. (ms)',axis=1,inplace=True)
        group=dr.groupby('MSISDN/Number').sum()
        group=group.sort_values('Dur. (s)',ascending=False)
        return group
def cluster_analysis( cols: list, data: pd.DataFrame, n_clusters: int):
        '''
        A function used for performing KMeans clustering and visualizing the result

        Parameter:
            cols(list): The columns that you need to perfom KMeans on
            data(pd.DataFrame): The data to perform KMeans
            n_cluster(int): The number of clusters for KMeans
        
        Returns:
            data, cluster_stats(tuple): Returns the data containing clusters and the stats for clusters dataframe         
        '''

        metrics = data[cols]
        scaler = StandardScaler()
        normalized_metrics = scaler.fit_transform(metrics)

        kmeans = KMeans(n_clusters= n_clusters, random_state=42)
        clusters = kmeans.fit_predict(normalized_metrics)

        
        data['Cluster'] = clusters       
        stats = []

        for col in cols:
            result = data.groupby('Cluster').agg({
            col: ['min', 'max', 'mean', 'sum'],
            })

            stats.append(result)

        cluster_stats = pd.concat(stats, axis =1).reset_index()
        ncols = 3
        nrows = math.ceil(len(cols) / ncols)
        
        # Create the subplots with the correct number of rows and columns
        fig, axes = plt.subplots(ncols=ncols, nrows=nrows, figsize=(15, 5 * nrows))
        axes = axes.flatten()
        
        for i, item in enumerate(cols):
            sns.barplot(x='Cluster', y=cluster_stats[item]['mean'], data=cluster_stats, ax=axes[i])
            axes[i].set_title(f'Average {item} per Cluster') 

        for j in range(i+1, len(axes)):
            fig.delaxes(axes[j])  
        return data, cluster_stats