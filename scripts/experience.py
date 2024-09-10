def exper(data):
    cols = ['MSISDN/Number','Avg RTT DL (ms)',
       'Avg RTT UL (ms)', 'Avg Bearer TP DL (kbps)', 'Avg Bearer TP UL (kbps)',
       'TCP DL Retrans. Vol (Bytes)', 'TCP UL Retrans. Vol (Bytes)']
    df=data[cols]
    return df.groupby('MSISDN/Number').sum()
