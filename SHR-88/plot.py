import pandas as pd

df = pd.read_csv('shrink_vs_short_shipped.csv').dropna()

df[ df['NET_SHRINK_QTY'] < 1000 ] [ df['SS_QTY'] > -1000 ].plot.scatter( x='NET_SHRINK_QTY' , y='SS_QTY' , alpha= 0.5 , figsize = (12,12) )

df[ df['NET_SHRINK_QTY'] < 500 ] [ df['SS_QTY'] > -500 ].plot.scatter( x='NET_SHRINK_QTY' , y='SS_QTY' , alpha= 0.5 , figsize = (12,12) )

df[ df['NET_SHRINK_QTY'] < 500 ] [ df['SS_QTY'] > -500 ].plot.scatter( y='NET_SHRINK_QTY' , x='SS_QTY' , alpha= 0.5 , figsize = (12,12) )

df.plot.scatter( y='NET_SHRINK_QTY' , x='SS_QTY' , alpha= 0.5 , figsize = (12,12) )

df[ df['NET_SHRINK_QTY'] < 500 ] [ df['SS_QTY'] < 500 ] [ df['SS_QTY'] > -500 ].plot.scatter( y='NET_SHRINK_QTY' , x='SS_QTY' , alpha= 0.5 , figsize = (12,12) )


