import choropleth
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("./5.csv", skipinitialspace=True)

choropleth.plot_choropleth_map(df.values.tolist())

plt.savefig("./output/5.png")
