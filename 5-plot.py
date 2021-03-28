import choropleth
import pandas as pd
import matplotlib.pyplot as plt

# Local result
df = pd.read_csv(
    "./output/local_tp_final_5_plays_by_country_rdd.csv/all.csv", skipinitialspace=True)

# Cluster result
# df = pd.read_csv("./output/tp_final_5_plays_by_country_rdd.csv/all.csv", skipinitialspace=True)

choropleth.plot_choropleth_map(df.values.tolist())

plt.savefig("./output/5.png")
