import matplotlib.pyplot as plt

# Local result
# f = open("./output/local_tp_final_1_histogram.csv/all.csv", 'r')

# Cluster result
f = open("./output/tp_final_1_histogram.csv/all.csv", 'r')

labels = []
values = []
for l in f:
    l = l[0:-1]
    parts = l.split(",")
    labels.append(parts[0])
    values.append(int(parts[1]))

plt.bar(labels, values)

plt.savefig("./output/1.png")
