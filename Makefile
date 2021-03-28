run_local:
	spark-submit $(file)

run_cluster:
	spark-submit --master yarn --deploy-mode client --executor-memory 4g --num-executors 2 --executor-cores 4 $(file)

run_jupyter:
	PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS='notebook' pyspark

scp_file:
	scp -r $(cluster_user)@$(cluster_ip):$(path) ./output/

ssh_tunnel:
	ssh $(cluster_user)@$(cluster_ip) -fNT -L 8088:localhost:8088

logs:
	yarn logs -applicationId $(app_id)

hdfs_ls:
	hdfs dfs -ls /user/$(cluster_user)/$(path)

hdfs_get:
	hdfs dfs -get /user/$(cluster_user)/$(path) ~

concat_parts:
	cd $(path) && cat part* > all.csv && cd ..

q1:
	make file=1.py run_local
	make path=output/local_tp_final_1_histogram.csv concat_parts
	python 1-plot.py