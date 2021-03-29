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

q2:
	make file=2.py run_local
	make path=output/local_tp_final_2_top_100_tracks_with_name_df.csv concat_parts

q4:
	make file=4.py run_local
	make path=output/local_tp_final_4_top_100_heavy_users_df.csv concat_parts

q5:
	rm -rf output/local_tp_final_5_user_play_counts_rdd.csv
	make file=5.py run_local
	make path=output/local_tp_final_5_user_play_counts_rdd.csv concat_parts

q6:
	rm -rf output/local_tp_final_6_plays_by_country_rdd.csv
	make file=6.py run_local
	make path=output/local_tp_final_6_plays_by_country_rdd.csv concat_parts

collab_filtering:
	make file=collab_filtering.py run_local
	make path=output/local_tp_final_collab_filtering_user_recommendations_with_info_df.csv concat_parts

countries_codes_and_coordinates:
	wget -P output https://gist.githubusercontent.com/tadast/8827699/raw/f5cac3d42d16b78348610fc4ec301e9234f82821/countries_codes_and_coordinates.csv