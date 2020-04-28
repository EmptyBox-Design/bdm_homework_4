spark-submit --num-executors 5 --executor-cores 5 --conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH --files hdfs:///tmp/bdm/neighborhoods.geojson,hdfs:///tmp/bdm/boroughs.geojson BDM_HW4.py hdfs:///tmp/bdm/yellow_tripdata_2011-05.csv output_folder