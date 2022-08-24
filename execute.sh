spark-submit \
--master 'local[*]' \
--py-files packages.zip \
--files configs/etl_config.json \
jobs/etl_job.py --current_file_path ../location_may.csv --previous_file_path ../location_apr.csv  --output_dir ./output
