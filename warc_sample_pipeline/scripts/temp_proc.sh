#!/bin/bash

pid_arr=()
start_time=$(date +%s)

spark-submit ipwarc_mmdb_pdudf.py --input_file input.txt --output_dir "iphost_out" &
pid1=$(pgrep -f "spark-submit ipwarc_mmdb_pdudf.py")
pid_arr+=($pid1)

# spark-submit warc_script_ext.py --input_file input.txt --output_dir "script_opt_out" &
# pid2=$(pgrep -f "spark-submit warc_script_ext.py")
# pid_arr+=($pid2)

wait ${pid_arr[@]}

end_time=$(date +%s)
elapsed_time=$((end_time-start_time))

echo "ELAPSED TIME: $elapsed_time seconds"
