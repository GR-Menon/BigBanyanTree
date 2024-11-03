#!/bin/bash

if [ ! $# -ge 2 ]; then
    echo "Usage: <input_file> script1.py script2.py ..."
    exit 1
fi

# note that the input must have a newline at the end
input_file=$1

check_file_exists() {
    if [ ! -f "$1" ]; then
        echo "File $1 doesn't exist!"
        exit 1
    fi
}

# gets cli args starting from the 2nd arg (0 indexed)
py_files=(${@:2})

# iterates over each line in input_file having warc file paths
while IFS= read -r warc_file; do

    echo "------------------------------------------------------"
    echo "Processing WARC file: $warc_file..."
    echo "------------------------------------------------------"
    
    pid_arr=()

    start_time=$(date +%s)
    
    # iterates over each python file and submits them to spark
    for py_file in ${py_files[*]}; do
        check_file_exists $py_file
        echo ">>> Submitting $py_file to spark with args $warc_file..."
        
        spark-submit $py_file --warc_file $warc_file --output_dir "iphost_out" &
        
        # spark-submit $py_file --warc_file $warc_file --output_dir "script_ext_out" &
        
        pid=$(pgrep -f "spark-submit $py_file")
        echo "PID: $pid"
        pid_arr+=($pid)
    done
    
    # waits for all the jobs to complete
    wait ${pids[@]}

    end_time=$(date +%s)
    elapsed_time=$((end_time-start_time))
    
    echo "$warc_file: $elapsed_time seconds" >> times.txt
    echo $warc_file >> done.txt
    
    echo "******************************************************"
    echo "WARC file $warc_file has finished processing"
    echo "******************************************************"

done < $input_file

echo "#######################################################"
echo "Processing complete. Exiting..."
echo "#######################################################"
