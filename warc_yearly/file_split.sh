
#!/bin/bash

# usage: file_split.sh <input_file> <output_dir> <num_warcs_to_proc>

# Input file containing WARC file paths
input_file=$1

# Output directory for split files
output_dir=$2
mkdir -p "$output_dir"

# Number of warc files to process in the input_file
num_warcs=$3

# Check if the input file exists
if [ ! -f "$input_file" ]; then
  echo "Input file not found!"
  exit 1
fi

# Shuffle the lines in the input file
shuffled_file=$(mktemp)
shuf "$input_file" > "$shuffled_file"

# Number of lines to sample per file
lines_per_file=10

# Total number of output files
total_files=$(($num_warcs / $lines_per_file))

# Create 100 files with random samples of 10 lines each
counter=1
for i in $(seq 1 $total_files); do
  output_file="$output_dir/warc_part_$(printf "%03d" "$counter").txt"
  head -n $lines_per_file "$shuffled_file" > "$output_file"
  # Remove the selected lines from the shuffled file to avoid duplication
  sed -i "1,${lines_per_file}d" "$shuffled_file"
  counter=$((counter + 1))
done

# Clean up the temporary shuffled file
rm "$shuffled_file"

echo "File split completed. Files saved in $output_dir"

