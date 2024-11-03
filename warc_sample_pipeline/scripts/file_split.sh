#!/bin/bash

# Input file containing 1000 WARC file paths
input_file="warc.paths"

# Output directory for split files
output_dir="warc_splits"
mkdir -p "$output_dir"

# Check if the input file exists
if [ ! -f "$input_file" ]; then
  echo "Input file not found!"
  exit 1
fi

# Shuffle the lines in the input file
shuffled_file=$(mktemp)
shuf "$input_file" > "$shuffled_file"

# Number of lines to sample per file
lines_per_file=60

input_file_len=$(wc -l < "$input_file")
# Total number of output files
total_files=$(($input_file_len / lines_per_file))

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

