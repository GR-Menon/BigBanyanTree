#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <input_file> <output_directory>"
    exit 1
fi

# Arguments
input_txt=$1
output_dir=$2

echo "Downloading files from file: $input_txt ..."

# Create the output directory if it doesn't exist
mkdir -p "$output_dir"

# Total number of files to download
total_files=$(wc -l < "$input_txt")
echo "Total files to download: $total_files"

# Download 10 files in parallel using xargs and wget
cat "$input_txt" | xargs -P 10 -I {} sh -c '
    echo "Downloading https://data.commoncrawl.org/{} ..."
    wget -q -P "'"$output_dir"'" "https://data.commoncrawl.org/{}"
'

# Check for .gz files in the directory
gz_files=$(ls "$output_dir"/*.gz 2>/dev/null)
if [ -z "$gz_files" ]; then
    echo "No .gz files found to process."
    exit 1
fi

# Total number of files downloaded
downloaded_files=$(ls "$output_dir"/*.gz | wc -l)
echo "Total files downloaded: $downloaded_files"

# Extract .gz files in parallel, 10 at a time
echo "Processing downloaded .gz files..."

# Process each .gz file in parallel (up to 10 files at once)
ls "$output_dir"/*.gz | xargs -P 10 -I {} sh -c '
    echo "Processing {} ..."
    
    # Print file size before extraction
    echo "File size before extraction:"
    du -h "{}"
    
    # Extract file
    gzip -d "{}"
'

echo "Download and extraction complete."
