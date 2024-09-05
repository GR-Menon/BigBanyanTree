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

# Download the sampled files to the output directory ("https://data.commoncrawl.org/$url")
while IFS= read -r url; do
    wget -P "$output_dir" "https://data.commoncrawl.org/$url"
done < "$input_txt"

# Extract downloaded .gz files one by one
for file in "$output_dir"/*.gz; do
    if [ -f "$file" ]; then
        echo "Processing $file ..."
        
        # Print file size before extraction
        echo "File size before extraction:"
        du -h "$file"
        
        # Move the uncompressed file to the output directory (in case extraction creates it in the current directory)
        # mv "$uncompressed_file" "$output_dir/"
    else
        echo "No .gz files found to process."
    fi
done

ls $output_dir/*.gz | xargs -P 10 -I {} gzip -d {}
