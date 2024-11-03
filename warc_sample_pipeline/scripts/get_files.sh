#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <number_of_files> <output_directory>"
    exit 1
fi

# Arguments
num_files=$1
output_dir=$2

# Create a temporary file for sampled file URLs
temp_file=$(mktemp)

# Sample the specified number of file URLs from warc.paths
echo "Sampling $num_files files from warc.paths ..."
shuf -n "$num_files" warc.paths > "$temp_file"

# Create the output directory if it doesn't exist
mkdir -p "$output_dir"

# Download the sampled files to the output directory ("https://data.commoncrawl.org/$url")
while IFS= read -r url; do
    wget -P "$output_dir" "https://data.commoncrawl.org/$url"
done < "$temp_file"

# Extract downloaded .gz files one by one
for file in "$output_dir"/*.gz; do
    if [ -f "$file" ]; then
        echo "Processing $file ..."
        
        # Print file size before extraction
        echo "File size before extraction:"
        du -h "$file"
        
        # Extract the .gz file
        gunzip "$file"
        
        # Print file size after extraction
        echo "File size after extraction (for uncompressed file):"
        # Assuming the .gz file was a single file archive, the uncompressed file will have the same base name
        uncompressed_file="${file%.gz}"
        du -h "$uncompressed_file"
        
        # Move the uncompressed file to the output directory (in case extraction creates it in the current directory)
        mv "$uncompressed_file" "$output_dir/"
    else
        echo "No .gz files found to process."
    fi
done

# Clean up
rm "$temp_file"
