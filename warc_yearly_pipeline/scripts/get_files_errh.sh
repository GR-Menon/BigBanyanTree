#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <input_file> <output_directory>"
    exit 1
fi

# Arguments
input_txt=$1
output_dir=$2

temp_file="temp_input.txt"
touch $temp_file

echo "Downloading files from file: $input_txt ..."

# Create the output directory if it doesn't exist
mkdir -p "$output_dir"

# Log file for failed downloads
failed_log="./failed_downloads.log"
# >> "$failed_log"  # Clear the log file

# Total number of files to download
total_files=$(wc -l < "$input_txt")
echo "Total files to download: $total_files"

batch_count=0

# Download 10 files in parallel using xargs and wget
cat "$input_txt" | xargs -P 10 -I {} sh -c '
    file_name=$(basename "{}")
    url="https://data.commoncrawl.org/{}"
    
    if [ -f "'"$output_dir"'/$file_name" ]; then
        echo "Skipping already downloaded $file_name"
    else
        echo "Downloading $url ..."
        if wget --spider -q "$url"; then
            if wget -q -P "'"$output_dir"'" "$url"; then
                echo "$file_name downloaded successfully"
                echo "{}" >> "'"$temp_file"'"
            else
                echo "Failed to download $url" >> "'"$failed_log"'"
            fi
        else
            echo "404 error for $url, removing from input"
            echo "Failed to download $url (404)" >> "'"$failed_log"'"
        fi
    fi
    
    batch_count=$((batch_count + 1))  # Increment batch counter
    if [ $((batch_count % 10)) -eq 0 ]; then
        echo "Pausing for 3 seconds..."
        sleep 3  # Pause for 3 seconds after every batch of 10
    fi
'
mv "$temp_file" "$input_txt"

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
    if ! gzip -d "{}"; then
        echo "Failed to extract {}" >> "'"$failed_log"'"
    fi
'

# Check if there were failed downloads
# if [ -s "$failed_log" ]; then
#     echo "Some files failed to download or extract. Check $failed_log for details."
# else
#     echo "All files downloaded and processed successfully."
# fi

echo "Download and extraction complete."