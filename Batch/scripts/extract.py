import subprocess
import os
from datetime import datetime, timedelta
import glob

def upload_to_hdfs(local_base_dir, hdfs_base_path):
    # Get the current time
    current_time = datetime.now()
    # Calculate the start and end of the previous hour window
    previous_hour_start = current_time - timedelta(hours=1)
    previous_hour_end = current_time

    try:
        for local_dir in os.listdir(local_base_dir):
            full_local_dir_path = os.path.join(local_base_dir, local_dir)
            if os.path.isdir(full_local_dir_path):
                # Get the creation time of the directory
                dir_creation_time = datetime.fromtimestamp(os.path.getctime(full_local_dir_path))

                # Check if the directory was created in the last hour
                if previous_hour_start <= dir_creation_time < previous_hour_end:
                    # Find all .csv files in this directory
                    local_files = glob.glob(os.path.join(full_local_dir_path, '*.csv'))

                    # Extract year, month, day, and hour from the previous hour's timestamp
                    year = previous_hour_start.strftime('%Y')       # Format as YYYY
                    month = previous_hour_start.strftime('%m')      # Format as MM
                    day = previous_hour_start.strftime('%d')        # Format as DD
                    hour = previous_hour_start.strftime('%H')       # Format as HH

                    # Construct HDFS destination base path with year, month, day, and hour subdirectories
                    hdfs_destination_base_path = f"{hdfs_base_path}/year={year}/month={month}/day={day}/hour={hour}/"

                    for local_file in local_files:
                        # Construct destination path in HDFS by preserving the structure after the base path
                        file_name = os.path.basename(local_file)
                        hdfs_destination_path = os.path.join(hdfs_destination_base_path, file_name)

                        # Create directories recursively if they don't exist in HDFS
                        create_dirs_in_hdfs(os.path.dirname(hdfs_destination_path))

                        # Command to upload file to HDFS
                        command = ["hdfs", "dfs", "-put", local_file, hdfs_destination_path]

                        # Execute the command
                        result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                        print(f"Successfully uploaded {local_file} to {hdfs_destination_path}")

                else:
                    print(f"Skipping directory {full_local_dir_path}: Not created in the last hour.")
            else:
                print(f"Skipping {full_local_dir_path}: Not a directory.")

    except subprocess.CalledProcessError as e:
        print(f"Failed to upload files to HDFS.")
        print(f"Error: {e.stderr.decode('utf-8')}")

def create_dirs_in_hdfs(hdfs_dir):
    # Function to create directories recursively in HDFS if they don't exist
    try:
        subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-mkdir", "-p", hdfs_dir], check=True)
        print(f"Created directory in HDFS: {hdfs_dir}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to create directory in HDFS: {hdfs_dir}")
        print(f"Error: {e.stderr.decode('utf-8')}")

# Example usage
local_base_dir = "/data/project/data"
hdfs_base_path = "/retail_data"

upload_to_hdfs(local_base_dir, hdfs_base_path)
