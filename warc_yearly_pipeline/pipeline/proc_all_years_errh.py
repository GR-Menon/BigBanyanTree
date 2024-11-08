import os
import pathlib
import shutil
import subprocess
import time

wp = "warc_2018.paths"
year = "2018"

os.makedirs("../unsuccessful/", exist_ok=True)
os.makedirs("../success/", exist_ok=True)


def num_warcs_to_proc(wp_file: str) -> int:
    """Returns the number of lines in the warc.paths file."""
    with open(f"./warc_paths/{wp_file}", 'r') as f:
        for count, _ in enumerate(f):
            pass
    return count + 1


def gen_file_splits(wp_file: str):
    """Given a warc.paths file, generates `.txt` files having specified number of WARC filepaths"""
    warc_sample_len = num_warcs_to_proc(wp_file) // 100  # not used. 900 is used directly
    os.system(f"./file_split.sh warc_paths/{wp_file} warc_splits/ 900 {wp_file.split('_')[-1].split('.')[0]}")


def to_paths(input_txt):
    """Converts the WARC URLs to their corresponding paths on the device."""
    updated = []
    with open(input_txt, 'r') as f:
        for l in f:
            l = l.split('/')[-1]
            updated.append("/opt/workspace/datasets/common_crawl/" + '.'.join(l.split('.')[:-1]))

    with open(input_txt, 'w') as f:
        for l in updated:
            f.write(l + "\n")


def submit_job(input_txt: str):
    """Submits two spark jobs and waits for them to finish. If both jobs succeed, then the `input_txt` file is moved to success/ dir."""
    os.makedirs("tmp/", exist_ok=True)
    cmd1 = ["spark-submit", "ipwarc_mmdb_pdudf-errh.py", "--input_file", f"warc_splits/{input_txt}", "--output_dir",
            "tmp/ipmaxmind_out", "--year", year]
    cmd2 = ["spark-submit", "script_extraction-errh.py", "--input_file", f"warc_splits/{input_txt}", "--output_dir",
            "tmp/script_extraction_out", "--year", year]

    status_file = "../logs/job_status.txt"
    if os.path.exists(status_file):
        os.remove(status_file)

    process1 = subprocess.Popen(cmd1)
    process2 = subprocess.Popen(cmd2)

    process1.wait()
    process2.wait()

    with open(status_file, 'r') as f:
        statuses = f.readlines()

    # Check if both jobs succeeded
    if all("success" in status for status in statuses):

        # Move temp output to final directory
        for filename in os.listdir("tmp/ipmaxmind_out/"):
            if filename == ".ipynb_checkpoints": continue
            src_file = os.path.join("tmp/ipmaxmind_out/", filename)
            dst_file = os.path.join(f"ipmaxmind_out_{year}/", filename)
            shutil.move(src_file, dst_file)

        for filename in os.listdir("tmp/script_extraction_out/"):
            if filename == ".ipynb_checkpoints": continue
            src_file = os.path.join("tmp/script_extraction_out/", filename)
            dst_file = os.path.join(f"script_extraction_out_{year}/", filename)
            shutil.move(src_file, dst_file)

        print("Both jobs succeeded. Outputs moved to final directories.")

        input_dir = os.path.dirname(f"warc_splits/{input_txt}")
        shutil.move(f"warc_splits/{input_txt}",
                    os.path.join("../success/", os.path.basename(f"warc_splits/{input_txt}")))

        print(f"Processing completed successfully. Input file warc_splits/{input_txt} moved to success/")

    else:
        # If any job failed, discard temporary output
        shutil.rmtree('tmp/ipmaxmind_out', ignore_errors=True)
        shutil.rmtree('tmp/script_extraction_out', ignore_errors=True)
        print("One or more jobs failed. Outputs discarded.")


def process_wp(wp_file: str):
    """Process a warc.paths file by generating splits, and submitting each of the split `.txt` file to spark."""
    start_time = time.time()
    os.makedirs("warc_splits", exist_ok=True)
    gen_file_splits(wp_file)

    ckpt_dir = pathlib.Path("warc_splits/.ipynb_checkpoints/")
    if ckpt_dir.exists() and ckpt_dir.is_dir():
        shutil.rmtree(ckpt_dir)

    data_dir = "/opt/workspace/datasets/common_crawl/"
    # data_dir = "/opt/workspace/warc_yearly_pipeline/data/"
    for input_txt in sorted(os.listdir("warc_splits")):
        if input_txt == ".ipynb_checkpoints": continue
        os.makedirs(data_dir)
        os.system(f"./get_files_errh.sh warc_splits/{input_txt} {data_dir}")
        to_paths(f"warc_splits/{input_txt}")
        submit_job(input_txt)
        shutil.rmtree(data_dir)

    # files that are processed successfully are moved to `success/`.
    # remaining files are hence not processed successfully.
    for file in os.listdir("warc_splits"):
        if file == ".ipynb_checkpoints": continue
        shutil.move(f"warc_splits/{file}", os.path.join("../unsuccessful/", os.path.basename(file)))

    end_time = time.time()
    total_time = end_time - start_time
    with open("../times.txt", 'a') as f:
        f.write(f"[{wp_file}]: {total_time:.2f} seconds\n")


os.makedirs(f"ipmaxmind_out_{year}/", exist_ok=True)
os.makedirs(f"script_extraction_out_{year}/", exist_ok=True)

process_wp(wp)
