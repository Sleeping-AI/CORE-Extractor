import os
import lzma
import json
import pyarrow as pa
import pyarrow.parquet as pq
import re

DESIRED_KEYS = ['coreId', 'title', 'authors', 'datePublished', 'fullText', 'relations', 'year']

def stream_json_xz(file_path):
    with lzma.open(file_path, mode='rt') as file:
        for line in file:
            try:
                json_data = json.loads(line)
                filtered_data = {key: json_data.get(key) for key in DESIRED_KEYS}
                yield filtered_data
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

def process_file(file_path):
    batch_data = []
    for json_object in stream_json_xz(file_path):
        if json_object.get('fullText'):
            batch_data.append(json_object)
    return batch_data

def save_to_parquet(data, output_path):
    if data:
        arrays = {key: pa.array([obj.get(key) for obj in data]) for key in DESIRED_KEYS}
        table = pa.table(arrays)
        pq.write_table(table, output_path)

def process_directory(directory_path, output_filename, checkpoint_dir, process_all):
    all_data = []
    checkpoint_counter = 0
    file_counter = 0
    final_output_path = os.path.join('/ammar_storage', output_filename)
    final_checkpoint_path = os.path.join(checkpoint_dir, 'final_checkpoint.parquet')

    files = [f for f in os.listdir(directory_path) if f.endswith('.json.xz')]
    files.sort(key=lambda f: int(re.search(r'(\d+)', f).group()))

    for file in files:
        if not process_all and file_counter >= 3:
            break

        file_path = os.path.join(directory_path, file)
        if not os.path.exists(file_path):
            print(f"File missing: {file_path}")
            continue

        print(f"Processing file: {file_path}")
        batch_data = process_file(file_path)
        all_data.extend(batch_data)
        file_counter += 1

        # Save checkpoint every 30 files instead of 100
        if file_counter % 30 == 0:
            checkpoint_counter += 1
            checkpoint_path = os.path.join(checkpoint_dir, f"checkpoint_{checkpoint_counter}.parquet")
            save_to_parquet(all_data, checkpoint_path)
            all_data = []
            print(f"Checkpoint {checkpoint_counter} created at {checkpoint_path}")
        elif not process_all and file_counter == 3:
            break

        print(f"Processed file: {file_path}")

    if all_data:
        final_checkpoint_path = os.path.join(checkpoint_dir, 'final_checkpoint.parquet')
        save_to_parquet(all_data, final_checkpoint_path)
        print(f"Final checkpoint created at {final_checkpoint_path}")

    combine_checkpoints(checkpoint_dir, final_output_path)

def combine_checkpoints(checkpoint_dir, final_output_path):
    tables = []
    for root, dirs, files in os.walk(checkpoint_dir):
        for file in files:
            if file.endswith('.parquet'):
                checkpoint_path = os.path.join(root, file)
                tables.append(pq.read_table(checkpoint_path))

    if tables:
        combined_table = pa.concat_tables(tables)
        pq.write_table(combined_table, final_output_path)
        print(f"All checkpoints combined into final file at {final_output_path}")

directory_path = '/ammar_storage/core/core_2018-03-01_fulltext'

process_all = input("Do you want to process the entire directory? (yes/no): ").strip().lower() == 'yes'
while True:
    output_filename = input("Enter the output Parquet file name (must end with .parquet): ")
    if output_filename.lower().endswith('.parquet'):
        break
    else:
        print("Invalid file extension. Please enter a file name ending with .parquet.")

checkpoint_dir = input("Enter the checkpoint folder name (will be created in /ammar_storage): ")

checkpoint_dir_path = os.path.join('/ammar_storage', checkpoint_dir)
os.makedirs(checkpoint_dir_path, exist_ok=True)

process_directory(directory_path, output_filename, checkpoint_dir_path, process_all)
