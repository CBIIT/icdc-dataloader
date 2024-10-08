import os
import shutil
import subprocess
import sys
from bento.common.utils import get_time_stamp

def get_latest_file(src_folder):
    files = [os.path.join(src_folder, f) for f in os.listdir(src_folder) if os.path.isfile(os.path.join(src_folder, f))]
    if not files:
        return None
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def backup_memgraph_mgconsole(memgraph_backup_dir, memgraph_dump_dir, memgraph_username, memgraph_password, log):
    try:
        timestamp = get_time_stamp()
        memgraph_dump_file_name = "memgraph_dump" + "_" + timestamp + ".cypherl"
        memgraph_dump_file_key = os.path.join("/"+os.path.basename(memgraph_dump_dir), memgraph_dump_file_name )
        backup_command = f'''docker exec -it memgraph_docker-memgraph-1 bash -c \
        'echo "DUMP DATABASE;" | mgconsole \
        --username {memgraph_username} --password {memgraph_password} --output-format=cypherl > {memgraph_dump_file_key}' \
        '''
        subprocess.run(backup_command, shell=True)
        dump_file_key = os.path.join(memgraph_dump_dir, memgraph_dump_file_name)
        dest_file_path = os.path.join(memgraph_backup_dir, memgraph_dump_file_name)
        shutil.copy2(dump_file_key, dest_file_path)
        log.info(f"Successfully copied the backup snapshot file {dump_file_key} to {dest_file_path}")
        return dest_file_path
    except Exception as e:
        log.error(e)
        return "Backup Memgraph failed"

def backup_memgraph(memgraph_backup_dir, memgraph_snapshot_dir, log):
    try:
        latest_file = get_latest_file(memgraph_snapshot_dir)
        if latest_file is None:
            log.error(f"No files found in the snapshot folder {memgraph_snapshot_dir}.")
            return False
        mkdir_cmd = [
                'mkdir',
                '-p',
                memgraph_backup_dir
            ]
        subprocess.call(mkdir_cmd)
        dest_file_path = os.path.join(memgraph_backup_dir, os.path.basename(latest_file))
        shutil.copy2(latest_file, dest_file_path)
        log.info(f"Successfully copied the backup snapshot file {latest_file} to {dest_file_path}")
        return dest_file_path
    except Exception as e:
        log.error(e)
        return "Backup Memgraph failed"

def delete_files(folder_path_list, log):
    # List all files in the folder
    for folder_path in folder_path_list:
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
        if not files:
            log.info("No files found in the folder.")
            return
        # Delete each file
        for file_path in files:
            os.remove(file_path)
            log.info(f"Deleted {file_path}")

def restore_memgraph(backup_file, memgraph_snapshot_dir, memgraph_wal_dir, memgraph_docker_file, address, log):
    try:
        if address in ['localhost', '127.0.0.1']:
            stop_memgraph_cmd = [
                "docker-compose",
                "-f",
                memgraph_docker_file,
                "stop"
            ]
            log.info("Stop the Memgraph container")
            subprocess.call(stop_memgraph_cmd)
            delete_folder_list = [memgraph_snapshot_dir, memgraph_wal_dir]
            delete_files(delete_folder_list, log)
            dest_file_path = os.path.join(memgraph_snapshot_dir, os.path.basename(backup_file))
            log.info(f"Copy the backup snapshot file {backup_file} to {dest_file_path}")
            shutil.copy2(backup_file, dest_file_path)
            start_memgraph_cmd = [
                "docker-compose",
                "-f",
                memgraph_docker_file,
                "up",
                "-d"
            ]
            log.info("Start the Memgraph container")
            subprocess.call(start_memgraph_cmd)
    except Exception as e:
        log.error(e)
        return False

