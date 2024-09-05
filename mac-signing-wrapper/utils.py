import os


def zip_files(ssh, target_dir, zip_name):
    zip_path = os.path.join(target_dir, zip_name)
    command = f"cd {target_dir} && zip -r {zip_path} ."
    ssh.run_command(command)
    return zip_path


def unzip_files(ssh, zip_path, output_dir):
    command = f"unzip -o {zip_path} -d {output_dir}"
    ssh.run_command(command)


def cleanup(ssh, target_dir):
    command = f"rm -rf {target_dir}"
    ssh.run_command(command)
