class OrasCommands:
    def __init__(self, ssh, registry, username, password):
        self.ssh = ssh
        self.registry = registry
        self.username = username
        self.password = password
        self.set_env_vars()

    def set_env_vars(self):
        """
        Set environment variables for the ORAS commands.
        """
        env_vars = (
            f"export ORAS_USERNAME='{self.username}' && export ORAS_PASSWORD='{self.password}'"
        )
        self.ssh.run_command(env_vars, sensitive=True)

    def pull_content(self, digest, target_dir):
        """
        Pull content from registry using the digest inside a target directory.
        """
        create_and_enter_dir = f"mkdir -p {target_dir} && cd {target_dir}"
        pull_command = f"oras pull {self.registry}@sha256:{digest}"
        check_files_command = f"ls -1 {target_dir} | wc -l"

        self.ssh.run_command(create_and_enter_dir)
        pull_output, err = self.ssh.run_command(pull_command)
        if err:
            raise Exception(f"Error pulling content: {err}")

        file_count, _ = self.ssh.run_command(check_files_command)
        if int(file_count) == 0:
            raise Exception("ORAS pull command did not retrieve any files")

        return pull_output

    def push_zip(self, zip_path):
        """
        Push a zip file to the registry and return the digest.
        ONLY ZIP FILES ARE SUPPORTED, can we extended later.
        """
        if not zip_path.endswith(".zip"):
            raise ValueError("The provided file must be a zip file")

        push_command = f"oras push {self.registry} {zip_path}"

        output, _ = self.ssh.run_command(push_command)
        digest = get_sha256(output)

        if digest:
            return digest
        else:
            raise ValueError(f"No SHA256 found in the oras push output. Full output: {output}")


def get_sha256(a_string: str):
    for line in a_string.splitlines():
        if "sha256:" in line:
            parts = line.split("sha256:")
            if len(parts) > 1:
                return parts[1].strip()
    return None
