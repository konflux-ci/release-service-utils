import logging

from paramiko import AutoAddPolicy, SSHClient

logger = logging.getLogger(__name__)


class SSHConnection:
    def __init__(self, hostname, username, password=None, key_filename=None):
        self.ssh = SSHClient()
        self.ssh.set_missing_host_key_policy(AutoAddPolicy())

        if password is None and key_filename is None:
            raise ValueError("Either password or key_filename must be provided")

        if key_filename:
            self.ssh.connect(hostname, username=username, key_filename=key_filename)
        else:
            self.ssh.connect(hostname, username=username, password=password)

    def run_command(self, command, sensitive=False):
        """
        Run a command on the remote machine

        :param command: The command to run
        :param sensitive: If the command is sensitive, only log at debug level. Still logs errors at info level.
        :return: The output and error of the command
        """
        if sensitive:
            logger.info("Running sensitive command")
        else:
            logger.info(f"Running command: {command}")
        stdin, stdout, stderr = self.ssh.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        if out:
            if sensitive:
                logger.info("Sensitive command output is not logged")
                # Only log sensitive command output at debug level
                logger.debug(f"Command output: {out}")
            else:
                logger.info(f"Command output: {out}")
        if err:
            logger.error(f"Command error: {err}")
        logger.info(f"Command exit status: {exit_status}")
        return out, err

    def _close(self):
        self.ssh.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close()
