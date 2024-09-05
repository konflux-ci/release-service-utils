import json


class MacOSCommands:
    def __init__(self, ssh):
        self.ssh = ssh

    def unlock_keychain(self, keychain_password):
        """
        Unlock the login keychain

        :param keychain_password: The password to unlock the keychain
        :return: The output of the command
        """
        return self.ssh.run_command(
            f"security unlock-keychain -p {keychain_password} login.keychain", sensitive=True
        )

    def sign_binaries(self, signing_identity, target_dir):
        """
        Sign all files in the target directory

        :param signing_identity: The identity to sign the files with
        :param target_dir: The directory containing the files to sign
        :return: A list of signed files
        """
        signed_files = []
        list_files_command = f"find {target_dir} -type f"
        out, _ = self.ssh.run_command(list_files_command)
        for file_path in out.splitlines():
            command = (
                f"xcrun codesign "
                f"--sign 'Developer ID Application: {signing_identity}' "
                f"--options runtime --timestamp --force {file_path}"
            )
            self.ssh.run_command(command, sensitive=True)
            signed_files.append(file_path)
        return signed_files

    def notarize_binaries(self, username, password, team_id, zip_path):
        """
        Submit the zip file for notarization

        :param username: The Apple ID username
        :param password: The app-specific password
        :param team_id: The team ID
        :param zip_path: The path to the zip file to submit
        :return: The notarization result

        Raises an exception if the notarization fails
        """
        command = (
            f"xcrun notarytool submit {zip_path} "
            f"--output-format json "
            f"--wait "
            f"--apple-id {username} "
            f"--team-id {team_id} "
            f"--password {password}"
        )

        out, err = self.ssh.run_command(command, sensitive=True)
        if err:
            raise Exception(f"Notarization error: {err}")

        notarization_result = json.loads(out)

        if notarization_result["status"] != "Accepted":
            error_message = f"Notarization failed. Status: {notarization_result['status']}"
            if "issues" in notarization_result:
                error_message += "\nIssues:"
                for issue in notarization_result["issues"]:
                    error_message += f"\n- {issue['message']}"
            raise Exception(error_message)

        return notarization_result
