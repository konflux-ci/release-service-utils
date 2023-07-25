#!/usr/bin/env python3
"""yamlline.py

Script to return the line number of a yaml path in a yaml file.

"""

import argparse
import re
import sys


def get_path_line_num(yamlfile, yamlpath):
    """Returns the line number of the given yaml path in a yaml file"""

    # try split the path using ".", otherwise tries "/"
    search_path = yamlpath.split(".")
    if len(search_path) == 1:
        search_path = yamlpath.split("/")

    line_number = 0
    try:
        with open(yamlfile, encoding="utf8") as file_name:
            found_root = False
            for line in iter(file_name.readline, ""):
                # leave the loop and return the line number
                # if there are no more strings to search
                if len(search_path) == 0:
                    return line_number

                try:
                    # check if the search string is a root key of a yaml path
                    if re.search(f"^{search_path[0]}:", line):
                        search_path.pop(0)
                        found_root = True

                        # return early if the path has a single key
                        if len(search_path) == 0:
                            line_number += 1
                            return line_number

                    # if the root key was already found, search only for the
                    # keys that are deeper
                    if found_root is True:
                        if re.search(f"\\s+.*{search_path[0]}:", line):
                            search_path.pop(0)

                except re.error:
                    # in case the key is a composite of [key=value] it should
                    # rebuild the string as `key: val` and then search for it
                    rawkey = search_path[0][1:-1]
                    key, val = rawkey.split("=")
                    search = f"{key}:.*{val}$"
                    if re.search(search, line):
                        search_path.pop(0)

                except ValueError:
                    pass

                line_number += 1

    except FileNotFoundError:
        print(f"file {yamlfile} was not found")
        return -1

    return 0


def main():
    """main function"""

    parser = argparse.ArgumentParser(description="Parameters for yamlline.")
    parser.add_argument(
        "-f", "--file", dest="filename", help="YAML file to read", metavar="FILE"
    )
    parser.add_argument("-p", "--path", dest="path", help="YAML path that we want to know the line number")
    args = parser.parse_args()

    if args.filename is None or args.path is None:
        parser.print_help()
        sys.exit(0)

    line_number = get_path_line_num(args.filename, args.path)
    print(line_number)


if __name__ == "__main__":
    main()
