#!/usr/bin/env python3
"""Yamlline

Simple helper script to return the actual line number of a yaml path in
a yaml file.

    * get_path_line_num - return the line number of the yaml path
    * main - the main function of the script
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
            # read the file line by line
            for line in iter(file_name.readline, ""):
                # leave the loop and return the line number
                # if the yaml path is found
                if len(search_path) == 0:
                    return line_number

                try:
                    if re.search(f"^{search_path[0]}:", line):
                        search_path.pop(0)
                        found_root = True
                        # return if it is the only key
                        if len(search_path) == 0:
                            line_number += 1
                            return line_number

                    if found_root is True:
                        if re.search(f"\\s+.*{search_path[0]}:", line):
                            search_path.pop(0)

                except re.error:
                    rawkey = search_path[0][1:-1]
                    key, val = rawkey.split("=")
                    search = f"{key}.*{val}$"
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
    parser.add_argument("-p", "--path", dest="path", help="YAML path to get the line number")
    args = parser.parse_args()

    if args.filename is None or args.path is None:
        parser.print_help()
        sys.exit(0)

    line_number = get_path_line_num(args.filename, args.path)
    print(line_number)


if __name__ == "__main__":
    main()
