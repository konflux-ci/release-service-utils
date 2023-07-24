#!/usr/bin/env python3
"""test_yamlline.py"""

import os
import tempfile
import yamlline

YAML = b"""---
some:
    test:
        yaml:
            - name: index1
              value: 1
            - name: index2
              value: 2
rootkey:
    - name: identifier1
      key: 
      - name: identifier2 
        parameters:
            SOME_DEEP_PARAMETER: value
somekey: value
"""
with tempfile.NamedTemporaryFile(delete=False) as YAMLFILE:
    YAMLFILE.write(YAML)


def teardown():
    """teardown function"""
    print(f"deleting {YAMLFILE.name}")
    os.unlink(YAMLFILE.name)


def test_load_with_nonexistent_file():
    """ assert that it returns -1 when a non existent file is given"""
    yamlfile = "nofile"
    yamlpath = "some/test/yaml"
    assert yamlline.get_path_line_num(yamlfile, yamlpath) == -1


def test_search_with_dot_separator():
    """ assert that get_path_line_num returns the line if the separator is '.'"""
    # index1 is at line 5
    yamlpath = "some.test.yaml.[name=index1]"
    assert yamlline.get_path_line_num(YAMLFILE.name, yamlpath) == 5


def test_search_with_slash_separator():
    """ assert that get_path_line_num returns the line if the separator is '/'"""
    # index1 is at line 5
    yamlpath = "some/test/yaml/[name=index1]"
    assert yamlline.get_path_line_num(YAMLFILE.name, yamlpath) == 5


def test_search_with_nonexistent_key():
    """ assert that get_path_line_num returns 0 for non existent keys"""
    yamlpath = "some/test/yaml/index1"
    assert yamlline.get_path_line_num(YAMLFILE.name, yamlpath) == 0


def test_search_with_deep_yaml_path():
    """ assert that get_path_line_num returns the line number for a deep key"""
    yamlpath = "rootkey/[name=identifier1]/key/[name=identifier2]/parameters/SOME_DEEP_PARAMETER"
    assert yamlline.get_path_line_num(YAMLFILE.name, yamlpath) == 14


def test_search_with_shallow_yaml_path():
    """ assert that get_path_line_num returns the line number for a shallow key"""
    yamlpath = "somekey"
    assert yamlline.get_path_line_num(YAMLFILE.name, yamlpath) == 15
