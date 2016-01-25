#!/usr/bin/python
# coding=UTF-8

import os
import zipfile
import tempfile
import shutil

__currendir = os.path.abspath(os.path.dirname(__file__))
out = os.path.join(__currendir,"qgis_versioning.zip")

files = ["README.md", "LICENSE", "metadata.txt"]
for file_ in os.listdir(__currendir):
    if file_[-4:]==".svg" or file_[-3:]==".py" or file_[-3:]==".ui":
        files.append(file_)

tmpdir = os.path.join(tempfile.gettempdir(), "qgis_versioning")
if os.path.isdir(tmpdir):
    for file_ in os.listdir(tmpdir):
        os.remove(os.path.join(tmpdir, file_))
else:
    os.mkdir(tmpdir)

for file_ in files:
    shutil.copy(os.path.join(__currendir, file_),
                os.path.join(tmpdir, file_))

os.chdir(tempfile.gettempdir())
with zipfile.ZipFile(out, 'w') as package:
    for root, dirs, files in os.walk("qgis_versioning"):
        for file_ in files:
            print root+"/"+file_
            package.write(os.path.join(root, file_))

print "->", out
