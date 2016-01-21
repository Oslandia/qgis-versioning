#!/usr/bin/python
# coding=UTF-8

import os
import zipfile
import tempfile
import shutil

__currendir = os.path.dirname(__file__)
files = ["README.md", "LICENSE", "metadata.txt"]
for file_ in os.listdir(__currendir):
    if file_[-4:]==".svg" or file_[-3:]==".py" or file_[-3:]==".ui":
        files.append(file_)

print files
tmpdir = os.path.join(tempfile.gettempdir(), "qgis_versioning")
print tmpdir
if os.path.isdir(tmpdir):
    for file_ in os.listdir(tmpdir):
        print "remove ", file_ ,"from", tmpdir
        os.remove(os.path.join(tmpdir, file_))
else:
    print "create", tmpdir
    os.mkdir(tmpdir)

for file_ in files:
    shutil.copy(os.path.join(__currendir, file_),
                os.path.join(tmpdir, file_))

with zipfile.ZipFile("qgis_versioning.zip", 'w') as package:
    for root, dirs, files in os.walk(tmpdir):
        for file_ in files:
            package.write(os.path.join(root, file_))


