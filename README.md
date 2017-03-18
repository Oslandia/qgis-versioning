Versioning
==========

Build and install the qgis plugin
---------------------------------

    cd
    git clone https://github.com/Oslandia/qgis-versioning.git
    cd qgis-versioning
    ./package.py # compresses all files into qgis_versioning.zip
    cd .qgis2/python/plugins/
    mkdir qgis-versioning
    cd qgis-versioning
    # unzip contents of directory *qgis_versioning* found in qgis_versioning.zip

If you have admin acces to a local postgres/postis server, you can run the regression tests:

    export PYTHONPATH=$PWD
    ./test.py # As of version 0.4; was *make test* in prior versions

Use the plugin in qgis
----------------------

Check that the plugin 'qgis-versioning' is activated in the plugin manager.

Load posgis layers from a scheme you want to version.

Group postgis layers together. Select the group and click on the 'historize' button in the plugin toolbar (make sure the toolbar is displayed). The layers will be replaced by their view in the head revision

Click on the group and then on the 'checkout' button. Choose a file to save your layers locally.

Modify your layers.

Click on the 'commit' icon.

Documentation
=======

For more information on this new plugin, you can test and install the QGIS versioning plugin directly in QGIS (Menu: Plugins=Manage plugins: Versioning) or go on this documentation site: http://qgis-versioning.readthedocs.io/en/latest/. You can also contribute to the source code by sending pull request or open issues if you have any comments or bug to report.

This article described why the plugin has been built and how : GIS Open Source versioning tool for a multi-user Distributed Environment (http://www.gogeomatics.ca/magazine/gis-open-source-versioning-tool-part-1.htm)

Credits
=======

This plugin has been developed by Oslandia ( http://www.oslandia.com ).

Oslandia provides support and assistance for QGIS and associated tools, including this plugin.

This work has been funded by European funds.
Thanks to the GIS Office of Apavil, Valcea County (Romania)

This works has been also supported by eHealth Africa (http://ehealthafrica.org) for SpatiaLite 4.x support, filter selection for SpatiaLite, Diff mode and User identification.

License
=======

This work is free software and licenced under the GNU GPL version 2 or any later version.
See LICENSE file.
