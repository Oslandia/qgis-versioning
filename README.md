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

	export QGIS_PREFIX_PATH=/path/to/your/qgis/installation
    export PYTHONPATH=$QGIS_DIR/python:$PYTHONPATH
	python3 tests.py 127.0.0.1 postgres -v
	
And if you want to run only one regression test: 

	export QGIS_PREFIX_PATH=/path/to/your/qgis/installation
    export PYTHONPATH=$QGIS_DIR/python:..:$PYTHONPATH
	python3 plugin_test.py 127.0.0.1 postgres

	
Use the plugin in qgis
----------------------

Check that the plugin 'qgis-versioning' is activated in the plugin manager or install the versioning plugin directly in QGIS (Menu : Plugins = Manage plugins : Versioning).

Load posgis layers from a scheme you want to version.

Group postgis layers together. Select the group and click on the 'historize' button in the plugin toolbar (make sure the toolbar is displayed). The layers will be replaced by their view in the head revision

Click on the group and then on the 'checkout' button. Choose a file to save your layers locally.

Modify your layers.

Click on the 'commit' icon.

Documentation
=======

For more information on this plugin, you can go on its plugin documentation site: http://qgis-versioning.readthedocs.io/en/latest/. You can also contribute to the source code by sending pull request or open issues if you have any comments or bug to report.

See also this article describing why the plugin has been built and how : [GIS Open Source versioning tool for a multi-user Distributed Environment](http://www.gogeomatics.ca/magazine/gis-open-source-versioning-tool-part-1.htm)
Cet article est aussi disponible en français : http://www.gogeomatics.ca/magazine/outil-de-versionnement-a-code-source-ouvert-partie-1.htm

Credits
=======

This plugin has been developed by Oslandia (http://www.oslandia.com).

Oslandia provides support and assistance for QGIS and associated tools, including this plugin.

This work has been funded by European funds.
Thanks to the GIS Office of Apavil, Valcea County (Romania)

This work has been also developed by eHealth Africa (http://ehealthafrica.org) for SpatiaLite 4.x support, filter selection for SpatiaLite file, diff mode and user identification improvements.

License
=======

This work is free software and licenced under the GNU GPL version 2 or any later version.
See LICENSE file.
