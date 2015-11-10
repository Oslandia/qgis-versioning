Versioning
==========

Build and install the qgis plugin
---------------------------------

    cd
    git clone https://github.com/Oslandia/qgis-versioning.git
    cd .qgis2/python/plugins/ 
    mkdir qgis-versioning
    cd qgis-versioning
    cmake $HOME/qgis-versioning && make

If you have admin acces to a local postgres/postis server, you can run the regression tests:
    
    export PYTHONPATH=$PWD
    make test

Use the plugin in qgis
----------------------

Check that the plugin 'qgis-versioning' is activated in the plugin manager.

Load posgis layers from a scheme you want to version.

Group postgis layers together. Select the group and click on the 'historize' button in the plugin toolbar (make sure the toolbar is displayed). The layers will be replaced by their view in the head revision

Click on the group and then on the 'checkout' button. Choose a file to save your layers locally.

Modify your layers.

Click on the 'commit' icon.

Credits
=======

This plugin has been developed by Oslandia ( http://www.oslandia.com ).

Oslandia provides support and assistance for QGIS and associated tools, including this plugin.

This work has been funded by European funds.
Thanks to the GIS Office of Apavil, Valcea County (Romania)

Support for spatialite 4.x was contributed by eHealth Africa (http://ehealthafrica.org).

License
=======

This work is free software and licenced under the GNU GPL version 2 or any later version.
See LICENSE file.
