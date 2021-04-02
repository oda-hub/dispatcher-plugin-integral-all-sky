

from __future__ import absolute_import, division, print_function


import pkgutil
import os

__author__ = "Andrea Tramacere"



pkg_dir = os.path.abspath(os.path.dirname(__file__))
pkg_name = os.path.basename(pkg_dir)
__all__=[]
for importer, modname, ispkg in pkgutil.walk_packages(path=[pkg_dir],
                                                      prefix=pkg_name+'.',
                                                      onerror=lambda x: None):

    if ispkg == True:
        __all__.append(modname)
    else:
        pass



conf_dir=os.path.dirname(__file__)+'/config_dir'

if conf_dir is not None:
    conf_dir=conf_dir


def find_config():
    config_file_resolution_order=[
        os.environ.get('CDCI_SPIACS_PLUGIN_CONF_FILE','.spiacs_data_server_conf.yml'),
        os.path.join(conf_dir,'data_server_conf.yml'),
        "/dispatcher/conf/conf.d/spiacs_data_server_conf.yml",
    ]

    for conf_file in config_file_resolution_order:
        if conf_file is not None and os.path.exists(conf_file): # and readable?
            return conf_file

    raise RuntimeError("no spiacs config found, tried: "+", ".join(config_file_resolution_order))

conf_file=find_config()
