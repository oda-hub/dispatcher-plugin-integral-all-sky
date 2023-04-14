"""
Overview
--------
   
general info about this module


Classes and Inheritance Structure
----------------------------------------------
.. inheritance-diagram:: 

Summary
---------
.. autosummary::
   list of the module you want
    
Module API
----------
"""

from __future__ import absolute_import, division, print_function


__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


from . import conf_file, conf_dir

from cdci_data_analysis.analysis.queries import  *
from cdci_data_analysis.analysis.parameters import Name
from cdci_data_analysis.analysis.instrument import Instrument
from .spiacs_dataserver_dispatcher import   SpiacsDispatcher

from .spiacs_lightcurve_query import   SpiacsLightCurveQuery




def common_instr_query():
    #not exposed to frontend
    #TODO make a special class
    #max_pointings=Integer(value=50,name='max_pointings')



    instr_query_pars=[]


    return instr_query_pars


def spiacs_factory():
    print('--> Spiacs Factory')
    src_query=SourceQuery('src_query')



    instr_query_pars = common_instr_query()

    data_level = Name(name_format='str', name='data level', value="ordinary")
    data_level._allowed_values = ["ordinary", "realtime"]

    instr_query = InstrumentQuery(
        name='spiacs_parameters',
        extra_parameters_list=instr_query_pars + [data_level],
        input_prod_list_name=None,
        input_prod_value=None,
        catalog=None,
        catalog_name='user_catalog')





    light_curve =SpiacsLightCurveQuery('spi_acs_lc_query')



    query_dictionary={}
    query_dictionary['spi_acs_lc'] = 'spi_acs_lc_query'
    #query_dictionary['update_image'] = 'update_image'

    print('--> conf_file',conf_file)
    print('--> conf_dir', conf_dir)



    return  Instrument('spi_acs',
                       asynch=False,
                       data_serve_conf_file=conf_file,                    
                       src_query=src_query,
                       instrumet_query=instr_query,
                       product_queries_list=[light_curve],
                       data_server_query_class=SpiacsDispatcher,
                       query_dictionary=query_dictionary)

