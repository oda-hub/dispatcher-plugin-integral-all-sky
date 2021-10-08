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

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

__author__ = "Volodymyr Savchenko, Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy
import ast
import requests
# Dependencies
# eg numpy
# absolute import eg: import numpy as np
import json

# Project
# relative import eg: from .mod import f
import logging
from . import conf_file as plugin_conf_file
from cdci_data_analysis.configurer import DataServerConf
from cdci_data_analysis.analysis.queries import *
from cdci_data_analysis.analysis.job_manager import Job
from cdci_data_analysis.analysis.io_helper import FilePath
from cdci_data_analysis.analysis.products import QueryOutput
import json
import traceback
import time
from ast import literal_eval
import os
from contextlib import contextmanager


logger = logging.getLogger('spiacs_dataserver_dispatcher')


class SpiacsAnalysisException(Exception):

    def __init__(self, message='Spiacs analysis exception', debug_message=''):
        super(SpiacsAnalysisException, self).__init__(message)
        self.message = message
        self.debug_message = debug_message


class SpiacsException(Exception):

    def __init__(self, message='Spiacs analysis exception', debug_message=''):
        super(SpiacsException, self).__init__(message)
        self.message = message
        self.debug_message = debug_message


class SpiacsUnknownException(SpiacsException):

    def __init__(self, message='Spiacs unknown exception', debug_message=''):
        super(SpiacsUnknownException, self).__init__(message, debug_message)


class SpiacsDispatcher(object):

    def __init__(self, config=None, param_dict=None, instrument=None):
        logger.info('--> building class SpiacsDispatcher instrument: %s config: %s', instrument, config)

        self.param_dict = param_dict

        
        config = DataServerConf(data_server_url=instrument.data_server_conf_dict['data_server_url'],
                                dummy_cache=instrument.data_server_conf_dict['dummy_cache'])

        logger.info('--> config passed to init %s', config)

        if config is not None:
            pass

        elif instrument is not None and hasattr(instrument, 'data_server_conf_dict'):

            logger.info('--> from data_server_conf_dict')
            try:
                # config = DataServerConf(data_server_url=instrument.data_server_conf_dict['data_server_url'],
                #                        data_server_port=instrument.data_server_conf_dict['data_server_port'])

                config = DataServerConf(data_server_url=instrument.data_server_conf_dict['data_server_url'],
                                        data_server_port=instrument.data_server_conf_dict['data_server_port'])
                # data_server_remote_cache=instrument.data_server_conf_dict['data_server_cache'],
                # dispatcher_mnt_point=instrument.data_server_conf_dict['dispatcher_mnt_point'],
                # s dummy_cache=instrument.data_server_conf_dict['dummy_cache'])

                logger.info('config %s', config)
                for v in vars(config):
                    logger.info('attr: %s = %s', v, getattr(config, v))

            except Exception as e:
                raise RuntimeError(f"failed to use config {e}")

        elif instrument is not None:
            try:
                print('--> plugin_conf_file', plugin_conf_file)
                config = instrument.from_conf_file(plugin_conf_file)

            except Exception as e:
                raise RuntimeError(f"failed to use config {e}")

        else:

            raise SpiacsException(message='instrument cannot be None',
                                  debug_message='instrument se to None in SpiacsDispatcher __init__')

        try:
            _data_server_url = config.data_server_url
            _data_server_port = config.data_server_port

        except Exception as e:
            #    #print(e)

            print("ERROR->")
            raise RuntimeError(f"failed to use config {e}")

        self.config(_data_server_url, _data_server_port)

        logger.info("data_server_url: %s", self.data_server_url)

    def config(self, data_server_url, data_server_port=None):
        logger.info('configuring %s with %s', self, data_server_url)

        self.data_server_url = data_server_url
        
    def test_communication(self, max_trial=120, sleep_s=1, logger=None):
        logger.info('--> start test connection')

        query_out = QueryOutput()

        message = 'connection OK'
        debug_message = ''

        query_out.set_done(message=message, debug_message=str(debug_message))

        logger.info('--> end test busy')

        return query_out

    def test_has_input_products(self, instrument, logger=None):

        query_out = QueryOutput()

        message = 'OK'
        debug_message = 'OK'

        query_out.set_done(message=message, debug_message=str(debug_message))

        return query_out, [1]

    def _run_test(self, t1=1482049941, t2=1482049941+100, dt=0.1, e1=10, e2=500):
        raise NotImplementedError

    def _run(self, data_server_url, param_dict):

        try:
            url = data_server_url.format(
                t0_isot=param_dict['t0_isot'],
                dt_s=param_dict['dt_s'],
            )
            logger.error('calling GET on %s', url)
            res = requests.get("%s" % (url), params=param_dict)

            if len(res.content) < 8000: # typical length to avoid searching in long strings, which can not be errors of this kind
                if 'this service are limited' in res.text or 'Over revolution' in res.text:
                    raise SpiacsAnalysisException(f"SPI-ACS backend refuses to process this request, due to resource constrain: {res.text}")

            logger.error('data server returned %s of len %s text: %s...', res, len(res.content), res.text[:500])

        except ConnectionError as e:

            raise SpiacsAnalysisException(
                f'Spiacs Analysis error: {e}')


        return res

    def run_query(self, call_back_url=None, run_asynch=False, logger=None, param_dict=None,):

        res = None
        message = ''
        debug_message = ''
        query_out = QueryOutput()

        if param_dict is None:
            param_dict = self.param_dict

        try:
            logger.info('--Spiacs disp--')
            logger.info('call_back_url %s', call_back_url)
            logger.info('data_server_url %s', self.data_server_url)
            logger.info('*** run_asynch %s', run_asynch)

            res = self._run(self.data_server_url, param_dict)
            
            # DONE
            query_out.set_done(message=message, debug_message=str(
                debug_message), job_status='done')

            # job.set_done()

        except SpiacsAnalysisException as e:

            run_query_message = f'Spiacs AnalysisException in run_query: {e}'
            debug_message = e.debug_message

            query_out.set_failed('run query ',
                                 message='run query message=%s' % run_query_message,
                                 logger=logger,
                                 excep=e,
                                 job_status='failed',
                                 e_message=run_query_message,
                                 debug_message=debug_message)

            raise SpiacsException(message=run_query_message,
                                  debug_message=debug_message)

        except Exception as e:
            logger.exception('Spiacs UnknownException: %s', e)
            run_query_message = 'Spiacs UnknownException in run_query'
            query_out.set_failed('run query ',
                                 message='run query message=%s' % run_query_message,
                                 logger=logger,
                                 excep=e,
                                 job_status='failed',
                                 e_message=run_query_message,
                                 debug_message=e)

            raise SpiacsUnknownException(
                message=run_query_message, debug_message=e)

        return res, query_out
