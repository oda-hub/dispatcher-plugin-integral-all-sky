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
import json
import re
from typing import List

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy
import os
import io

from cdci_data_analysis.analysis.parameters import Name

# Dependencies
# eg numpy
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


#import ddosaclient as dc

# Project
# relative import eg: from .mod import f
import numpy as np
from astropy.table import Table
from astropy import time
from astropy import units as u

from pathlib import Path

from astropy.io import fits as pf
from cdci_data_analysis.analysis.io_helper import FitsFile
from cdci_data_analysis.analysis.queries import LightCurveQuery
from cdci_data_analysis.analysis.products import LightCurveProduct, QueryProductList, QueryOutput
from cdci_data_analysis.analysis.io_helper import FilePath
from oda_api.data_products import NumpyDataProduct, NumpyDataUnit, BinaryData
from cdci_data_analysis.configurer import DataServerConf

from .spiacs_dataserver_dispatcher import SpiacsDispatcher
from .spiacs_dataserver_dispatcher import SpiacsAnalysisException

import traceback
import logging

logger = logging.getLogger('spiacs_dataserver_dispatcher')


class DummySpiacsRes(object):

    def __init__(self):
        pass


integral_mjdref = 51544.0

class SpicasLightCurve(LightCurveProduct):
    def __init__(self, name, file_name, data, header, prod_prefix=None, out_dir=None, src_name=None, meta_data={}):

        if meta_data == {} or meta_data is None:
            self.meta_data = {'product': 'spiacs_lc',
                              'instrument': 'spiacs', 'src_name': src_name}
        else:
            self.meta_data = meta_data

        self.meta_data['time'] = 'TIME'
        self.meta_data['rate'] = 'RATE'
        self.meta_data['rate_err'] = 'ERROR'

        super().__init__(name=name,
                         data=data,
                         name_prefix=prod_prefix,
                         file_dir=out_dir,
                         file_name=file_name,
                         meta_data=meta_data)

    @classmethod
    def build_from_res(cls,
                       res,
                       data_level,
                       src_name='',
                       prod_prefix='spiacs_lc',
                       out_dir=None,
                       delta_t=None):

        (res, res_ephs) = res

        lc_list = []

        if out_dir is None:
            out_dir = './'

        if prod_prefix is None:
            prod_prefix = ''

        file_name = src_name+'.fits'

        meta_data = {}
        meta_data['src_name'] = src_name

        res_text_stripped = res.text.replace(r"\n", "\n").strip('" \n\\n')
        res_ephs_text_stripped = re.sub(r"[\'\" \n\r]+", " ", res_ephs.text).strip('" \n\\n')

        for keyword in 'ZeroData', 'NoData':
            if keyword in res_text_stripped:
                raise SpiacsAnalysisException(
                    message=f'no usable data found for this time interval: server reports {keyword} (status {res.status_code}). Raw response: {res.text}')

        try:
            # [IJD] [seconds since reference] [counts in bin] [seconds since midnight]

            if data_level == 'ordinary':
                data = cls.parse_ordinary_data(res_text_stripped)
                comment = []
            else:
                data, comment = cls.parse_realtime_data(res_text_stripped)

            data, extra_meta_data = cls.reformat_and_rebin(data, delta_t)

            t_start = extra_meta_data.pop('t_start')
            t_stop = extra_meta_data.pop('t_stop')
            t_ref =  extra_meta_data.pop('t_ref')

            meta_data.update(extra_meta_data)

            logger.info("data mean: %s error mean %s", np.mean(data['RATE']), np.mean(data['ERROR']))
            

            header = {}
            header['EXTNAME'] = 'RATE'
            header['TIMESYS'] = 'TT'
            header['TIMEREF'] = 'LOCAL'
            header['ONTIME'] = t_stop - t_start
            header['TASSIGN'] = 'SATELLITE'

            Integral_jd = (t_ref.mjd-integral_mjdref)*u.day
            header['TSTART'] = Integral_jd.to('s').value + t_start
            header['TSTOP'] = Integral_jd.to('s').value + t_stop

            t1 = time.Time(t_start / 86400. + t_ref.value,
                           scale='tt', format='mjd')
            t2 = time.Time(t_start / 86400. + t_ref.value,
                           scale='tt', format='mjd')

            # TODO add comment  "Start time (UTC) of the light curve" now fits writer is failing
            header['DATE-OBS'] = '%s' % t1.isot
            # TODO add comment  "Start time (UTC) of the light curve" now fits writer is failing
            header['DATE-END'] = '%s' % t2.isot

            header['TIMEDEL'] = meta_data['time_bin']

            header['MJDREF'] = integral_mjdref

            header['TELESCOP'] = 'INTEGRAL'
            header['INSTRUME'] = 'SPI-ACS'
            #print ((t_ref.value*u.d).to('s'))
            header['TIMEZERO'] = (
                t_ref.value*u.d-integral_mjdref*u.d).to('s').value
            header['TIMEUNIT'] = 's '

            header['PROPHECY'] = comment
            header['EPHS'] = res_ephs_text_stripped
            units_dict = {}

            units_dict['RATE'] = 'count/s'
            units_dict['ERROR'] = 'count/s'
            units_dict['TIME'] = 's'

            logger.info("data std: %s", np.std(data['RATE']/data['ERROR']))

            npd = NumpyDataProduct(data_unit=NumpyDataUnit(data=data,
                                                           name='RATE',
                                                           data_header=header,
                                                           hdu_type='bintable',
                                                           units_dict=units_dict),
                                   meta_data=meta_data)


            lc = cls(name=src_name, data=npd, header=None, file_name=file_name, out_dir=out_dir,
                     prod_prefix=prod_prefix,
                     src_name=src_name, meta_data=meta_data)

            lc_list.append(lc)

        except Exception as e:
            logger.info(traceback.format_exc())

            raise SpiacsAnalysisException(
                message='spiacs light curve failed: %s' % e.__repr__(), debug_message=str(e))

        return lc_list


    @classmethod
    def parse_ordinary_data(cls, res_text_stripped):
        data = np.genfromtxt(
                    io.StringIO(res_text_stripped),
                    delimiter=" ",
                    usecols=[0, 2],
                    dtype=[('TIME_IJD', '<f8'), ('COUNTS', '<f8')])

        assert len(data['TIME_IJD']) > 100

        return data
                

    @classmethod
    def parse_realtime_data(cls, res_text_stripped):
        jdata = json.loads(res_text_stripped)

        cn = jdata['lc']['columns']
        cd  = np.array(jdata['lc']['data'])

        data = np.zeros(cd.shape[0], dtype=[('TIME_IJD', '<f8'), ('COUNTS', '<f8')])

        # TODO: add tracked deviation from the accurate time from the past
        data['TIME_IJD'] = cd[:, cn.index('ijd')] 
        print("\033[32mdata['TIME_IJD']", data['TIME_IJD'], "\033[0m")
        data['COUNTS'] = cd[:, cn.index('counts')]

        assert len(data['TIME_IJD']) > 100            
        
        return data, jdata['prophecy']


    @classmethod
    def reformat_and_rebin(cls, data, delta_t):
        meta_data = {}

        dt_s = (data['TIME_IJD'][1:] - data['TIME_IJD'][:-1]) * 24 * 3600

        unique_dt_s, unique_dt_s_counts = np.unique(
            np.round(dt_s, 3), return_counts=True)
        i = np.argmax(unique_dt_s_counts)
        instr_t_bin = unique_dt_s[i]


        logging.info("deduced instr_t_bin: %s, fraction %s",
                        instr_t_bin, unique_dt_s_counts[i]/len(dt_s))

        t_ref = time.Time(
            (data['TIME_IJD'][0] + data['TIME_IJD'][-1]) / 2 + integral_mjdref,
            format='mjd')

        # IJD offset from MJD, https://heasarc.gsfc.nasa.gov/W3Browse/integral/intscw.html
        data = np.array(list(zip((data['TIME_IJD'] - t_ref.mjd + integral_mjdref) * 24 * 3600,
                                    data['COUNTS'] / instr_t_bin,
                                    data['COUNTS'] ** 0.5 / instr_t_bin
                                    )),
                        dtype=[('TIME', float),
                                ('RATE', float),
                                ('ERROR', float)]
                        )

        logger.info("\033[31m got raw time column: %s\033[0m", data['TIME'])
        logger.info("\033[31m got raw rate column: %s\033[0m", data['RATE'])
        logger.info("\033[31m got raw error column: %s\033[0m", data['ERROR'])

        if delta_t is not None:
            delta_t = int(delta_t/instr_t_bin)*instr_t_bin

        logger.info("\033[31m got delta_t %s, instr_t_bin %s\033[0m", delta_t, instr_t_bin)

        if delta_t is None:
            meta_data['time_bin'] = instr_t_bin
        else:
            meta_data['time_bin'] = delta_t

        t_start = data['TIME'][0]
        t_stop = data['TIME'][-1] + instr_t_bin
        if delta_t is not None and delta_t > instr_t_bin:

            t1 = data['TIME'][0]
            t2 = data['TIME'][-1] + instr_t_bin

            digitized_ids = np.digitize(
                data['TIME'], np.arange(t1, t2, delta_t))

            binned_data = np.zeros(np.unique(digitized_ids).size, dtype=[
                                    ('TIME', '<f8'), ('RATE', '<f8'), ('ERROR', '<f8')])
            _t_frac = np.zeros(binned_data.size)
            for ID, binned_id in enumerate(np.unique(digitized_ids)):

                msk = digitized_ids == binned_id
                _t_frac[ID] = msk.sum()*instr_t_bin
                binned_data['RATE'][ID] = np.mean(data['RATE'][msk])
                binned_data['TIME'][ID] = np.mean(data['TIME'][msk])
                binned_data['ERROR'][ID] = np.sqrt(binned_data['RATE'][ID]*_t_frac[ID])/_t_frac[ID]
            
            logger.info('binned data RATE %s', binned_data['RATE'])
            logger.info('binned data RATE_ERROR %s', binned_data['ERROR'])

            data = binned_data

            logger.info('binned rate')
        else:
            logger.info('raw rate')
            data['RATE'] = data['RATE'] 
            data['ERROR'] = np.sqrt(data['RATE'] * instr_t_bin) / instr_t_bin

        meta_data['t_start'] = t_start
        meta_data['t_stop'] = t_stop
        meta_data['t_ref'] = t_ref

        return data, meta_data




class SpiacsLightCurveQuery(LightCurveQuery):

    def __init__(self, name):

        data_level = Name(name_format='str', name='data_level', value="ordinary")
        data_level._allowed_values = ["ordinary", "realtime"]

        super(SpiacsLightCurveQuery, self).__init__(name, parameters_list=[data_level])

    def build_product_list(self, instrument, res, out_dir, prod_prefix='spiacs_lc', api=False):
        src_name = 'query'

        T1 = instrument.get_par_by_name('T1')._astropy_time
        T2 = instrument.get_par_by_name('T2')._astropy_time
        
        delta_t = instrument.get_par_by_name('time_bin')._astropy_time_delta.sec
        data_level = instrument.get_par_by_name('data_level').value

        prod_list = SpicasLightCurve.build_from_res(res,
                                                    data_level=data_level,
                                                    src_name=src_name,
                                                    prod_prefix=prod_prefix,
                                                    out_dir=out_dir,
                                                    delta_t=delta_t)
        return prod_list

    def get_data_server_query(self, instrument,
                              config=None):

        T1 = instrument.get_par_by_name('T1')._astropy_time
        T2 = instrument.get_par_by_name('T2')._astropy_time

        data_level = instrument.get_par_by_name('data_level').value

        delta_t = T2 - T1
        delta_t_s = delta_t.sec * 0.5
        T_ref = time.Time((T2.mjd + T1.mjd) * 0.5, format='mjd').isot

        param_dict = self.set_instr_dictionaries(T_ref, delta_t_s, data_level)

        q = SpiacsDispatcher(instrument=instrument,
                             config=config,
                             param_dict=param_dict)

        return q

    def set_instr_dictionaries(self, T_ref, delta_t_s, data_level):
        return dict(
            t0_isot=T_ref,
            dt_s=delta_t_s,
            data_level=data_level
        )

    def process_product_method(self, instrument, prod_list, api=False):

        _names = []
        _lc_path = []
        # _root_path=[]
        _html_fig = []

        _data_list = []
        _binary_data_list = []
        for query_lc in prod_list.prod_list:
            # TODO: why is _current_par_dic only used here? Does base dispatcher need to support this?
            query_lc.add_url_to_fits_file(
                instrument._current_par_dic, url=instrument.disp_conf.products_url)
            query_lc.write()
            if api == False:
                _names.append(query_lc.name)
                _lc_path.append(str(query_lc.file_path.name))
                # x_label='MJD-%d  (days)' % mjdref,y_label='Rate  (cts/s)'
                du = query_lc.data.get_data_unit_by_name('RATE')
                dx = np.zeros(du.data['TIME'].shape) + du.header['TIMEDEL'] / 2.
                _html_fig.append(query_lc.get_html_draw(x=du.data['TIME'],
                                                        dx=dx,
                                                        y=du.data['RATE'],
                                                        dy=du.data['ERROR'],
                                                        title='Start Time: %s' % instrument.get_par_by_name(
                                                            'T1')._astropy_time.utc.value,
                                                        x_label='Time  (s)',
                                                        y_label='Rate  (cts/s)'))

            if api == True:
                _data_list.append(query_lc.data)


        query_out = QueryOutput()

        if api == True:
            query_out.prod_dictionary['numpy_data_product_list'] = _data_list
            query_out.prod_dictionary['binary_data_product_list'] = _binary_data_list
        else:
            query_out.prod_dictionary['name'] = _names
            query_out.prod_dictionary['file_name'] = _lc_path
            #query_out.prod_dictionary['root_file_name'] = _root_path
            query_out.prod_dictionary['image'] = _html_fig
            query_out.prod_dictionary['download_file_name'] = 'light_curves.tar.gz'

        query_out.prod_dictionary['prod_process_message'] = ''

        return query_out

    def get_dummy_products(self, instrument, config, out_dir='./', prod_prefix='spiacs', api=False):
        config = DataServerConf.from_conf_dict(
            instrument.data_server_conf_dict)

        meta_data = {'product': 'light_curve',
                     'instrument': 'isgri', 'src_name': ''}

        meta_data['query_parameters'] = self.get_parameters_list_as_json()
        data_level = instrument.get_par_by_name('data_level').value

        dummy_cache = config.dummy_cache

        res = (DummySpiacsRes(), DummySpiacsRes())
        with open('%s/query_spiacs_lc.txt' % dummy_cache, 'r') as file:
            text = str(file.read())

        res[0].__setattr__('text', text)
        res[1].__setattr__('text', text)

        res[0].__setattr__('status_code', 200)
        res[1].__setattr__('status_code', 200)

        prod_list = SpicasLightCurve.build_from_res(res,
                                                    data_level=data_level,
                                                    src_name='lc',
                                                    prod_prefix=prod_prefix,
                                                    out_dir=out_dir)

        prod_list = QueryProductList(prod_list=prod_list)
        #
        return prod_list


    def check_query_roles(self, provided_roles: List[str], par_dic: dict):
        needed_roles = []
        needed_roles_with_comments = {}

        data_class = par_dic.get('data_level')

        if data_class == 'realtime':
            needed_roles.append('integral-realtime')
            needed_roles_with_comments['integral-realtime'] = "access to real time data requires special role"

        if all([needed_role in provided_roles for needed_role in needed_roles]):
            return dict(authorization=True, needed_roles=[])
        else:
            return dict(authorization=False, needed_roles=needed_roles,
                        needed_roles_with_comments=needed_roles_with_comments)

