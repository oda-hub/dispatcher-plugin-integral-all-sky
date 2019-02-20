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

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy
import os

# Dependencies
# eg numpy
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


import ddosaclient as dc

# Project
# relative import eg: from .mod import f
import  numpy as np
import pandas as pd
from astropy.table import Table
from astropy import time
from pathlib import Path

from astropy.io import fits as pf
from cdci_data_analysis.analysis.io_helper import FitsFile
from cdci_data_analysis.analysis.queries import LightCurveQuery
from cdci_data_analysis.analysis.products import LightCurveProduct,QueryProductList,QueryOutput
from cdci_data_analysis.analysis.io_helper import FilePath
from oda_api.data_products import NumpyDataProduct,NumpyDataUnit,BinaryData

from .spiacs_dataserver_dispatcher import SpiacsDispatcher
from .spiacs_dataserver_dispatcher import  SpiacsAnalysisException


class SpicasLigthtCurve(LightCurveProduct):
    def __init__(self,name,file_name,data,header,prod_prefix=None,out_dir=None,src_name=None,meta_data={}):


        if meta_data == {} or meta_data is None:
            self.meta_data = {'product': 'spiacs_lc', 'instrument': 'spiacs', 'src_name': src_name}
        else:
            self.meta_data = meta_data

        self.meta_data['time'] = 'time'
        self.meta_data['rate'] = 'rate'
        self.meta_data['rate_err'] = 'rate_err'



        super(LightCurveProduct, self).__init__(name=name,
                                               data=data,
                                               name_prefix=prod_prefix,
                                               file_dir=out_dir,
                                               file_name=file_name,
                                               meta_data=meta_data)


    @classmethod
    def build_from_res(cls,
                             res,
                             src_name='',
                             prod_prefix='spiacs_lc',
                             out_dir=None,
                             delta_t=None):



        lc_list = []

        if out_dir is None:
            out_dir = './'

        if prod_prefix is None:
            prod_prefix=''







        file_name =  src_name+'.fits'
        print ('file name',file_name)

        meta_data={}
        meta_data['src_name'] = src_name

        df = res.content.splitlines()

        if len(df) <= 2:
            raise SpiacsAnalysisException(message='no data found for this time interval')

        if len(df) > 0 and  ('ZeroData' in df[0] or 'NoData' in df[0]):
            raise SpiacsAnalysisException(message='no data found for this time interval')


        try:




            instr_t_bin=float(df[1].split()[1])

            if delta_t is  None:
                meta_data['time_bin'] = instr_t_bin
            else:
                meta_data['time_bin']=delta_t


            data = np.zeros(len(df)-3, dtype=[('rate', '<f8'), ('rate_err', '<f8'), ('time', '<f8')])
            for ID,d in enumerate(df[2:-1]):
                t,r,_=d.split()
                data['rate'][ID]=float(r)
                data['time'][ID] = float(t)

            if delta_t is not None:
                delta_t=np.int(delta_t/instr_t_bin)

            if delta_t is not None and delta_t>instr_t_bin:

                t1=data['time'][0]
                t2=data['time'][-1]+instr_t_bin


                digitized_ids =np.digitize(data['time'],np.arange(t1,t2,delta_t))
                #print(t1,t2,delta_t,data['time'][0],data['time'][1],digitized_ids)

                binned_data = np.zeros(np.unique(digitized_ids).size, dtype=[('rate', '<f8'), ('rate_err', '<f8'), ('time', '<f8')])
                _t_frac = np.zeros(binned_data.size)
                for ID,binned_id in enumerate(np.unique(digitized_ids)):

                    msk=digitized_ids==binned_id
                    _t_frac[ID]=msk.sum()*instr_t_bin
                    binned_data['rate'][ID] = np.sum(data['rate'][msk])
                    binned_data['time'][ID] = np.mean(data['time'][msk])

                binned_data['rate']*=1.0/_t_frac
                binned_data['rate_err'] = np.sqrt(binned_data['rate']/_t_frac)
                data=binned_data

            else:
                data['rate'] = data['rate'] /instr_t_bin
                data['rate_err'] = np.sqrt(data['rate']/instr_t_bin)

            header={}
            header['EXTNAME']='RATE'

            npd = NumpyDataProduct(data_unit=NumpyDataUnit(data=data,
                                                           name='',
                                                           hdu_type='bintable'),
                                                           meta_data=meta_data,
                                                           data_header=header)

            lc = cls(name=src_name, data=npd, header=None, file_name=file_name, out_dir=out_dir,
                     prod_prefix=prod_prefix,
                     src_name=src_name, meta_data=meta_data)

            lc_list.append(lc)
        except Exception as e:



            raise SpiacsAnalysisException(message='spiacs light curve failed: %s'%e.__repr__(),debug_message=str(e))






        return lc_list



class SpiacsLightCurveQuery(LightCurveQuery):

    def __init__(self, name):

        super(SpiacsLightCurveQuery, self).__init__(name)

    def build_product_list(self, instrument, res, out_dir, prod_prefix='spiacs_lc',api=False):
        src_name = 'query'
        #T1 = instrument.get_par_by_name('T1')._astropy_time
        #T2 = instrument.get_par_by_name('T2')._astropy_time
        delta_t=instrument.get_par_by_name('time_bin')._astropy_time_delta.sec

        prod_list = SpicasLigthtCurve.build_from_res(res,
                                                      src_name=src_name,
                                                      prod_prefix=prod_prefix,
                                                      out_dir=out_dir,
                                                      delta_t=delta_t)

        # print('spectrum_list',spectrum_list)

        return prod_list


    def get_data_server_query(self, instrument,
                              config=None):


        src_name = 'query'
        T1=instrument.get_par_by_name('T1')._astropy_time
        T2=instrument.get_par_by_name('T2')._astropy_time

        delta_t=T2-T1
        delta_t=delta_t.sec*0.5
        T_ref=time.Time((T2.mjd + T1.mjd) * 0.5, format='mjd').isot
        param_dict=self.set_instr_dictionaries(T_ref,delta_t)

        print ('build here',config,instrument)
        q = SpiacsDispatcher(instrument=instrument,config=config,param_dict=param_dict)

        return q


    def set_instr_dictionaries(self, T_ref,delta_t):
        return  dict(
            requeststring='%s %s'%(T_ref,delta_t),
            submit="Submit",
            generate='ipnlc',
        )


    def process_product_method(self, instrument, prod_list,api=False):

        _names = []
        _lc_path = []
        _root_path=[]
        _html_fig = []

        _data_list=[]
        _binary_data_list=[]
        for query_lc in prod_list.prod_list:
            print('->name',query_lc.name)

            query_lc.write()
            if api == False:
                _names.append(query_lc.name)
                _lc_path.append(str(query_lc.file_path.name))
                #x_label='MJD-%d  (days)' % mjdref,y_label='Rate  (cts/s)'
                _html_fig.append(query_lc.get_html_draw(x=query_lc.data.data_unit[0].data['time'],
                                                        y=query_lc.data.data_unit[0].data['rate'],
                                                        dy=query_lc.data.data_unit[0].data['rate_err'],
                                                        title='Start Time: %s'%instrument.get_par_by_name('T1')._astropy_time.utc.value,
                                                        x_label='Time  (s)',
                                                        y_label='Rate  (cts/s)'))

            if api==True:
                _data_list.append(query_lc.data)
                #try:
                #    open(root_file_path.path, "wb").write(BinaryData().decode(res_json['root_file_b64']))
                #    lc.root_file_path = root_file_path
                #except:
                #    pass
                _d,md=BinaryData(str(query_lc.root_file_path)).encode()
                _binary_data_list.append(_d)

        query_out = QueryOutput()

        if api == True:
            query_out.prod_dictionary['numpy_data_product_list'] = _data_list
            query_out.prod_dictionary['binary_data_product_list'] = _binary_data_list
        else:
            query_out.prod_dictionary['name'] = _names
            query_out.prod_dictionary['file_name'] = _lc_path
            query_out.prod_dictionary['root_file_name'] = _root_path
            query_out.prod_dictionary['image'] =_html_fig
            query_out.prod_dictionary['download_file_name'] = 'light_curves.tar.gz'

        query_out.prod_dictionary['prod_process_message'] = ''


        return query_out

    def get_dummy_products(self, instrument, config, out_dir='./'):
        raise RuntimeError('method to implement')

        # src_name = instrument.get_par_by_name('src_name').value
        #
        # dummy_cache = config.dummy_cache
        # delta_t = instrument.get_par_by_name('time_bin')._astropy_time_delta.sec
        # print('delta_t is sec', delta_t)
        # query_lc = LightCurveProduct.from_fits_file(inf_file='%s/query_lc.fits' % dummy_cache,
        #                                             out_file_name='query_lc.fits',
        #                                             prod_name='isgri_lc',
        #                                             ext=1,
        #                                             file_dir=out_dir)
        # print('name', query_lc.header['NAME'])
        # query_lc.name=query_lc.header['NAME']
        # #if src_name is not None:
        # #    if query_lc.header['NAME'] != src_name:
        # #        query_lc.data = None
        #
        # prod_list = QueryProductList(prod_list=[query_lc])
        #
        # return prod_list













