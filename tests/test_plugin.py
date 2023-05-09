import requests
import time
import json
import logging
import jwt

import pytest
import numpy as np


logger = logging.getLogger(__name__)

default_parameters = dict(
    instrument='spi_acs',
    product='spi_acs_lc',
    RA='255.98655344',
    DEC='-37.84414224',
    T1='2023-03-25T20:27:40.0',
    T2='2023-03-25T20:32:15.0',
    time_bin=2,
    product_type='spi_acs_lc',
)


def construct_token(roles, dispatcher_test_conf, expires_in=5000):
    secret_key = dispatcher_test_conf['secret_key']

    default_exp_time = int(time.time()) + expires_in
    default_token_payload = dict(
        sub="mtm@mtmco.net",
        name="mmeharga",
        roles="general",
        exp=default_exp_time,
        tem=0,
        mstout=True,
        mssub=True
    )

    token_payload = {
        **default_token_payload,
        "roles": roles
    }

    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    if isinstance(encoded_token, bytes):
        encoded_token = encoded_token.decode()

    return encoded_token


def test_discover_plugin():
    import cdci_data_analysis.plugins.importer as importer

    assert 'dispatcher_plugin_integral_all_sky' in importer.cdci_plugins_dict.keys()



def test_default(dispatcher_live_fixture):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    c = requests.get(server + "/run_analysis",
                     params={
                         **default_parameters,
                         'query_status': 'new',                         
                         'query_type': 'Real'
                         }
    )

    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200

    assert jdata['job_status'] == 'done'
    

@pytest.mark.odaapi
def test_odaapi(dispatcher_live_fixture):
    import oda_api.api

    d = oda_api.api.DispatcherAPI(
        url=dispatcher_live_fixture)

    assert 'spi_acs' in d.get_instruments_list()

    # d.get_instrument_description() TODO: this fails without instrument?
    assert 'spi_acs_lc' in d.get_instrument_description('spi_acs')[
        0][1]['prod_dict']


@pytest.mark.odaapi
@pytest.mark.parametrize("data_level", ["realtime", "ordinary"])
@pytest.mark.parametrize("roles", ["integral-realtime", ""])
def test_odaapi_data(dispatcher_live_fixture, data_level, dispatcher_test_conf, roles):
    import oda_api.api

    params = {**default_parameters, 'data_level': data_level}
    params['token'] = construct_token(roles.split(","), dispatcher_test_conf)
        
    def get_products():
        product_spiacs = oda_api.api.DispatcherAPI(
            url=dispatcher_live_fixture).get_product(**params)

        product_spiacs_raw_bin = oda_api.api.DispatcherAPI(
            url=dispatcher_live_fixture).get_product(**{**params, 'time_bin': 0.05})
        
        return product_spiacs, product_spiacs_raw_bin
        
    if data_level == "realtime" and roles == "":
        with pytest.raises(Exception) as excinfo:
            get_products()

        assert 'Unfortunately, your priviledges are not sufficient' in str(excinfo.value)
    else:
        product_spiacs, product_spiacs_raw_bin = get_products()

        assert product_spiacs.spi_acs_lc_0_query.data_unit[1].header['INSTRUME'] == "SPI-ACS"
        assert product_spiacs_raw_bin.spi_acs_lc_0_query.data_unit[1].header['INSTRUME'] == "SPI-ACS"

        data = np.array(product_spiacs.spi_acs_lc_0_query.data_unit[1].data)
        data_raw_bin = np.array(product_spiacs_raw_bin.spi_acs_lc_0_query.data_unit[1].data)
        
        assert len(data) > 100
        assert len(data_raw_bin) > 100
        
        assert np.std((data['RATE'] - np.mean(data['RATE']))/data['ERROR']) < 1.5
        assert np.std((data_raw_bin['RATE'] - np.mean(data_raw_bin['RATE']))/data_raw_bin['ERROR']) < 1.5


@pytest.mark.odaapi
def test_odaapi_data_coherence(dispatcher_live_fixture, dispatcher_test_conf):
    import oda_api.api

    params = {**default_parameters, 'time_bin': 0.05}
    params['token'] = construct_token(["integral-realtime"], dispatcher_test_conf)
        
    product_spiacs = oda_api.api.DispatcherAPI(
        url=dispatcher_live_fixture).get_product(**params)

    product_spiacs_rt = oda_api.api.DispatcherAPI(
        url=dispatcher_live_fixture).get_product(**{**params, 'data_level': "realtime"})
        
    data = np.array(product_spiacs.spi_acs_lc_0_query.data_unit[1].data)
    data_rt = np.array(product_spiacs_rt.spi_acs_lc_0_query.data_unit[1].data)
        
    assert len(data) > 100
    assert len(data_rt) > 100

    dt_s = (data['TIME'] - data_rt['TIME']) * 24 * 3600
    print("dt min, max, mean, std", dt_s.min(), dt_s.max(), dt_s.mean(), dt_s.std())

    cc = np.correlate(data['RATE'], data_rt['RATE'], mode='valid')
    cc_offset = cc.argmax() - cc.shape[0]//2

    assert np.abs(cc_offset) < 1

    from matplotlib import pyplot as plt
    plt.plot(data['RATE'])
    plt.plot(data_rt['RATE'])
    plt.xlim(0, 10)
    plt.savefig("test_odaapi_data_coherence.png")
    
    
    assert (data['RATE'] - data_rt['RATE']).max() < 1e-5


def test_request_too_large(dispatcher_live_fixture):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)
    c = requests.get(server + "/run_analysis",
                     params={
                         **default_parameters,
                         'T2': '2004-03-16T00:03:15.0',
                         'query_status': 'new',                         
                         'query_type': 'Real',
                         }
    )

    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)
    assert c.status_code == 200

    assert jdata['job_status'] == 'failed'
    assert 'SPI-ACS backend refuses to process this request' in jdata['exit_status']['error_message']




@pytest.mark.parametrize("roles", ["integral-realtime", ""])
def test_realtime(dispatcher_live_fixture, dispatcher_test_conf, roles):
    server = dispatcher_live_fixture

    logger.info("constructed server: %s", server)

    params = {
                **default_parameters,
                'query_status': 'new',
                'query_type': 'Real',
                'data_level': 'realtime'
            }

    params['token'] = construct_token(roles.split(","), dispatcher_test_conf)

    c = requests.get(server + "/run_analysis",
                     params=params)

    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(json.dumps(jdata, indent=4, sort_keys=True))
    logger.info(jdata)

    print(jdata)

    if roles:
        assert jdata['job_status'] == 'done'
        assert c.status_code == 200
        print(jdata['products']['analysis_parameters'])
    else:
        assert jdata['job_status'] == 'failed'
        assert c.status_code == 403

