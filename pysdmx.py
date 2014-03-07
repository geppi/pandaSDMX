#! /usr/bin/env python3
# -*- coding: utf-8 -*-
""" Python interface to SDMX """

import requests
import pandas
import lxml.etree
import uuid
import datetime
import numpy


def date_parser(date, frequency):
    if frequency == 'A':
        return datetime.datetime.strptime(date, '%Y')
    if frequency == 'Q':
        date = date.split('Q')
        date = str(int(date[1])*3) + date[0]
        return datetime.datetime.strptime(date, '%m%Y')
    if frequency == 'M':
        return datetime.datetime.strptime(date, '%YM%m')


def query_rest(url):
    request = requests.get(url)
    parser = lxml.etree.XMLParser(
        ns_clean=True, recover=True, encoding='utf-8')
    return lxml.etree.fromstring(
        request.text.encode('utf-8'), parser=parser)


class Data(object):
    def __init__(self, SDMXML):
        self.tree = SDMXML
        self._time_series = None

    @property
    def time_series(self):
        if not self._time_series:
            self._time_series = {}
            for series in self.tree.iterfind(".//generic:Series",
                                             namespaces=self.tree.nsmap):
                codes = {}
                for key in series.iterfind(".//generic:Value",
                                           namespaces=self.tree.nsmap):
                    codes[key.get('id')] = key.get('value')
                time_series_ = []
                for observation in series.iterfind(".//generic:Obs",
                                                   namespaces=self.tree.nsmap):
                    dimensions = observation.xpath(".//generic:ObsDimension",
                                                   namespaces=self.tree.nsmap)
                    dimension = dimensions[0].values()
                    dimension = date_parser(dimension[0], codes['FREQ'])
                    values = observation.xpath(".//generic:ObsValue",
                                               namespaces=self.tree.nsmap)
                    value = values[0].values()
                    value = value[0]
                    observation_status = 'A'
                    for attribute in \
                        observation.iterfind(".//generic:Attributes",
                                             namespaces=self.tree.nsmap):
                        for observation_status_ in \
                            attribute.xpath(
                                ".//generic:Value[@id='OBS_STATUS']",
                                namespaces=self.tree.nsmap):
                            if observation_status_ is not None:
                                observation_status \
                                    = observation_status_.get('value')
                    time_series_.append((dimension, value, observation_status))
                time_series_.sort()
                dates = numpy.array(
                    [observation[0] for observation in time_series_])
                values = numpy.array(
                    [observation[1] for observation in time_series_])
                time_series_ = pandas.Series(values, index=dates)
                self._time_series[str(uuid.uuid1())] = (codes, time_series_)
        return self._time_series


class Dataflows(object):
    def __init__(self, SDMXML):
        self.tree = SDMXML
        self._all_dataflows = None

    @property
    def all_dataflows(self):
        if not self._all_dataflows:
            self._all_dataflows = {}
            for dataflow in self.tree.iterfind(".//str:Dataflow",
                                               namespaces=self.tree.nsmap):
                id = dataflow.get('id')
                agencyID = dataflow.get('agencyID')
                version = dataflow.get('version')
                titles = {}
                for title in dataflow.iterfind(".//com:Name",
                                               namespaces=self.tree.nsmap):
                    language = title.values()
                    language = language[0]
                    titles[language] = title.text
                self._all_dataflows[id] = (agencyID, version, titles)
        return self._all_dataflows


class SDMX_REST(object):
    def __init__(self, sdmx_url, agencyID):
        self.sdmx_url = sdmx_url
        self.agencyID = agencyID
        self._dataflow = None

    @property
    def dataflow(self):
        if not self._dataflow:
            resource = 'dataflow'
            resourceID = 'all'
            version = 'latest'
            url = (self.sdmx_url + '/'
                   + resource + '/'
                   + self.agencyID + '/'
                   + resourceID + '/'
                   + version)
            self._dataflow = Dataflows(query_rest(url))
        return self._dataflow

    def data_extraction(self, flowRef, key, startperiod, endperiod):
        resource = 'data'
        url = (self.sdmx_url + '/'
               + resource + '/'
               + flowRef + '/'
               + key
               + '?startperiod=' + startperiod
               + '&endPeriod=' + endperiod)
        return Data(query_rest(url))


eurostat = SDMX_REST('http://www.ec.europa.eu/eurostat/SDMX/diss-web/rest',
                     'ESTAT')
