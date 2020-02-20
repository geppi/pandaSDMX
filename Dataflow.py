#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Copyright (c) 2020 digitX Gbr. All rights reserved.
# Author: Thomas Geppert



import attr
import numpy as np
import pandas as pd
import pandasdmx as sdmx



def list_providers():
    return sdmx.list_sources()


@attr.s
class Dataflow():
    
    id       = attr.ib(validator=attr.validators.instance_of(str))
    provider = attr.ib(validator=attr.validators.instance_of(str))
    agency   = attr.ib(default = None, validator=attr.validators.instance_of((str, type(None))))
    
    def __attrs_post_init__(self):
        # retrieve the dataflow definition
        if self.provider not in list_providers():
            raise ValueError("No preconfigured request parameters for provider %s available." % self.provider)
        self.source = sdmx.Request(self.provider)
        if self.agency is None: self.agency = self.provider
        try:
            self.flow_msg = self.source.dataflow(self.id, provider=self.agency)
        except:
            raise RuntimeError("Retrieval of metadata for dataflow %s failed." % self.id)
        self.flow = self.flow_msg.dataflow[self.id]
        self.name = self.flow.name.localized_default('de')
        
        # retrieve the data structure definition
        if len(self.flow_msg.concept_scheme) == 0: dsd_maintainer = self.provider
        else: dsd_maintainer = self.flow_msg.concept_scheme[0].maintainer.id
        try:
            self.struct_msg = self.source.datastructure(self.flow.structure.id, provider=dsd_maintainer)
        except:
            raise RuntimeError("Retrieval of DSD for dataflow %s failed." % self.id)
        # let's assure that we have exactly one DSD for this dataflow
        if len(self.struct_msg.structure) != 1 : raise RuntimeError("Number of DSDs retrieved is %d but expected exactly 1." % len(self.struct_msg.structure))
        # as well as exacltly one concept_scheme
        if len(self.struct_msg.concept_scheme) != 1 : raise RuntimeError("Number of Concept Schemes retrieved is %d but expected exactly 1." % len(self.struct_msg.concept_scheme))
        
        self.dsd  = self.struct_msg.structure[0]
        self.concept_scheme = sdmx.to_pandas(self.struct_msg.concept_scheme[0])
        self.codelists = sdmx.to_pandas(self.struct_msg.codelist)
        
        # generate the codemap for decoding dimension and attribute values to describing text
        self.codemap = {}
        for dim in self.dsd.dimensions.components:
            if dim.local_representation.enumerated is None: continue
            cl = dim.local_representation.enumerated.id
            d = {key : value for key, value in self.codelists[cl].items()}
            d[dim.id] = self.concept_scheme[dim.id]
            self.codemap[dim.id] = d
        for attr in self.dsd.attributes.components:
            if attr.local_representation.enumerated is None: continue
            cl = attr.local_representation.enumerated.id
            d = {key : value for key, value in self.codelists[cl].items()}
            d[attr.id] = self.concept_scheme[attr.id]
            self.codemap[attr.id] = d
                
        # our request for the data structure definition might have returned alternative dataflows for constrained datasets
        self.alt_flows = self.struct_msg.dataflow
        if len(self.alt_flows) > 1: self.has_alt_flows = True
        else: self.has_alt_flows = False        
        self._contents = None
        
    @property
    def contents(self):
        if self._contents is None: self._contents = self.source.series_keys(self.id)
        return sdmx.to_pandas(list(self._contents))
    
    def decode(self, dim, code=None):
        if not isinstance(dim, str):
            raise TypeError("Expected first argument type <class 'str'> but received %s." % type(dim))
        if code is None: code = dim
        elif not isinstance(code, str):
            raise TypeError("Expected second argument type <class 'str'> but received %s." % type(code))
        return self.codemap[dim][code]

    @property
    def contents_decoded(self):
        if self._contents is None: self._contents = self.source.series_keys(self.id)
        decoded = self.contents.copy()
        for col in decoded.columns:
            decoded[col] = decoded[col].map(self.codemap[col])
        decoded.columns = decoded.columns.map({dim : self.decode(dim) for dim in self.codemap})
        return decoded
        
    def use_flow(self, new_id):
        if new_id not in self.alt_flows:
            raise ValueError("Invalid id %s for alternative dataflow" % new_id)
        if new_id != self.id:
            self.id = new_id
            self.flow = self.alt_flows[self.id]
            self.agency = self.flow.maintainer.id
            self.name = self.flow.name.localized_default('de')
            self._contents = None


@attr.s(repr=False)
class DataflowDirectory():
    
    provider = attr.ib(validator=attr.validators.instance_of(str))
    
    def __attrs_post_init__(self):
        # retrieve all available dataflow definitions from this provider
        if not isinstance(self.provider, str):
            raise TypeError("Expected argument type <class 'str'> but received %s." % type(self.provider))
        if self.provider not in list_providers():
            raise ValueError("No preconfigured request parameters for provider %s available." % self.provider)
        source = sdmx.Request(self.provider)
        base_url = source.dataflow(dry_run=True).url.split('dataflow')[0]
        try:
            self._flow_msg = source.dataflow(url=base_url + 'dataflow')
        except:
            try:
                self._flow_msg = source.dataflow(url=base_url + 'dataflow/all/all/latest')
            except:
                try:
                    self._flow_msg = source.dataflow()
                except:
                    raise RuntimeError("Retrieval of dataflows for provider %s failed." % self.provider)
        self.flows = sdmx.to_pandas(self._flow_msg.dataflow)
        self.flows.sort_index(inplace=True)
        self.selection = False
        
    def __repr__(self):
        if self.selection: flow_spec = ', ***SELECTED*** Dataflows: flows=\n'
        else: flow_spec = ", ***ALL*** available Dataflows: flows=\n"
        return "DataflowDirectory(provider=" + self.provider.__repr__() + flow_spec + self.flows.__repr__()
        
    def find(self, pattern):
        if not isinstance(pattern, str):
            raise TypeError("Expected argument type <class 'str'> but received %s." % type(pattern))
        hits = self.flows.where(self.flows.str.contains(pattern)).dropna()
        if len(hits) > 0:
            self.flows = hits
            self.selection = True
    
    def reset(self):
        self.flows = sdmx.to_pandas(self._flow_msg.dataflow)
        self.flows.sort_index(inplace=True)
        self.selection = False
        
    def get(self, flow_id=None):
        if flow_id is not None:
            if not isinstance(flow_id, str):
                raise TypeError("Expected argument type <class 'str'> but received %s." % type(flow_id))
            if flow_id not in self._flow_msg.dataflow:
                raise ValueError("Dataflow with id %s was not found on provider %s." % (flow_id, self.provider))
        else:
            if len(self.flows) != 1:
                raise ValueError("Expected exactly one dataflow to retrieve in selection\n" + \
                                 "but selection contains %d dataflow items." % len(self.flows))
            flow_id = list(self.flows.keys())[0]
        return Dataflow(flow_id, self.provider, self._flow_msg.dataflow[flow_id].maintainer.id)


