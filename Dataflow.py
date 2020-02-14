#!/usr/bin/python3
# -*- coding: utf-8 -*-
# Copyright (c) 2020 digitX Gbr. All rights reserved.
# Author: Thomas Geppert



import attr
import numpy as np
import pandas as pd
import pandasdmx as sdmx


@attr.s
class Dataflow():
    
    id       = attr.ib(validator=attr.validators.instance_of(str))
    provider = attr.ib(validator=attr.validators.instance_of(str))
    
    def __attrs_post_init__(self):
        # retrieve the dataflow definition
        if self.provider not in sdmx.list_sources():
            raise ValueError("No preconfigured request parameters for provider %s available." % self.provider)
        self.source = sdmx.Request(self.provider)
        try:
            self.flow_msg = self.source.dataflow(self.id)
        except:
            raise ValueError("Retrieval of metadata for dataflow %s failed." % self.id)
        self.flow = self.flow_msg.dataflow[self.id]
        self.name = self.flow.name.localized_default('de')
        
        # retrieve the data structure definition
        try:
            self.struct_msg = self.source.datastructure(self.flow.structure.id)
        except:
            raise RuntimeError("Retrieval of DSD for dataflow %s failed." % self.id)
        # let's assure that we have exactly one DSD for this dataflow
        if len(self.struct_msg.structure) != 1 : raise RuntimeError("Number of DSDs retrieved is %d but expected exactly 1." % len(self.struct_msg.structure))
        # as well as exacltly one concept_scheme
        if len(self.struct_msg.concept_scheme) != 1 : raise RuntimeError("Number of Concept Schemes retrieved is %d but expected exactly 1." % len(self.struct_msg.concept_scheme))
        
        self.dsd  = self.struct_msg.structure[0]
        self.concept_scheme = sdmx.to_pandas(self.struct_msg.concept_scheme[0])
        self.codelists = sdmx.to_pandas(self.struct_msg.codelist)
        
        self.dimensions = pd.DataFrame({'Concept' : [self.concept_scheme.get(dim.id, dim.id) for dim in self.dsd.dimensions], 'Codelist' : [dim.local_representation.enumerated.id if dim.local_representation.enumerated else np.nan for dim in self.dsd.dimensions]}, index=[dim.id for dim in self.dsd.dimensions])
        self.dimensions.index.name = 'Dimension'
        
        # our request for the data structure definition might have returned alternative dataflows for constrained datasets
        self.alt_flows = self.struct_msg.dataflow
        if len(self.alt_flows) > 1: self.has_alt_flows = True
        else: self.has_alt_flows = False        
        self._contents = None
        
    @property
    def contents(self):
        if self._contents is None: self._contents = self.source.series_keys(self.id)
        return sdmx.to_pandas(list(self._contents))
    
    def code2speaking(self, contents):
        if not isinstance(contents, pd.DataFrame):
            raise TypeError("Expected argument type <class 'pandas.core.frame.DataFrame'> but received %s." % type(contents))
        return pd.DataFrame({self.concept_scheme[col] : [self.codelists[self.dimensions.loc[col]['Codelist']][code] for code in contents[col]] for col in contents.columns})
    
    @property
    def contents_speaking(self):
        if self._contents is None: self._contents = self.source.series_keys(self.id)
        return self.code2speaking(sdmx.to_pandas(list(self._contents)))
        
    def use_flow(self, new_id):
        if new_id not in self.alt_flows:
            raise ValueError("Invalid id %s for alternative dataflow" % new_id)
        if new_id != self.id:
            self.id = new_id
            self.flow = self.alt_flows[self.id]
            self.name = self.flow.name.localized_default('de')
            self._contents = None
