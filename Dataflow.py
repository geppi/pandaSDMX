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
    
    provider = attr.ib(validator=attr.validators.instance_of(str))
    flow_id  = attr.ib(validator=attr.validators.instance_of(str))
    
    def __attrs_post_init__(self):
        if self.provider not in sdmx.list_sources():
            raise ValueError("No preconfigured request parameters for provider %s available." % self.provider)
        self.source = sdmx.Request(self.provider)
        try:
            self.flow_msg = self.source.dataflow(self.flow_id)
        except:
            raise ValueError("Retrieval of metadata for dataflow %s failed." % self.flow_id)
        
        # We need to check if the provider did respect the parameter references='all'
        # that is used by pandaSDMX in the request for a particular dataflow to
        # retrieved the DSD along with the DataflowDescription.
        # Some providers like Eurostat do not conform with the SDMX standard and
        # ignore the parameter forcing us to explicitely request the datastructure by it's id.
        if len(self.flow_msg.structure) == 0:
            try:
                self.struct_msg = self.source.datastructure(self.flow_msg.dataflow[self.flow_id].structure.id)
            except:
                raise RuntimeError("Retrieval of DSD for dataflow %s failed after provider %s ignored parameter references='all' in initial request for the dataflow." % (self.flow_id, self.provider))
        else: self.struct_msg = self.flow_msg
        # let's assure that we have exactly one DSD for this dataflow
        if len(self.struct_msg.structure) != 1 : raise RuntimeError("Number of DSDs retrieved is %d but expected exactly 1." % len(self.struct_msg.structure))
        # as well as exacltly one concept_scheme
        if len(self.struct_msg.concept_scheme) != 1 : raise RuntimeError("Number of Concept Schemes retrieved is %d but expected exactly 1." % len(self.struct_msg.concept_scheme))
        
        self.flow = self.flow_msg.dataflow[self.flow_id]
        self.dsd  = self.struct_msg.structure[0]
        
        self.concept_scheme = sdmx.to_pandas(self.struct_msg.concept_scheme[0])
        self.codelists = sdmx.to_pandas(self.struct_msg.codelist)
        
        self.dimensions = pd.DataFrame({'Concept' : [self.concept_scheme.loc[dim.id] for dim in self.dsd.dimensions], 'Codelist' : [dim.local_representation.enumerated.id if dim.local_representation.enumerated else np.nan for dim in self.dsd.dimensions]}, index=[dim.id for dim in self.dsd.dimensions])
        self.dimensions.index.name = 'Dimension'
