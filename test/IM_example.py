import os
from pm4py.objects.log.importer.xes import importer as xes_importer

log = xes_importer.apply(os.path.join("input_data","running-example.xes"))

#create dfg & activities from log
from pm4py import discovery as dfg_discovery
dfg, start_activities, end_activities = dfg_discovery.discover_dfg(log)
#activities=dfg.get(activities)
#print(activities)

from pm4py.algo.filtering.log.attributes import attributes_filter
activities = attributes_filter.get_attribute_values(log, "concept:name")
resources = attributes_filter.get_attribute_values(log, "org:resource")
#print(activities)

from pm4py.visualization.dfg import visualizer as dfg_visualization
#display frequency
gviz = dfg_visualization.apply(dfg, log=log, variant=dfg_visualization.Variants.FREQUENCY)
dfg_visualization.view(gviz)

#display time
#dfg = dfg_discovery.apply(log, variant=dfg_discovery.Variants.PERFORMANCE)
#gviz = dfg_visualization.apply(dfg, log=log, variant=dfg_visualization.Variants.PERFORMANCE)
#dfg_visualization.view(gviz)

#inductive miner(dfg->petri_net)
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.visualization.process_tree import visualizer as pt_visualizer

net, initial_marking, final_marking = inductive_miner.apply_dfg(dfg,start_activities,end_activities,activities)

#gviz = pt_visualizer.apply(tree)
#pt_visualizer.view(gviz)


from pm4py.objects.conversion.process_tree import converter as pt_converter
from pm4py.visualization.petri_net import visualizer as pn_visualizer

#net, initial_marking, final_marking = pt_converter.apply(tree, variant=pt_converter.Variants.TO_PETRI_NET)
gviz = pn_visualizer.apply(net, initial_marking, final_marking )
pn_visualizer.view(gviz)
