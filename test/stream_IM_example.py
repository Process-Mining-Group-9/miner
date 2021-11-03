#discover a DFG from a stream of events
from pm4py.streaming.stream.live_event_stream import LiveEventStream
live_event_stream = LiveEventStream()

from pm4py.streaming.algo.discovery.dfg import algorithm as dfg_discovery
streaming_dfg = dfg_discovery.apply()

live_event_stream.register(streaming_dfg)
live_event_stream.start()

import os
from pm4py.objects.log.importer.xes import importer as xes_importer
log = xes_importer.apply(os.path.join("input_data", "running-example.xes"))

from pm4py.objects.conversion.log import converter as stream_converter
static_event_stream = stream_converter.apply(log, variant=stream_converter.Variants.TO_EVENT_STREAM)

for ev in static_event_stream:
    live_event_stream.append(ev)

live_event_stream.stop()
dfg, activities, sa, ea = streaming_dfg.get()

from pm4py.visualization.dfg import visualizer as dfg_visualization
#display frequency
gviz = dfg_visualization.apply(dfg, log=log, variant=dfg_visualization.Variants.FREQUENCY)
dfg_visualization.view(gviz)

#converte dfg to process tree
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
from pm4py.visualization.process_tree import visualizer as pt_visualizer
from pm4py.objects.conversion.dfg import converter as dfg_converter

#tree = dfg_converter.apply(dfg)

#gviz = pt_visualizer.apply(tree)
#pt_visualizer.view(gviz)

#convert process tree to petri net
from pm4py.objects.conversion.process_tree import converter as pt_converter
from pm4py.visualization.petri_net import visualizer as pn_visualizer
from pm4py.objects.conversion.dfg import converter as dfg_converter
net, initial_marking, final_marking = dfg_converter.apply(dfg, variant=dfg_converter.Variants.VERSION_TO_PETRI_NET_INVISIBLES_NO_DUPLICATES)
gviz = pn_visualizer.apply(net, initial_marking, final_marking )
pn_visualizer.view(gviz)
