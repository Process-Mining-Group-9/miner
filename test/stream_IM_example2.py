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
#    print(ev)

live_event_stream.stop()
dfg, activities, sa, ea = streaming_dfg.get()

from pm4py.visualization.dfg import visualizer as dfg_visualization
#display frequency
gviz = dfg_visualization.apply(dfg, log=log, variant=dfg_visualization.Variants.FREQUENCY)
dfg_visualization.view(gviz)


#inductive miner(dfg->petri_net)
from pm4py.algo.discovery.inductive import algorithm as inductive_miner
net, initial_marking, final_marking = inductive_miner.apply_dfg(dfg,sa,ea,activities)

from pm4py.visualization.petri_net import visualizer as pn_visualizer
gviz = pn_visualizer.apply(net, initial_marking, final_marking )
pn_visualizer.view(gviz)
