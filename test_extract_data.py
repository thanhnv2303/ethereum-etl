import json

from web3 import Web3

from ethereumetl.cli import export_events, export_all
from ethereumetl.jobs.export_events_job import ExportEventsJob
from ethereumetl.jobs.exporters.event_item_exporter import event_item_exporter
from ethereumetl.providers.auto import get_provider_from_uri
from ethereumetl.thread_local_proxy import ThreadLocalProxy

provider = "http://25.19.185.225:8545"
provider_uri = "http://25.19.185.225:8545"
provider_uri = "https://bsc-dataseed.binance.org/"
start = "6895623"
end = "6895625"
start_block = 6895566
end_block = start_block + 100
partition_batch_size = 10000
batch_size = 100
output = "./data"
output_exporter = output + "/events.csv"
max_workers = 5
export_batch_size = 100
subscriber_event_file = "./examples/mint_event.json"
tokens = ''

# export_all(start=3000000, end=3000100, partition_batch_size=10000, output_dir="./data", max_workers=5,
#            provider_uri=provider)
# # export_all(start=3000000, end=3000100, partition_batch_size=10000, output_dir="./data", max_workers=5,
# #            provider_uri=provider)
#
# export_token_transfers(start_block=start, end_block=end, batch_size=batch_size, output=output_dir, max_workers=max_workers, provider_uri=provider_uri)
# export_events(start, end, batch_size, output_dir, max_workers, provider_uri, "",subscriber_event_file)
#
# export_events(start_block=start, end_block=end, batch_size=batch_size, output=output_exporter, max_workers=max_workers,
#               provider_uri=provider_uri,
#               subscriber_event_file=subscriber_event_file)

with open(subscriber_event_file) as json_file:
    subscriber_event = json.load(json_file)
event_name = subscriber_event.get("name")
inputs = subscriber_event.get("inputs")
add_fields_to_export = []
for input in inputs:
    if input:
        add_fields_to_export.append(input.get("name"))

output_exporter = output + "/events.csv"
job = ExportEventsJob(
    start_block=start_block,
    end_block=end_block,
    batch_size=batch_size,
    web3=ThreadLocalProxy(lambda: Web3(get_provider_from_uri(provider_uri))),
    item_exporter=event_item_exporter(event_name,output_exporter, add_fields_to_export),
    max_workers=max_workers,
    subscriber_event=subscriber_event,
    tokens=tokens)
job.run()
