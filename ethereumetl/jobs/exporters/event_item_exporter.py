from blockchainetl.jobs.exporters.composite_item_exporter import CompositeItemExporter

FIELDS_TO_EXPORT = [
    'contract_address',
    'transaction_hash',
    'log_index',
    'block_number',
    'event_type'
]


def event_item_exporter(event_name, event_output, add_fields_to_export):
    FIELDS_TO_EXPORT_ADDED = FIELDS_TO_EXPORT + add_fields_to_export
    print("FIELDS_TO_EXPORT_ADDED")
    print(FIELDS_TO_EXPORT_ADDED)
    return CompositeItemExporter(
        filename_mapping={
            event_name: event_output
        },
        field_mapping={
            event_name: FIELDS_TO_EXPORT_ADDED
        }
    )
