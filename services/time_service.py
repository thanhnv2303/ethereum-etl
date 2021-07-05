def round_timestamp_to_date(timestamp):
    timestamp_a_day = 86400
    timestamp_unit_day = timestamp / timestamp_a_day
    recover_to_unit_second = int(timestamp_unit_day) * timestamp_a_day
    return recover_to_unit_second
