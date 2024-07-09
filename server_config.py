config = {
    "broker_IP":broker_IP,
    "port_Num":port_Num,
    "verdict_min_refresh_time": 0.5, # Min number of seconds before a new verdict can be submitted
    "oldest_allowable_data": 2.5, # Max number of seconds before data is considered too old
    "show_verbose_output": True,
    "reputation_increment": 0.005, # Amount to increment or decrement client reputation by when they make a right decision
    "reputation_decrement": 0.010, # Amount to decrement client reputation by when they make a wrong decision
    "min_reputation": 0.35, # Minimum reputation value
}