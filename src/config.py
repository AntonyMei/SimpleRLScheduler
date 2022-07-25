"""
Yixuan Mei, 2022.07.22
This file contains configurations for this scheduler.
"""

# master location
master_ip = "10.200.3.112"
master_port_inside = 4000
master_port_outside = 12000

# worker location
worker_ip_list = ["10.200.13.19",
                  "10.200.3.48"]
worker_port_inside = 4000
worker_port_outside = 12000

# trainer location
master2trainer_port = 4001
trainer2master_port = 4002
