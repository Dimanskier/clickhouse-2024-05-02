from infi.clickhouse_orm import Database

import time
import psutil
import datetime

from clients_examples.models import CpuStats, CpuStatsBuffer

db = Database('default')
db.create_table(CpuStatsBuffer)

while True:
    time.sleep(1)
    timestamp = datetime.datetime.now()
    stats = psutil.cpu_percent(percpu=True)
    print(stats)
    result = []
    for i in range(len(stats)):
        cpu_stat = CpuStatsBuffer(timestamp=timestamp, cpu_id=i, cpu_percent=stats[i])
        result.append(cpu_stat)
    db.insert(result)