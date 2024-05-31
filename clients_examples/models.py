from infi.clickhouse_orm import Model, UInt32Field, Float32Field, DateTimeField, MergeTree, BufferModel, Buffer


class CpuStats(Model):
    cpu_id  = UInt32Field()
    cpu_percent = Float32Field()
    timestamp = DateTimeField()

    engine = MergeTree(order_by=[cpu_id], date_col='timestamp')


class CpuStatsBuffer(BufferModel, CpuStats):
    engine = Buffer(CpuStats)