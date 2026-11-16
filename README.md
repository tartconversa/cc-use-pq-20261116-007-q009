# HeatWatch 供热网络监测服务

HeatWatch 是本地供热数据后端，接收供热站遥测，校验质量，维护阈值告警、事故和抢修派单，并提供报表与容量估算。演示数据均为本地合成数据。

## 运行方式

安装 requirements.txt 锁定的依赖后，运行 uvicorn heatwatch.api:create_app --factory。默认 SQLite 数据库为当前目录 heatwatch.sqlite3。FastAPI 的 /docs 提供接口说明。离线演示：python -m heatwatch.demo --fixture fixtures/demo.json。

测试：python -m unittest discover -s tests -v。语法检查：python -m compileall -q heatwatch。

## 数据约定

导入遥测前须配置供热站和有向管线。观测包含站点、稳定的传感器和来源标识、递增序号、带时区时间、供回水温度、流量和压力。迟到、乱序或物理异常记录留在账本，但不推进实时告警。同批次同内容安全重试，同名异内容返回冲突。事故保存下游影响范围，派单变更要求修订号。

容量规划将管线每小时立方米流量按假定的 20 摄氏度温差和每立方米每摄氏度 1.163 千瓦时转换为热功率兆瓦，列出未满足需求和剩余容量。报表区分合格观测和拒绝数，导出保存 SHA-256 摘要。/reports/reconcile 和 /operations/audit 检查账本一致性。容量规划仅供本地演示，不替代现场调度。