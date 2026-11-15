# ERA5 Download

本地程序。给定数据集、变量、气压层、年、月、日和区域后，用内置的 [aria2](https://aria2.github.io/) 从 [Earth Data Hub](https://earthdatahub.destine.eu/collections/era5) 的 ERA5 Zarr 镜像取出对应对象，写成一个 NetCDF。

程序由 Grok（xAI）编写，仅供科研使用。

## 数据

ERA5 是 ECMWF 第五代再分析，自 1940 年起，由 Copernicus Climate Data Store 发布。本程序读取 Earth Data Hub 的 ERA5 Zarr 镜像。

- 集合页：https://earthdatahub.destine.eu/collections/era5
- STAC items：https://earthdatahub.destine.eu/api/stac/v1/collections/era5/items
- 对象主机：`data.earthdatahub.destine.eu`

接入时请求上述 STAC，采用每个 item 里唯一的数据资产 URL。界面上的数据集名称用 STAC 标题。STAC 不可用时，改用程序内已核对的 10 条路径，并在页面注明来源。

读取的集合是 `era5`。Climate DT 在另一套目录中，本程序不打开。

| 数据集 | 备注 |
| --- | --- |
| ERA5 hourly data on pressure levels | 0.25°。DOI [10.24381/cds.bd0915c6](https://doi.org/10.24381/cds.bd0915c6) |
| ERA5 monthly data on pressure levels | 气压层月值 |
| ERA5 hourly data on pressure levels for wind developers | 风电用气压层，层次较稀 |
| ERA5 hourly data on single levels - atmosphere | 单层大气，小时 |
| ERA5 daily data on single levels - atmosphere | 单层大气，日值 |
| ERA5 monthly data on single levels - atmosphere | 单层大气，月值 |
| Ensemble spreads of ERA5 single levels | 单层集合离散度。原生步长 3 h，0.5° |
| ERA5-Land hourly data | 0.1°。DOI [10.24381/cds.e2161bac](https://doi.org/10.24381/cds.e2161bac) |
| ERA5-Land daily data | 陆面日值 |
| ERA5-Land monthly data | 陆面月值。DOI [10.24381/cds.68d2bb30](https://doi.org/10.24381/cds.68d2bb30)。1950–2019 年有误的部分月变量已从镜像撤下 |
| ERA5 hourly data on single levels - ocean waves | 海浪，小时，0.5° |
| ERA5 monthly data on single levels - ocean waves | 海浪，月值 |
| ERA5 monthly anomalies of Essential Climate Variables | 基本气候变量月距平 |

条目以运行时读到的 STAC 为准。上表是编写时该集合中的产品。

数据使用遵从 Copernicus License 和 Earth Data Hub 的条款。气压层小时资料的引用 DOI 为 `10.24381/cds.bd0915c6`。

API key 在 [账户页](https://earthdatahub.destine.eu/quota-api-keys#my-personal-access-tokens) 生成。每人使用自己的 key。请求数受该 key 的额度限制。

## 输出

结果是一个 NetCDF，写在页面指定的目录。文件名包含数据集、变量、气压层、时间尺度，以及所选的年、月、日。

年、月、日分开勾选，取它们的组合。例如勾选 2002 与 2010 年、1 与 7 月、1 与 15 日，得到这 8 个日期。2 月 31 日这种日期会跳过。月值数据集只选年和月。

时间尺度决定写出的时次。小时、3 小时和 6 小时从当天的分析里取对应时次。日平均和月平均在这些时次上计算。日值和月值用数据集里已经存好的值。

aria2 下载与所选时间、层次、纬度和经度相交的存储块。NetCDF 只保留选中的格点，并按所选时间尺度处理。分块键来自该数组的 `chunk_key_encoding`。Zarr v3 默认是 `c/i/j/...`，v2 是以 `.` 分隔的下标。数组若有 shard，按 shard 网格取；否则按 chunk 网格取。

区域四项都留空时，范围取该数据集的全部水平网格。气压层产品为全球。ERA5-Land 的纬度止于其坐标南端，约 57°S。四项中只填一部分时拒绝执行。

经度为 0–360。跨 0° 时西界写负值，例如西 `-20`、东 `20`。跨日期变更线时东界取更大的 0–360 值，例如西 `170`、东 `190`。

目标文件已存在则不覆盖。对象数超过 20000 时中止。

API key 只写入当次下载的临时配置，传输结束后删除。NetCDF 里没有 key。

## 运行

依赖见 `requirements.txt`。开发与使用环境为 Python 3.13，3.11 及以上可以运行。

```bash
py -3 -m pip install -r requirements.txt
py -3 app.py
```

Windows 下可执行 `启动.bat`。服务绑定 `127.0.0.1:8765` 并打开浏览器。未指定 `--port` 且 8765 被占用时改用 8766。`--no-browser` 只启动服务。

页面顺序：填 key 并接入；选择数据集、变量、气压层、年、月、日和区域；下载。输出目录在页面填写。

接入和切换数据集时读取坐标、变量和分块形状。下载时由内置 aria2 1.37.0 取对象，再写成 NetCDF。aria2 作为单独进程运行，许可证为 GPL-2.0，见 `third_party/aria2/COPYING`。源码在 https://github.com/aria2/aria2/releases/tag/release-1.37.0 。

窗口程序：

```bash
py -3 -m PyInstaller --noconfirm --distpath . --workpath build ERA5选取.spec
```

生成的 exe 不放入本仓库。

## 来源

程序由 Grok（xAI）编写。下载使用 aria2。

ERA5 由 ECMWF 制作，Copernicus Climate Data Store 发布。Zarr 镜像由 Earth Data Hub 提供。
