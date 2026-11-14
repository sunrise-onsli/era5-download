# ERA5 Download

本机选择 ERA5，写出一份可以直接下载的链接。由 **Grok（xAI）** 编写，**仅供科研使用**。

数据来自 [Earth Data Hub](https://earthdatahub.destine.eu/collections/era5) 上的 ERA5 Zarr 镜像。程序不把请求送进 Copernicus Climate Data Store（CDS）排队，而是按你选的范围列出已经放在镜像里的数据块地址。你再用自己的下载工具去取。

> 本软件只用于科研。不要用于商业或业务运行。  
> 仓库里没有数据文件，也没有任何人的 API key。

## 不排队

CDS 的用法是提交一次定制请求，然后等队列。请求一大、人一多，就要等很久。Earth Data Hub 把常用的 ERA5 做成了可直接读取的 Zarr。这个程序只做三件事：核对你的 key、读目录、把选中范围对应的数据块地址写成 txt。

| | CDS | 本程序 |
| --- | --- | --- |
| 获取方式 | 提交请求，进入队列 | 列出镜像里已经存在的数据块 |
| 等待 | 队列忙时要等 | 链接写好后即可用自己的工具下载 |
| 范围 | 每次在请求里重填 | 本机选择数据集、变量、气压层、时间和区域 |
| 下载工具 | 跟请求绑在一起 | wget、aria2、IDM 或其他工具都可以 |

下载本身仍要花时间和流量。个人 key 也有 Earth Data Hub 的访问额度，额度以[账户页面](https://earthdatahub.destine.eu/quota-api-keys#my-personal-access-tokens)为准。这里省掉的是 CDS 那一段排队。

```mermaid
flowchart LR
  A[填入自己的 key] --> B[读取 ERA5 目录]
  B --> C[选择数据集、变量、时间和区域]
  C --> D[写出链接 txt]
  D --> E[用自己的工具下载数据块]
```

## 数据来源

ERA5 是 ECMWF 的第五代全球再分析，时间从 1940 年起。原始资料由 Copernicus Climate Data Store 发布。Earth Data Hub 提供其中常用数据集的 Zarr 镜像，保留原有分辨率，并更新到最近一个已经结束的时段。

本程序用的是这套 **ERA5 再分析镜像**，不是 Destination Earth 的 Climate DT。Climate DT 是公里尺度的气候模式，不能当作 ERA5 使用，也不需要它的升级权限。

接入时，程序读取公开目录：

[https://earthdatahub.destine.eu/api/stac/v1/collections/era5/items](https://earthdatahub.destine.eu/api/stac/v1/collections/era5/items)

数据块放在 `data.earthdatahub.destine.eu`。页面上的数据集名称用目录里的英文原名。目录暂时读不到时，才使用程序里已经核对过的十个地址，并在页面上说明。

| 数据集 | 说明 |
| --- | --- |
| ERA5 hourly data on pressure levels | 气压层小时数据，0.25°。[10.24381/cds.bd0915c6](https://doi.org/10.24381/cds.bd0915c6) |
| ERA5 monthly data on pressure levels | 气压层月值 |
| ERA5 hourly data on pressure levels for wind developers | 面向风电的气压层小时数据 |
| ERA5 hourly data on single levels - atmosphere | 单层大气小时数据 |
| ERA5 daily data on single levels - atmosphere | 单层大气日值 |
| ERA5 monthly data on single levels - atmosphere | 单层大气月值 |
| Ensemble spreads of ERA5 single levels | 单层集合离散度 |
| ERA5-Land hourly data | 陆面小时数据，0.1°。[10.24381/cds.e2161bac](https://doi.org/10.24381/cds.e2161bac) |
| ERA5-Land daily data | 陆面日值 |
| ERA5-Land monthly data | 陆面月值。[10.24381/cds.68d2bb30](https://doi.org/10.24381/cds.68d2bb30)。部分月变量因 1950–2019 年的数据有误已经撤下 |
| ERA5 hourly data on single levels - ocean waves | 海浪小时数据 |
| ERA5 monthly data on single levels - ocean waves | 海浪月值 |
| ERA5 monthly anomalies of Essential Climate Variables | 基本气候变量的月距平 |

目录若有增减，以当场读到的列表为准。

数据的使用条件遵循 [Copernicus License](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-pressure-levels) 以及 Earth Data Hub 的条款。引用气压层小时数据时，可使用 DOI `10.24381/cds.bd0915c6`。

## 使用

1. 在 [Earth Data Hub](https://earthdatahub.destine.eu/quota-api-keys#my-personal-access-tokens) 注册并取得自己的 API key。小组成员各自使用自己的 key。
2. 安装依赖后运行：

   ```bash
   py -3 -m pip install -r requirements.txt
   py -3 app.py
   ```

   Windows 上也可以双击 `启动.bat`。程序只监听本机 `127.0.0.1:8765`，并打开浏览器。若 8765 已被占用，会改用 8766。
3. 填入 key，点「接入」。
4. 选择数据集、变量、气压层、起止日期和区域。区域四个格子都空着，表示该数据集的整幅网格。气压层数据就是全球；ERA5-Land 的网格南界大约在 57°S，程序用它实际有的范围。
5. 点「输出链接」。txt 写在你填的输出目录里，一行一个 HTTPS 地址。

经度按 0–360 填写。跨过 0° 时西界写负数，例如西 `-20`、东 `20`。跨日期变更线时把东界写成更大的数，例如西 `170`、东 `190`。

## 链接里有什么

每一行是一个 Zarr 数据块或分片的地址，覆盖你选的日期、区域、变量和气压层。程序不事先裁剪，也不事先做日平均或月平均。时间尺度只用于估计，不改变这批原始数据块。

地址里带有你的 key，形式是：

```text
https://edh:<你的 key>@data.earthdatahub.destine.eu/...
```

这样 wget、aria2、IDM 等工具都能直接认证。txt 不要发给别人。每个人用自己的 key 再生成一份。同名文件已存在时，程序不会覆盖。一次最多 20000 条链接。

## 运行环境

- Python 3.11 及以上。本程序在 Python 3.13 上编写和使用。
- 依赖见 `requirements.txt`。打开数据集时会读取坐标和分块信息；写出链接时再读取分块地址。程序不把整份数据拉下来。

需要窗口程序时，在本目录执行：

```bash
py -3 -m PyInstaller --noconfirm --distpath . --workpath build ERA5选取.spec
```

生成的 exe 不放进本仓库。

## 作者

程序由 Grok（xAI）编写，供科研使用。

ERA5 由 ECMWF 制作，Copernicus 发布。Zarr 镜像由 Earth Data Hub 提供。
