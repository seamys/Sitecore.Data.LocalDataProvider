# Sitecore.Data.LocalDataProvider

[Read me here](README.md)

### 它用来做什么的?

当Sitecore项目第一次启动的时候存储数据到本地文件系统，做二次缓存。
数据会被存储到项目目录 **/data/cache** 文件夹里。


### 为啥需要它?

Sitecore 每次启动的时候会检查它的本地缓存， 如果本地缓存没有会尝试从数据库拉取数据缓存到本地。
如果项目小开发的时候时可以接受的，当项目越来越大的时候，等待的时间会越来也长。
Sitecore 缓存使用的是 Hashtable，因此缓存非常容易丢失，当你从新编译并发布或者回收IIS应用程序时它会重新去拉去数据。
如果你使用的远程数据库，这个情况会更加的糟糕。

### 如何使用它?

时间仓促,非常抱歉没有使用 nuget 包管理。

1. 下载源代码.
2. 从新编译一下代码. 
3. 复制 *Sitecore.Data.LocalDataProvider.dll* 到Sitecore bin 目录里面
4. 修改 *Sitecore.config* 或者 *web.config* 取决你使用 Sitecore 版本.
```xml
  <dataApis>
    <!-- Data api for accessing SQL Server databases. -->
    <dataApi name="SqlServer" type="Sitecore.Data.LocalDataProvider.$(database)LocalDataApi, Sitecore.Data.LocalDataProvider">
      <param connectionStringName="$(1)" />
    </dataApi>
  </dataApis>
  <!-- DATA PROVIDERS -->
  <dataProviders>
    <main type="Sitecore.Data.LocalDataProvider.$(database)LocalDataProvider, Sitecore.Data.LocalDataProvider">
	  <param name="api" ref="dataApis/dataApi" param1="$(1)"/>
      <Name>$(1)</Name>
    </main>
  </dataProviders>
````

### 如何联系我?
##### **website:**
http://www.frllk.com
##### **email:**
yiim@foxmail.com
##### **wechat:**
<img alt="SEAMYSHAI" src="https://www.frllk.com/wechat.jpg" width="200" />
