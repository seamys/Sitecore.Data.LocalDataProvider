# Sitecore.Data.LocalDataProvider

[说明文档](README-CN.md)

### What is it for?

Storage data to the local file system when Sitecore first launch.
The data will be stored */data/cache* folder. if you don't need, just delete it. 

### Why you need it?

The Sitecore first launched needs to fetch data from the database and caching data to own's cache. 
when the project small the Sitecore working friendly with the developer, but when the project grows, the developer will have to wait longer and longer.
Unfortunately, sitecore is very easy to lose cache when you compile and publish the project or recycle the application pool from IIS and if you using remote data it will be worse.

### How to use it?

1. Download the source code.
2. Rebuilding the project. 
3. Copy the *Sitecore.Data.LocalDataProvider.dll* to Sitecore website bin folder.
4. Modify *Sitecore.config* or *web.config*.
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

### How to contact me?
##### **website:**
http://www.frllk.com
##### **email:**
yiim@foxmail.com
##### **wechat:**
<img alt="SEAMYSHAI" src="https://www.frllk.com/wechat.jpg" width="200" />
