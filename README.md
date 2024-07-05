# Nexus.Sources.Transform

It is often necessary to shorten resource names, extract units from resource names, set resource group memberships, or perform other transformations on the original resource catalog. This package provides these (and more) features and removes the need for individual Nexus extensions to implement them themselves.

The idea is to place this source at the end in the Nexus pipeline.

As an extension developer you might wonder where to provide things like units to the resources as there are at least three different places. Here is a recommendation:

1. When the unit is part of the raw data file, read it from there and put it into the resource via

```cs 
var resource = new ResourceBuilder(id: <resourceId>)
    .WithUnit(<unit>)
    ...
    .Build();
```

2. If the unit is derived from the original resource name, as is often the case in CSV files where column names are often in a format like `temp_1 (°C)`, then use this data source and put it at the end of the data source pipeline in Nexus. Of course, you could go back to 1. but this will make your data source tightly coupled to the input data format.

3. If there is no unit information in the raw data, but you know it from elsewhere (documentation, etc), edit the metadata in the Nexus UI or use the REST API instead. The metadata is like an overlay for an existing catalog and its resources and properties. This metadata is persistently stored by Nexus.