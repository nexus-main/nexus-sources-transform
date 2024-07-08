using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using Xunit;

namespace Nexus.Sources.Tests;

public class TransformDataSourceTests
{
    [Theory]

    [InlineData([
        "foo in m/s", 
        @"^.*in\s(.*)", 
        default, 
        "m/s"
    ])]

    [InlineData([
        "foo in meters per second", 
        @"^.*in\s(.*) per (.*)", 
        "$1/$2", 
        "meters/second"
    ])]

    public async Task CanDeriveUnitFromOriginalName(
        string originalName,
        string sourcePattern,
        string? targetTemplate,
        string expected
    )
    {
        // Arrange

        /* data source setup */
        var transform = new Transform(
            Operation: TransformOperation.Set,
            SourcePattern: sourcePattern,
            TargetTemplate: targetTemplate,
            SourcePath: DataModelExtensions.OriginalNameKey,
            TargetProperty: DataModelExtensions.UnitKey
        );

        var settings = new TransformSettings(
            Transforms: [transform]
        );

        var sourceConfiguration = JsonSerializer
            .Deserialize<IReadOnlyDictionary<string, JsonElement>>(JsonSerializer.SerializeToElement(settings));

        var context = new DataSourceContext(
            ResourceLocator: default,
            SystemConfiguration: default,
            SourceConfiguration: sourceConfiguration,
            RequestConfiguration: default
        );

        var dataSource = new TransformDataSource();

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        /* catalog setup */
        var resource = new ResourceBuilder(id: "foo")
            .WithOriginalName(originalName)
            .Build();

        var catalog = new ResourceCatalogBuilder(id: "/bar")
            .AddResource(resource)
            .Build();

        // Act
        var actualCatalog = await dataSource
            .EnrichCatalogAsync(catalog, CancellationToken.None);

        // Assert
        var actual = actualCatalog.Resources![0].Properties!
            .GetStringValue([DataModelExtensions.UnitKey]);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData([nameof(TransformOperation.Set), "foobar"])]
    [InlineData([nameof(TransformOperation.SetIfNotExists), "m/s"])]
    public async Task CanHandleDifferentTransformOperations(
        string operationString,
        string expected
    )
    {
        // Arrange
        var operation = Enum.Parse<TransformOperation>(operationString);

        /* data source setup */
        var transform = new Transform(
            Operation: operation,
            SourcePattern: ".*",
            TargetTemplate: "$0",
            SourcePath: DataModelExtensions.OriginalNameKey,
            TargetProperty: DataModelExtensions.UnitKey
        );

        var settings = new TransformSettings(
            Transforms: [transform]
        );

        var sourceConfiguration = JsonSerializer
            .Deserialize<IReadOnlyDictionary<string, JsonElement>>(JsonSerializer.SerializeToElement(settings));

        var context = new DataSourceContext(
            ResourceLocator: default,
            SystemConfiguration: default,
            SourceConfiguration: sourceConfiguration,
            RequestConfiguration: default
        );

        var dataSource = new TransformDataSource();

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        /* catalog setup */
        var resource = new ResourceBuilder(id: "foo")
            .WithOriginalName("foobar")
            .WithUnit("m/s")
            .Build();

        var catalog = new ResourceCatalogBuilder(id: "/bar")
            .AddResource(resource)
            .Build();

        // Act
        var actualCatalog = await dataSource
            .EnrichCatalogAsync(catalog, CancellationToken.None);

        // Assert
        var actual = actualCatalog.Resources![0].Properties!
            .GetStringValue([DataModelExtensions.UnitKey]);

        Assert.Equal(expected, actual);
    }
}