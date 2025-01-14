using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using Xunit;

namespace Nexus.Sources.Tests;

public class TransformDataSourceTests
{
    [Fact]
    public async Task CanTransformResourceIdInTwoSteps()
    {
        // Arrange
        var id = "temp_very_long_name_which_should_be_shortened_in_two_steps";
        var sourcePattern1 = "(.*)very_long_name(.*)";
        var targetTemplate1 = "$1VLN$2";
        var sourcePattern2 = "(.*)_which_should_be_shortened_in_two_steps(.*)";
        var targetTemplate2 = "$1WSBSI2S$2";
        var expected = "temp_VLNWSBSI2S";

        /* data source setup */
        var transform1 = new IdTransform(
            SourcePattern: sourcePattern1,
            TargetTemplate: targetTemplate1
        );

        var transform2 = new IdTransform(
            SourcePattern: sourcePattern2,
            TargetTemplate: targetTemplate2
        );

        var settings = new TransformSettings(
            IdTransforms: [transform1, transform2],
            PropertyTransforms: []
        );

        var sourceConfiguration = JsonSerializer
            .Deserialize<IReadOnlyDictionary<string, JsonElement>>(JsonSerializer.SerializeToElement(settings));

        var context = new DataSourceContext(
            ResourceLocator: default,
            SystemConfiguration: default,
            SourceConfiguration: sourceConfiguration,
            RequestConfiguration: default
        );

        var dataSource = new Transform();

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        /* catalog setup */
        var resource = new ResourceBuilder(id: id)
            .Build();

        var catalog = new ResourceCatalogBuilder(id: "/bar")
            .AddResource(resource)
            .Build();

        // Act
        var actualCatalog = await dataSource
            .EnrichCatalogAsync(catalog, CancellationToken.None);

        // Assert
        var actual = actualCatalog.Resources![0].Id;

        Assert.Equal(expected, actual);
    }

    [Theory]

    /* test the default target template ($1) which requires a single regex capture group */
    [InlineData([
        "foo in m/s",
        @"^.*in\s(.*)",
        default,
        "m/s"
    ])]

    /* test the combination of multiple back references */
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
        var transform = new PropertyTransform(
            Operation: default,
            SourcePath: DataModelExtensions.OriginalNameKey,
            SourcePattern: sourcePattern,
            TargetProperty: DataModelExtensions.UnitKey,
            TargetTemplate: targetTemplate,
            Separator: default
        );

        var settings = new TransformSettings(
            IdTransforms: [],
            PropertyTransforms: [transform]
        );

        var sourceConfiguration = JsonSerializer
            .Deserialize<IReadOnlyDictionary<string, JsonElement>>(JsonSerializer.SerializeToElement(settings));

        var context = new DataSourceContext(
            ResourceLocator: default,
            SystemConfiguration: default,
            SourceConfiguration: sourceConfiguration,
            RequestConfiguration: default
        );

        var dataSource = new Transform();

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

    [Fact]
    public async Task CanDeriveUnitFromOriginalNameInTwoSteps()
    {
        // Arrange
        var originalName = "temp_very_long_name_which_should_be_shortened_in_two_steps";
        var sourcePattern1 = "(.*)very_long_name(.*)";
        var targetTemplate1 = "$1VLN$2";
        var sourcePattern2 = "(.*)_which_should_be_shortened_in_two_steps(.*)";
        var targetTemplate2 = "$1WSBSI2S$2";
        var expected = "temp_VLNWSBSI2S";

        /* data source setup */
        var transform1 = new PropertyTransform(
            Operation: default,
            SourcePath: DataModelExtensions.OriginalNameKey,
            SourcePattern: sourcePattern1,
            TargetProperty: DataModelExtensions.UnitKey,
            TargetTemplate: targetTemplate1,
            Separator: default
        );

        var transform2 = new PropertyTransform(
            Operation: default,
            SourcePath: DataModelExtensions.UnitKey,
            SourcePattern: sourcePattern2,
            TargetProperty: DataModelExtensions.UnitKey,
            TargetTemplate: targetTemplate2,
            Separator: default
        );

        var settings = new TransformSettings(
            IdTransforms: [],
            PropertyTransforms: [transform1, transform2]
        );

        var sourceConfiguration = JsonSerializer
            .Deserialize<IReadOnlyDictionary<string, JsonElement>>(JsonSerializer.SerializeToElement(settings));

        var context = new DataSourceContext(
            ResourceLocator: default,
            SystemConfiguration: default,
            SourceConfiguration: sourceConfiguration,
            RequestConfiguration: default
        );

        var dataSource = new Transform();

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

    [Fact]
    public async Task DoesNotModifyPropertiesIfNoMatch()
    {
        // Arrange
        var originalName = "foo in meters per second";
        var sourcePattern = ".^";

        /* data source setup */
        var transform = new PropertyTransform(
            Operation: default,
            SourcePath: DataModelExtensions.OriginalNameKey,
            SourcePattern: sourcePattern,
            TargetProperty: DataModelExtensions.UnitKey,
            TargetTemplate: default,
            Separator: default
        );

        var settings = new TransformSettings(
            IdTransforms: [],
            PropertyTransforms: [transform]
        );

        var sourceConfiguration = JsonSerializer
            .Deserialize<IReadOnlyDictionary<string, JsonElement>>(JsonSerializer.SerializeToElement(settings));

        var context = new DataSourceContext(
            ResourceLocator: default,
            SystemConfiguration: default,
            SourceConfiguration: sourceConfiguration,
            RequestConfiguration: default
        );

        var dataSource = new Transform();

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

        Assert.Null(actual);
    }

    [Fact]
    public async Task CanDeriveGroupsFromOriginalName()
    {
        // Arrange
        var originalName = "v_horz_100m_blue_avg";
        var sourcePattern = ".*?([0-9]+m)_(.*)_(.*)";
        var targetTemplate = "$2 ($1);$2 ($3)";
        var separator = ";";
        var expected = new string[] { "blue (100m)", "blue (avg)" };

        /* data source setup */
        var transform = new PropertyTransform(
            Operation: default,
            SourcePath: DataModelExtensions.OriginalNameKey,
            SourcePattern: sourcePattern,
            TargetProperty: DataModelExtensions.GroupsKey,
            TargetTemplate: targetTemplate,
            Separator: separator
        );

        var settings = new TransformSettings(
            IdTransforms: [],
            PropertyTransforms: [transform]
        );

        var sourceConfiguration = JsonSerializer
            .Deserialize<IReadOnlyDictionary<string, JsonElement>>(JsonSerializer.SerializeToElement(settings));

        var context = new DataSourceContext(
            ResourceLocator: default,
            SystemConfiguration: default,
            SourceConfiguration: sourceConfiguration,
            RequestConfiguration: default
        );

        var dataSource = new Transform();

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
            .GetStringArray([DataModelExtensions.GroupsKey]);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task CanDeriveIdFromOriginalName()
    {
        // Arrange
        var originalName = "v_horz_100m_blue_avg in m/s";
        var sourcePattern = @"(.*)\sin .*";
        var expected = "v_horz_100m_blue_avg";

        /* data source setup */
        var transform = new PropertyTransform(
            Operation: default,
            SourcePath: DataModelExtensions.OriginalNameKey,
            SourcePattern: sourcePattern,
            TargetProperty: default,
            TargetTemplate: default,
            Separator: default
        );

        var settings = new TransformSettings(
            IdTransforms: [],
            PropertyTransforms: [transform]
        );

        var sourceConfiguration = JsonSerializer
            .Deserialize<IReadOnlyDictionary<string, JsonElement>>(JsonSerializer.SerializeToElement(settings));

        var context = new DataSourceContext(
            ResourceLocator: default,
            SystemConfiguration: default,
            SourceConfiguration: sourceConfiguration,
            RequestConfiguration: default
        );

        var dataSource = new Transform();

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
        var actual = actualCatalog.Resources![0].Id;

        Assert.Equal(expected, actual);
    }

    [Theory]

    /* test creation of missing value or replacement of wrong value */
    [InlineData([nameof(TransformOperation.SetAlways), "foobar"])]

    /* test setting a default value (this is useful for default groups as well) */
    [InlineData([nameof(TransformOperation.SetIfNotExists), "m/s"])]

    public async Task CanHandleDifferentTransformOperations(
        string operationString,
        string expected
    )
    {
        // Arrange
        var operation = Enum.Parse<TransformOperation>(operationString);

        /* data source setup */
        var transform = new PropertyTransform(
            Operation: operation,
            SourcePath: DataModelExtensions.OriginalNameKey,
            SourcePattern: ".*",
            TargetProperty: DataModelExtensions.UnitKey,
            TargetTemplate: "$0",
            Separator: default
        );

        var settings = new TransformSettings(
            IdTransforms: [],
            PropertyTransforms: [transform]
        );

        var sourceConfiguration = JsonSerializer
            .Deserialize<IReadOnlyDictionary<string, JsonElement>>(JsonSerializer.SerializeToElement(settings));

        var context = new DataSourceContext(
            ResourceLocator: default,
            SystemConfiguration: default,
            SourceConfiguration: sourceConfiguration,
            RequestConfiguration: default
        );

        var dataSource = new Transform();

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