using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using Xunit;

namespace Nexus.Sources.Tests;

public class TransformDataSourceTests
{
    [Fact]
    public async Task CanProvideUnitFromOriginalResourceName()
    {
        // Arrange
        var originalName = "foo in m/s";
        var resourceId = "foo_in_m_s";
        var pattern = @"^.*in\s(.*)";
        var expected = "m/s";

        /* data source setup */
        var settings = new TransformSettings(
            UnitFromOriginalResourceNamePattern: pattern
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
        var resource = new ResourceBuilder(id: resourceId)
            .WithOriginalName(originalName)
            .Build();

        var catalog = new ResourceCatalogBuilder(id: "/bar")
            .AddResource(resource)
            .Build();

        // Act
        var actualCatalog = await dataSource.EnrichCatalogAsync(catalog, CancellationToken.None);

        // Assert
        var actual = actualCatalog.Resources![0].Properties!.GetStringValue([DataModelExtensions.UnitKey]);

        Assert.Equal(expected, actual);
    }
}