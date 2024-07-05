using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

internal record TransformSettings(
    string? UnitFromOriginalResourceNamePattern
);

/// <summary>
/// A data source which helps transforming the resources and properties of a catalog.
/// </summary>
public class TransformDataSource : IDataSource
{
    private static readonly string[] _originalNamePath = [DataModelExtensions.OriginalNameKey];

    private TransformSettings? _settings;

    /// <inheritdoc/>
    public Task SetContextAsync(DataSourceContext context, ILogger logger, CancellationToken cancellationToken)
    {
        // TODO: this is not so nice
        _settings = JsonSerializer
            .Deserialize<TransformSettings>(JsonSerializer.Serialize(context.SourceConfiguration));

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
    {
        return Task.FromResult(Array.Empty<CatalogRegistration>());
    }

    /// <inheritdoc/>
    public Task<ResourceCatalog> EnrichCatalogAsync(ResourceCatalog catalog, CancellationToken cancellationToken)
    {
        if (catalog.Resources is null || _settings is null)
            return Task.FromResult(catalog);

        var newResources = new List<Resource>();

        foreach (var resource in catalog.Resources)
        {
            var resourceProperties = resource.Properties;
            var newResource = resource;

            var newResourceProperties = resourceProperties is null
                ? []
                : resourceProperties!.ToDictionary(entry => entry.Key, entry => entry.Value);

            // extract unit from original resource name
            if (_settings.UnitFromOriginalResourceNamePattern is not null)
            {
                var originalName = resourceProperties?.GetStringValue(_originalNamePath)!;
                var match = Regex.Match(originalName, _settings.UnitFromOriginalResourceNamePattern);

                if (match.Success && match.Groups.Count >= 2)
                {
                    newResourceProperties[DataModelExtensions.UnitKey]
                        = JsonSerializer.SerializeToElement(match.Groups[1].Value);
                }
            }

            newResource = resource with
            {
                Properties = newResourceProperties
            };

            newResources.Add(newResource);
        }

        catalog = catalog with
        {
            Resources = newResources
        };

        return Task.FromResult(catalog);
    }

    /// <inheritdoc/>
    public Task<(DateTime Begin, DateTime End)> GetTimeRangeAsync(string catalogId, CancellationToken cancellationToken)
    {
        return Task.FromResult((DateTime.MaxValue, DateTime.MinValue));
    }

    /// <inheritdoc/>
    public Task<double> GetAvailabilityAsync(string catalogId, DateTime begin, DateTime end, CancellationToken cancellationToken)
    {
        return Task.FromResult(double.NaN);
    }

    /// <inheritdoc/>
    public Task ReadAsync(DateTime begin, DateTime end, ReadRequest[] requests, ReadDataHandler readData, IProgress<double> progress, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}