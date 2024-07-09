using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

// internal enum TransformOperation
// {
//     SetAlways,
//     SetIfNotExists
// }

internal record PropertyTransform(
    // TransformTarget target, /* (catalog vs. resource - maybe required later) */
    // TransformOperation Operation, /* (maybe required later)
    string SourcePath,
    string SourcePattern,
    string TargetProperty,
    string? TargetTemplate,
    string? Separator
);

internal record IdTransform(
    // TransformTarget target, /* (catalog vs. resource - maybe required later) */
    // TransformOperation Operation,
    // string SourcePattern,
    // string? TargetTemplate,
    // string SourcePath,
    // string TargetProperty
);

internal record TransformSettings(
    PropertyTransform[] PropertyTransforms
);

/// <summary>
/// A data source which helps transforming the resources and properties of a catalog.
/// </summary>
public class TransformDataSource : IDataSource
{
    private const string DEFAULT_TARGET_TEMPLATE = "$1";

    private readonly Dictionary<string, string[]> _pathCache = new();

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
            var newResourceProperties = default(Dictionary<string, JsonElement>);

            if (_settings.PropertyTransforms is not null)
            {
                foreach (var transform in _settings.PropertyTransforms)
                {
                    // get source value
                    if (!_pathCache.TryGetValue(transform.SourcePath, out var sourcePathSegments))
                    {
                        sourcePathSegments = transform.SourcePath.Split('/');
                        _pathCache[transform.SourcePath] = sourcePathSegments;
                    }

                    var sourceValue = resourceProperties?.GetStringValue(sourcePathSegments);

                    if (sourceValue is null)
                        continue;

                    // get target value
                    if (!_pathCache.TryGetValue(transform.TargetProperty, out var targetPathSegments))
                    {
                        targetPathSegments = [transform.TargetProperty];
                        _pathCache[transform.TargetProperty] = targetPathSegments;
                    }

                    var targetValue = resourceProperties?.GetStringValue(targetPathSegments);

                    // if (targetValue is not null && transform.Operation == TransformOperation.SetIfNotExists)
                    //     continue;

                    // get new target value
                    if (newResourceProperties is null)
                    {
                        newResourceProperties = resourceProperties is null
                            ? []
                            : resourceProperties!.ToDictionary(entry => entry.Key, entry => entry.Value);
                    }

                    var newTargetValue = Regex
                        .Replace(
                            input: sourceValue, 
                            pattern: transform.SourcePattern, 
                            replacement: transform.TargetTemplate ?? DEFAULT_TARGET_TEMPLATE
                        );

                    if (transform.Separator is null)
                    {
                        newResourceProperties[transform.TargetProperty]
                            = JsonSerializer.SerializeToElement(newTargetValue);
                    }

                    else
                    {
                        newResourceProperties[transform.TargetProperty]
                            = JsonSerializer.SerializeToElement(newTargetValue.Split(transform.Separator));
                    }
                }
            }

            if (newResourceProperties is null)
            {
                newResources.Add(resource);
            }

            else
            {
                newResources.Add(resource with
                {
                    Properties = newResourceProperties
                });
            }
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