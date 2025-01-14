using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

internal enum TransformOperation
{
    SetAlways,

    SetIfNotExists
}

internal record PropertyTransform(
    // TransformTarget target, /* (catalog vs. resource - maybe required later) */
    TransformOperation Operation,
    string SourcePath,
    string SourcePattern,
    string TargetProperty,
    string? TargetTemplate,
    string? Separator
);

internal record IdTransform(
    // TransformTarget target, /* (catalog vs. resource - maybe required later) */
    string SourcePattern,
    string? TargetTemplate
);

internal record TransformSettings(
    IdTransform[]? IdTransforms,
    PropertyTransform[]? PropertyTransforms
);

/// <summary>
/// A data source which helps transforming the resources and properties of a catalog.
/// </summary>
[ExtensionDescription(
    "A data source to transform catalog properties like resources names and group memberships",
    "https://github.com/nexus-main/nexus-sources-transform",
    "https://github.com/nexus-main/nexus-sources-transform")]
public class Transform : IDataSource
{
    private const string DEFAULT_TARGET_TEMPLATE = "$1";

    private readonly Dictionary<string, string[]> _pathCache = [];

    private TransformSettings? _settings;

    private ILogger _logger = default!;

    /// <inheritdoc/>
    public Task SetContextAsync(DataSourceContext context, ILogger logger, CancellationToken cancellationToken)
    {
        // TODO: this is not so nice
        _settings = JsonSerializer
            .Deserialize<TransformSettings>(JsonSerializer.Serialize(context.SourceConfiguration));

        _logger = logger;

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

        if (_settings.IdTransforms is null)
            _logger.LogDebug("There are no identifier transforms to process");

        if (_settings.PropertyTransforms is null)
            _logger.LogDebug("There are no property transforms to process");

        foreach (var resource in catalog.Resources)
        {
            var localResource = resource;

            // resource id
            if (_settings.IdTransforms is not null)
            {
                var newId = resource.Id;

                foreach (var transform in _settings.IdTransforms)
                {
                    newId = Regex.Replace(
                        newId,
                        transform.SourcePattern,
                        transform.TargetTemplate ?? DEFAULT_TARGET_TEMPLATE
                    );
                }

                if (_settings.IdTransforms.Length != 0)
                    localResource = resource with { Id = newId };
            }

            // resource properties
            var resourceProperties = localResource.Properties;
            var newResourceProperties = default(Dictionary<string, JsonElement>);

            if (_settings.PropertyTransforms is not null)
            {
                newResourceProperties = resourceProperties is null
                    ? []
                    : resourceProperties!.ToDictionary(entry => entry.Key, entry => entry.Value);

                foreach (var transform in _settings.PropertyTransforms)
                {
                    // get source value
                    if (!_pathCache.TryGetValue(transform.SourcePath, out var sourcePathSegments))
                    {
                        sourcePathSegments = transform.SourcePath.Split('/');
                        _pathCache[transform.SourcePath] = sourcePathSegments;
                    }

                    var sourceValue = newResourceProperties.GetStringValue(sourcePathSegments);

                    if (sourceValue is null)
                    {
                        _logger.LogDebug("Source path not found, skipping");
                        continue;
                    }

                    // get target value
                    var existingValue = newResourceProperties.GetValueOrDefault(transform.TargetProperty);

                    if (existingValue.ValueKind != JsonValueKind.Null && transform.Operation == TransformOperation.SetIfNotExists)
                        continue;

                    // get new target value
                    var isMatch = Regex.IsMatch(sourceValue, transform.SourcePattern);

                    if (!isMatch)
                        continue;

                    var targetValue = Regex
                        .Replace(
                            input: sourceValue,
                            pattern: transform.SourcePattern,
                            replacement: transform.TargetTemplate ?? DEFAULT_TARGET_TEMPLATE
                        );

                    if (transform.Separator is null)
                    {
                        newResourceProperties[transform.TargetProperty]
                            = JsonSerializer.SerializeToElement(targetValue);
                    }

                    else
                    {
                        newResourceProperties[transform.TargetProperty]
                            = JsonSerializer.SerializeToElement(targetValue.Split(transform.Separator));
                    }
                }
            }

            if (newResourceProperties is null)
            {
                newResources.Add(localResource);
            }

            else
            {
                newResources.Add(localResource with
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