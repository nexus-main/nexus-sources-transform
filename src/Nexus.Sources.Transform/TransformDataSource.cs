using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

/// <summary>
/// Specifies the type of transformation operation to be performed.
/// </summary>
public enum TransformOperation
{
    /// <summary>
    /// Always set the value, regardless of its current state.
    /// </summary>
    SetAlways,

    /// <summary>
    /// Set the value only if it does not already exist.
    /// </summary>
    SetIfNotExists
}

/// <summary>
/// Represents a transformation operation to be applied to a property.
/// </summary>
/// <param name="Operation">The operation to be performed during the transformation.</param>
/// <param name="SourcePath">The path to the source data.</param>
/// <param name="SourcePattern">The pattern to match within the source data.</param>
/// <param name="TargetProperty">The target property where the transformed data will be applied. Can be null.</param>
/// <param name="TargetTemplate">The template to be used for the target property. Can be null.</param>
/// <param name="Separator">The separator to be used in the transformation. Can be null.</param>
public record PropertyTransform(
    // TransformTarget target, /* (catalog vs. resource - maybe required later) */
    TransformOperation Operation,
    string SourcePath,
    string SourcePattern,
    string? TargetProperty,
    string? TargetTemplate,
    string? Separator
)
{
    internal bool NeedsVariableExpansion { get; set; }
}

/// <summary>
/// Represents a transformation identifier with a source pattern and an optional target template.
/// </summary>
/// <param name="SourcePattern">The pattern used to identify the source.</param>
/// <param name="TargetTemplate">The optional template used to define the target.</param>
public record IdTransform(
    // TransformTarget target, /* (catalog vs. resource - maybe required later) */
    string SourcePattern,
    string? TargetTemplate
);

/// <summary>
/// Represents the settings for transforming data sources.
/// </summary>
/// <param name="PropertyTransforms">An array of property transformations to be applied.</param>
/// <param name="IdTransforms">An array of ID transformations to be applied.</param>
public record TransformSettings(
    PropertyTransform[]? PropertyTransforms,
    IdTransform[]? IdTransforms
);

/// <summary>
/// A data source which helps transforming the resources and properties of a catalog.
/// </summary>
[ExtensionDescription(
    "A data source to transform catalog properties like resources names and group memberships",
    "https://github.com/nexus-main/nexus-sources-transform",
    "https://github.com/nexus-main/nexus-sources-transform")]
public partial class Transform : IDataSource<TransformSettings>
{
    private const string DEFAULT_TARGET_TEMPLATE = "$1";

    private readonly Dictionary<string, string[]> _pathCache = [];

    private DataSourceContext<TransformSettings> _context = default!;

    private ILogger _logger = default!;

    [GeneratedRegex(@"\${(.*?)}")]
    private partial Regex VariableExpansionRegex { get; }

    /// <inheritdoc/>
    public Task SetContextAsync(
        DataSourceContext<TransformSettings> context,
        ILogger logger,
        CancellationToken cancellationToken
    )
    {
        _context = context;

        var settings = context.SourceConfiguration;

        if (settings is not null && settings.PropertyTransforms is not null)
        {
            foreach (var propertyTransform in settings.PropertyTransforms)
            {
                if (propertyTransform.TargetTemplate is not null)
                    propertyTransform.NeedsVariableExpansion = VariableExpansionRegex.IsMatch(propertyTransform.TargetTemplate);
            }
        }

        _logger = logger;

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(
        string path,
        CancellationToken cancellationToken
    )
    {
        return Task.FromResult(Array.Empty<CatalogRegistration>());
    }

    /// <inheritdoc/>
    public Task<ResourceCatalog> EnrichCatalogAsync(
        ResourceCatalog catalog,
        CancellationToken cancellationToken
    )
    {
        if (catalog.Resources is null)
            return Task.FromResult(catalog);

        var newResources = new List<Resource>();
        var settings = _context.SourceConfiguration;

        if (settings.IdTransforms is null)
            _logger.LogDebug("There are no identifier transforms to process");

        if (settings.PropertyTransforms is null)
            _logger.LogDebug("There are no property transforms to process");

        foreach (var resource in catalog.Resources)
        {
            var localResource = resource;

            // resource properties
            var resourceProperties = localResource.Properties;
            var newResourceProperties = default(Dictionary<string, JsonElement>);

            if (settings.PropertyTransforms is not null)
            {
                newResourceProperties = resourceProperties is null
                    ? []
                    : resourceProperties!.ToDictionary(entry => entry.Key, entry => entry.Value);

                foreach (var transform in settings.PropertyTransforms)
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
                    if (transform.TargetProperty is not null)
                    {
                        var existingValue = newResourceProperties.GetValueOrDefault(transform.TargetProperty);
                        var isNullOrUndefined = existingValue.ValueKind == JsonValueKind.Null || existingValue.ValueKind == JsonValueKind.Undefined;

                        if (!isNullOrUndefined && transform.Operation == TransformOperation.SetIfNotExists)
                            continue;
                    }

                    // get new target value
                    var isMatch = Regex.IsMatch(sourceValue, transform.SourcePattern);

                    if (!isMatch)
                        continue;

                    var targetTemplate = transform.TargetTemplate is null
                        ? DEFAULT_TARGET_TEMPLATE
                        : transform.NeedsVariableExpansion
                            ? ExpandVariables(transform.TargetTemplate, resourceProperties)
                            : transform.TargetTemplate;

                    var targetValue = Regex
                        .Replace(
                            input: sourceValue,
                            pattern: transform.SourcePattern,
                            replacement: targetTemplate
                        );

                    // apply value
                    if (transform.TargetProperty is null)
                    {
                        localResource = resource with { Id = targetValue };
                    }

                    else
                    {
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
            }

            // resource id
            if (settings.IdTransforms is not null)
            {
                var newId = resource.Id;

                foreach (var transform in settings.IdTransforms)
                {
                    newId = Regex.Replace(
                        newId,
                        transform.SourcePattern,
                        transform.TargetTemplate ?? DEFAULT_TARGET_TEMPLATE
                    );
                }

                if (settings.IdTransforms.Length != 0)
                    localResource = resource with { Id = newId };
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
    public Task<CatalogTimeRange> GetTimeRangeAsync(string catalogId, CancellationToken cancellationToken)
    {
        return Task.FromResult(new CatalogTimeRange(DateTime.MaxValue, DateTime.MinValue));
    }

    /// <inheritdoc/>
    public Task<double> GetAvailabilityAsync(
        string catalogId,
        DateTime begin,
        DateTime end,
        CancellationToken cancellationToken
    )
    {
        return Task.FromResult(double.NaN);
    }

    /// <inheritdoc/>
    public Task ReadAsync(
        DateTime begin,
        DateTime end,
        ReadRequest[] requests,
        ReadDataHandler readData,
        IProgress<double> progress,
        CancellationToken cancellationToken
    )
    {
        return Task.CompletedTask;
    }

    private string ExpandVariables(
        string targetTemplate,
        IReadOnlyDictionary<string, JsonElement>? resourceProperties
    )
    {
        return VariableExpansionRegex.Replace(targetTemplate, match =>
        {
            var path = match.Groups[1].Value;

            if (!_pathCache.TryGetValue(path, out var pathSegments))
            {
                pathSegments = path.Split('/');
                _pathCache[path] = pathSegments;
            }

            return resourceProperties is null
                ? ""
                : resourceProperties.GetStringValue(pathSegments) ?? "";
        });
    }
}