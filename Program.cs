using System.Collections.Concurrent;
using System.Globalization;

namespace _1brc;

public static class Program
{
    private static async Task Main(string[] args)
    {
        var filePath = args[0];
        var stationData = new ConcurrentDictionary<string, (double Min, double Max, double Sum, int Count)>();
        
        await ProcessFileInParallelAsync(filePath, stationData);
        
        var orderedResults = stationData.OrderBy(pair => pair.Key);
        foreach (var pair in orderedResults)
        {
            var (min, max, sum, count) = pair.Value;
            var mean = sum / count;
            Console.WriteLine($"{pair.Key};{min:F1};{mean:F1};{max:F1}");
        }
    }

    private static async Task ProcessFileInParallelAsync(string filePath, ConcurrentDictionary<string, (double Min, double Max, double Sum, int Count)> stationData)
    {
        var processorCount = Environment.ProcessorCount;
        var tasks = new Task[processorCount];

        for (var chunkIndex = 0; chunkIndex < processorCount; chunkIndex++)
        {
            var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, true);
            var chunkLength = (stream.Length / Environment.ProcessorCount);
            var chunkOffset = chunkIndex * chunkLength;
            var chunkEnd = chunkOffset + chunkLength;
            if (chunkOffset > 0)
            {
                stream.Seek(chunkOffset, SeekOrigin.Begin);
                // skip to next line in case the chunk starts in the middle of a line
                while (stream.ReadByte() != '\n')
                    stream.Seek(-2, SeekOrigin.Current);
            }
            
            var reader = new StreamReader(stream);
            tasks[chunkIndex] = Task.Run(async () =>
            {
                while (stream.Position < chunkEnd && await reader.ReadLineAsync() is { } line)
                {
                    var separatorIndex = line.IndexOf(';', StringComparison.Ordinal);
                    if (!double.TryParse(line.AsSpan(separatorIndex + 1), NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var temperature))
                        continue;
                    
                    stationData.AddOrUpdate(line.AsSpan(0, separatorIndex).ToString(),
                        _ => (temperature, temperature, temperature, 1),
                        (_, oldValue) => (
                            Math.Min(oldValue.Min, temperature),
                            Math.Max(oldValue.Max, temperature),
                            oldValue.Sum + temperature,
                            oldValue.Count + 1
                        )
                    );
                }
                reader.Close();
            });
        }

        await Task.WhenAll(tasks);
    }
}