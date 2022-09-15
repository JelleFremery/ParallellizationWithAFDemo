namespace ParallelizationWithAFDemo;

public static class Functions
{
    [FunctionName("HttpStart")]
    public static async Task<IActionResult> HttpStart(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get",
            Route = "hash/{start:int?}/{end:int?}")] HttpRequestMessage req,
        int start, 
        int end,
        [DurableClient] IDurableOrchestrationClient starter,
        ILogger log)
    {
        log.LogInformation($"C# HTTP trigger function processed a request: {req.RequestUri}");
        var request = new ConversionRequest { Start = start, End = end };
        string id = await starter.StartNewAsync("Orchestrator", request);
        return new OkObjectResult($"Orchestration {id} started!");
    }

    [FunctionName("Orchestrator")]
    public static async Task Orchestrator(
        [OrchestrationTrigger] IDurableOrchestrationContext context,
        ILogger log)
    {        
        if (!context.IsReplaying)
        {
            log.LogInformation("Now running...");
        }

        var data = context.GetInput<ConversionRequest>();
        var batch = await context.CallActivityAsync<List<int>>("GetValidatedBatch", data);

        if (!batch.Any())
        {
            return;
        }

        var parallelTasks = new List<Task<ConversionResult>>();
        foreach (int nr in batch)
        {                
            Task<ConversionResult> task = context.CallActivityAsync<ConversionResult>("MD5Hash", nr);
            parallelTasks.Add(task);
        }
        
        var results = await Task.WhenAll(parallelTasks);        

        StringBuilder builder = new();
        foreach (var result in results.OrderBy(r => r.Number))
        {
            builder.AppendLine(result.Number + ": " + result.Value + "\r\n");
        }
        
        await context.CallActivityAsync("WriteResultsToFile", builder);
    }

    [FunctionName("GetValidatedBatch")]
    public static List<int> GetValidatedBatch([ActivityTrigger] ConversionRequest request,
        ILogger log)
    {        
        log.LogInformation("Validating input...");

        if (request.Start <= request.End)
        {
            log.LogInformation("Valid hashing request!");
            int count = request.End - request.Start + 1;
            return Enumerable.Range(request.Start, count).ToList();
        }

        log.LogWarning($"Range {request.Start}-{request.End} is invalid. No hashing possible.");
        return new List<int>();
    }

    [FunctionName("MD5Hash")]
    public static ConversionResult MD5Hash([ActivityTrigger] int nr,
        ILogger log)
    {
        log.LogInformation($"Converting '{nr}' to MD5 hash");

        using MD5 md5 = MD5.Create();
        byte[] inputBytes = Encoding.ASCII.GetBytes(nr.ToString());
        byte[] hashBytes = md5.ComputeHash(inputBytes);

        return new ConversionResult { Number = nr, Value = Convert.ToHexString(hashBytes) };
    }        

    [FunctionName("WriteResultsToFile")]
    public static void WriteResultsToFile([ActivityTrigger] StringBuilder builder, 
        ILogger log)
    {
        string path = Directory.GetParent(System.Reflection.Assembly.GetExecutingAssembly().Location).FullName;
        string fileName = Path.Combine(path, $"ParallelAFDemo-{DateTime.Now:yyyyMMddHHmmss}.txt");
        log.LogInformation($"Writing to file: {fileName}");
        File.WriteAllText(fileName, builder.ToString());
    }
}