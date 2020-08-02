using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using AdvertApi.Models.Messages;
using Amazon.Lambda.Core;
using Amazon.Lambda.SNSEvents;
using Nest;
using Newtonsoft.Json;
using JsonSerializer = Amazon.Lambda.Serialization.Json.JsonSerializer;

[assembly:LambdaSerializer(typeof(JsonSerializer))]
namespace WebAdvert.SearchWorker
{
    public class SearchWorker
    {
        private readonly IElasticClient _client;

        public SearchWorker():
            this(ElasticSearchHelper.GetInstance(ConfigurationHelper.Instance))
        {
            
        }

        public SearchWorker(IElasticClient client)
        {
            _client = client;
        }
        public async Task Function(SNSEvent snsEvent, ILambdaContext context)
        {
            foreach (var record in snsEvent.Records)
            {
                context.Logger.LogLine(record.Sns.Message);

                 //ElasticClient
                 var message = JsonConvert.DeserializeObject<AdvertConfirmedMessage>(record.Sns.Message);
                 var advertDocument = MappingHelper.Map(message);

                 await _client.IndexDocumentAsync(advertDocument);
            }
        }
    }
}
