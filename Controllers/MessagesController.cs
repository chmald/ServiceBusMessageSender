using Azure.Messaging.ServiceBus;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Net;

namespace ServiceBusMessageSender.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MessagesController : ControllerBase
    {
        private readonly ServiceBusSender _serviceBusSender;

        public MessagesController(ServiceBusSender serviceBusSender)
        {
            _serviceBusSender = serviceBusSender;
        }

        [HttpGet]
        public async Task<ActionResult> Index(
            [FromQuery(Name = "numMsgs")] int numMsgs = 0)
        {

            using ServiceBusMessageBatch messageBatch = await _serviceBusSender.CreateMessageBatchAsync();

            for (int i = 0; i < numMsgs; i++)
            {
                if(!messageBatch.TryAddMessage( new ServiceBusMessage($"Message {i}")))
                {
                    throw new Exception($"The message {i} is too large to fit in the batch.");
                }
            }

            try
            {
                await _serviceBusSender.SendMessagesAsync(messageBatch);
                Console.WriteLine($"A batch of {numMsgs} messages has been published to the queue.");
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }

            return Ok();
        }
    }
}
