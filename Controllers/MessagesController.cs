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
            int batchLimit = 2000;
            int fullRuns = 0;
            int remaining;

            if(numMsgs <= batchLimit)
            {
                remaining = numMsgs;
            } else
            {
                fullRuns = Convert.ToInt32(Math.Floor(Convert.ToDecimal(numMsgs / batchLimit)));
                remaining = numMsgs - (fullRuns * batchLimit);
            }

            for(int i = 0; i < fullRuns; i++)
            {
                await RunBatch(batchLimit);
            }

            await RunBatch(remaining);

            return Ok();
        }

        private async Task RunBatch(int messages)
        {
            using ServiceBusMessageBatch messageBatch = await _serviceBusSender.CreateMessageBatchAsync();

            for (int i = 0; i < messages; i++)
            {
                if (!messageBatch.TryAddMessage(new ServiceBusMessage($"Message {i}")))
                {
                    throw new Exception($"The message {i} is too large to fit in the batch.");
                }
            }

            try
            {
                await _serviceBusSender.SendMessagesAsync(messageBatch);
                Console.WriteLine($"A batch of {messages} messages has been published to the queue.");
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }
        }
    }
}
